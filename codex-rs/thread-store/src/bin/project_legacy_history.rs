use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use codex_protocol::ThreadId;
use codex_state::SqliteConfig;
use codex_thread_store::LocalThreadStore;
use codex_thread_store::LocalThreadStoreConfig;
use codex_thread_store::materialize_legacy_to_postgres;
use codex_utils_absolute_path::AbsolutePathBuf;
use serde::Serialize;
use sqlx::Row;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePoolOptions;

#[derive(Debug, Parser)]
#[command(name = "codex-project-legacy-history")]
#[command(about = "Project legacy Codex rollout JSONL into PostgreSQL turn/item tables")]
struct Args {
    #[arg(long, env = "CODEX_SQLITE_HOME")]
    sqlite_home: PathBuf,

    #[arg(long, env = "CODEX_STATE_DATABASE_URL")]
    postgres_url: String,

    #[arg(long, env = "CODEX_STATE_SCHEMA")]
    schema: String,

    #[arg(long)]
    user_id: String,

    #[arg(long)]
    thread_owners: PathBuf,
}

#[derive(Debug, Serialize)]
struct ProjectionReport {
    schema: String,
    user_id: String,
    projected_threads: usize,
    projected_thread_ids: Vec<String>,
    missing_rollouts: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    codex_state::postgres::validate_identifier(&args.schema)?;
    let ownership_raw = tokio::fs::read(&args.thread_owners)
        .await
        .with_context(|| format!("read thread ownership map {}", args.thread_owners.display()))?;
    let thread_owners: HashMap<String, String> =
        serde_json::from_slice(&ownership_raw).context("parse thread ownership map")?;

    // This process owns these values for its entire lifetime. The thread-store
    // opens all state/history pools from this immutable environment.
    unsafe {
        std::env::set_var("CODEX_STATE_DATABASE_URL", &args.postgres_url);
        std::env::set_var("CODEX_STATE_SCHEMA", &args.schema);
        std::env::set_var("CODEX_STATE_BACKEND", "postgres-only");
    }

    let absolute_home = AbsolutePathBuf::try_from(args.sqlite_home.as_path())?;
    let store = LocalThreadStore::new(
        LocalThreadStoreConfig {
            codex_home: args.sqlite_home.clone(),
            sqlite: SqliteConfig::from_sqlite_home(absolute_home),
            default_model_provider_id: "unknown".to_string(),
        },
        None,
    );
    let state_path = args.sqlite_home.join("state_5.sqlite");
    let sqlite = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&state_path)
                .read_only(true)
                .create_if_missing(false),
        )
        .await
        .with_context(|| format!("open source SQLite database {}", state_path.display()))?;
    let rows = sqlx::query("SELECT id, rollout_path FROM threads ORDER BY created_at, id")
        .fetch_all(&sqlite)
        .await?;

    let postgres = sqlx::PgPool::connect(&args.postgres_url).await?;
    codex_state::postgres::create_schema(&args.postgres_url, &args.schema).await?;
    let mut projected_threads = 0;
    let mut projected_thread_ids = Vec::new();
    let mut missing_rollouts = Vec::new();
    for row in rows {
        let thread_id: String = row.try_get("id")?;
        if thread_owners.get(&thread_id).map(String::as_str) != Some(args.user_id.as_str()) {
            continue;
        }
        let stored_path: String = row.try_get("rollout_path")?;
        let Some(rollout_path) = resolve_rollout_path(&args.sqlite_home, &stored_path).await else {
            missing_rollouts.push(thread_id);
            continue;
        };
        let parsed_thread_id = ThreadId::from_string(&thread_id)
            .with_context(|| format!("parse Codex thread ID {thread_id}"))?;
        for table in [
            "thread_items",
            "thread_turns",
            "thread_rollout_events",
            "thread_history_projection_state",
        ] {
            let delete = format!(
                "DELETE FROM \"{}\".\"{}\" WHERE thread_id = $1",
                args.schema, table
            );
            sqlx::query(sqlx::AssertSqlSafe(delete))
                .bind(&thread_id)
                .execute(&postgres)
                .await?;
        }
        materialize_legacy_to_postgres(&store, parsed_thread_id, &rollout_path)
            .await
            .with_context(|| format!("project legacy rollout for thread {thread_id}"))?;
        projected_threads += 1;
        projected_thread_ids.push(thread_id);
    }
    sqlite.close().await;

    if !projected_thread_ids.is_empty() {
        let update = format!(
            "UPDATE \"{}\".threads SET history_mode = 'paginated' WHERE id = ANY($1)",
            args.schema
        );
        sqlx::query(sqlx::AssertSqlSafe(update))
            .bind(&projected_thread_ids)
            .execute(&postgres)
            .await?;
    }
    postgres.close().await;

    let report = ProjectionReport {
        schema: args.schema,
        user_id: args.user_id,
        projected_threads,
        projected_thread_ids,
        missing_rollouts,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !report.missing_rollouts.is_empty() {
        anyhow::bail!(
            "{} owned thread rollout(s) were not found; their PostgreSQL history mode remains legacy",
            report.missing_rollouts.len()
        );
    }
    Ok(())
}

async fn resolve_rollout_path(sqlite_home: &Path, stored_path: &str) -> Option<PathBuf> {
    let stored = PathBuf::from(stored_path);
    if tokio::fs::try_exists(&stored).await.ok()? {
        return Some(stored);
    }
    let relative = stored_path
        .split_once("/sessions/")
        .map(|(_, suffix)| PathBuf::from("sessions").join(suffix))?;
    let candidate = sqlite_home.join(relative);
    tokio::fs::try_exists(&candidate)
        .await
        .ok()
        .filter(|exists| *exists)
        .map(|_| candidate)
}
