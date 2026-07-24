use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;

mod copy;
mod schema;

#[derive(Debug, Parser)]
#[command(name = "codex-state-sqlite-to-postgres")]
#[command(about = "Migrate Codex state SQLite databases into PostgreSQL")]
struct Args {
    /// Directory containing state_5.sqlite and the other Codex state databases.
    #[arg(long, env = "CODEX_SQLITE_HOME")]
    sqlite_home: PathBuf,

    /// PostgreSQL connection string for the isolated migration database.
    #[arg(long, env = "CODEX_STATE_DATABASE_URL")]
    postgres_url: String,

    /// PostgreSQL schema that will receive the migrated rows. The schema must
    /// be dedicated to exactly one tenant and system user ID.
    #[arg(long)]
    schema: String,

    /// Tenant assigned by the Starwork control plane.
    #[arg(long, default_value = "ysp")]
    tenant_id: String,

    /// Stable system user ID; this is not the login username.
    #[arg(long)]
    user_id: String,

    /// JSON object mapping every known Codex thread ID to its system user ID.
    #[arg(long)]
    thread_owners: PathBuf,

    /// Include records that are not owned by a thread. Use this only for the
    /// dedicated tenant system schema.
    #[arg(long, default_value_t = false)]
    include_global: bool,
}

#[derive(Debug, Serialize)]
struct MigrationReport {
    source: String,
    destination_schema: String,
    tenant_id: String,
    user_id: String,
    counts: copy::MigrationCounts,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    schema::validate_identifier(&args.schema)?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&args.postgres_url)
        .await
        .context("connect to PostgreSQL")?;

    codex_state::postgres::create_schema(&args.postgres_url, &args.schema).await?;
    let ownership_raw = tokio::fs::read(&args.thread_owners)
        .await
        .with_context(|| format!("read thread ownership map {}", args.thread_owners.display()))?;
    let thread_owners: HashMap<String, String> =
        serde_json::from_slice(&ownership_raw).context("parse thread ownership map")?;
    let counts = copy::migrate_all(
        &pool,
        &args.schema,
        &args.sqlite_home,
        &args.user_id,
        &thread_owners,
        args.include_global,
    )
    .await?;
    copy::verify_counts(&pool, &args.schema, &counts).await?;

    let report = MigrationReport {
        source: args.sqlite_home.display().to_string(),
        destination_schema: args.schema,
        tenant_id: args.tenant_id,
        user_id: args.user_id,
        counts,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    pool.close().await;
    Ok(())
}
