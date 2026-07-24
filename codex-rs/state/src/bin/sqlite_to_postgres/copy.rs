use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;

use anyhow::Context;
use serde::Serialize;
use sqlx::PgPool;
use sqlx::Row;
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePoolOptions;

use super::schema;

#[derive(Debug, Default, Serialize)]
pub(super) struct MigrationCounts {
    threads: u64,
    thread_dynamic_tools: u64,
    thread_spawn_edges: u64,
    backfill_state: u64,
    remote_control_enrollments: u64,
    external_agent_config_imports: u64,
    agent_jobs: u64,
    agent_job_items: u64,
    thread_goals: u64,
    thread_goal_continuation_deferrals: u64,
    stage1_outputs: u64,
    memory_jobs: u64,
    logs: u64,
    thread_turns: u64,
    thread_items: u64,
    thread_history_projection_state: u64,
}

pub(super) async fn migrate_all(
    postgres: &PgPool,
    schema_name: &str,
    sqlite_home: &Path,
    user_id: &str,
    thread_owners: &HashMap<String, String>,
    include_global: bool,
) -> anyhow::Result<MigrationCounts> {
    let mut counts = MigrationCounts::default();
    let owned_thread_ids = thread_owners
        .iter()
        .filter_map(|(thread_id, owner)| (owner == user_id).then_some(thread_id.clone()))
        .collect::<HashSet<_>>();

    if let Some(sqlite) = open_sqlite(sqlite_home, "state_5.sqlite").await? {
        counts.threads = copy_threads(postgres, schema_name, &sqlite, &owned_thread_ids).await?;
        counts.thread_dynamic_tools = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "thread_dynamic_tools",
                destination: "thread_dynamic_tools",
                columns: &[
                    "thread_id",
                    "position",
                    "name",
                    "description",
                    "input_schema",
                    "defer_loading",
                    "namespace",
                ],
                conflict_columns: &["thread_id", "position"],
                boolean_columns: &["defer_loading"],
                integer_columns: &["position"],
                owner_column: Some("thread_id"),
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        counts.thread_spawn_edges = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "thread_spawn_edges",
                destination: "thread_spawn_edges",
                columns: &["parent_thread_id", "child_thread_id", "status"],
                conflict_columns: &["child_thread_id"],
                boolean_columns: &[],
                integer_columns: &[],
                owner_column: Some("child_thread_id"),
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        counts.backfill_state = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "backfill_state",
                destination: "backfill_state",
                columns: &[
                    "id",
                    "status",
                    "last_watermark",
                    "last_success_at",
                    "updated_at",
                ],
                conflict_columns: &["id"],
                boolean_columns: &[],
                integer_columns: &["id", "last_success_at", "updated_at"],
                owner_column: None,
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        counts.remote_control_enrollments = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "remote_control_enrollments",
                destination: "remote_control_enrollments",
                columns: &[
                    "websocket_url",
                    "account_id",
                    "app_server_client_name",
                    "server_id",
                    "environment_id",
                    "server_name",
                    "updated_at",
                    "remote_control_enabled",
                ],
                conflict_columns: &["websocket_url", "account_id", "app_server_client_name"],
                boolean_columns: &["remote_control_enabled"],
                integer_columns: &["updated_at"],
                owner_column: None,
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        counts.external_agent_config_imports = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "external_agent_config_imports",
                destination: "external_agent_config_imports",
                columns: &["import_id", "completed_at_ms", "successes", "failures"],
                conflict_columns: &["import_id"],
                boolean_columns: &[],
                integer_columns: &["completed_at_ms"],
                owner_column: None,
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        counts.agent_jobs = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "agent_jobs",
                destination: "legacy_agent_jobs",
                columns: &[
                    "id",
                    "name",
                    "status",
                    "instruction",
                    "output_schema_json",
                    "input_headers_json",
                    "input_csv_path",
                    "output_csv_path",
                    "auto_export",
                    "created_at",
                    "updated_at",
                    "started_at",
                    "completed_at",
                    "last_error",
                    "max_runtime_seconds",
                ],
                conflict_columns: &["id"],
                boolean_columns: &["auto_export"],
                integer_columns: &[
                    "created_at",
                    "updated_at",
                    "started_at",
                    "completed_at",
                    "max_runtime_seconds",
                ],
                owner_column: None,
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        counts.agent_job_items = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "agent_job_items",
                destination: "legacy_agent_job_items",
                columns: &[
                    "job_id",
                    "item_id",
                    "row_index",
                    "source_id",
                    "row_json",
                    "status",
                    "assigned_thread_id",
                    "attempt_count",
                    "result_json",
                    "last_error",
                    "created_at",
                    "updated_at",
                    "completed_at",
                    "reported_at",
                ],
                conflict_columns: &["job_id", "item_id"],
                boolean_columns: &[],
                integer_columns: &[
                    "row_index",
                    "attempt_count",
                    "created_at",
                    "updated_at",
                    "completed_at",
                    "reported_at",
                ],
                owner_column: None,
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        sqlite.close().await;
    }

    if let Some(sqlite) = open_sqlite(sqlite_home, "goals_1.sqlite").await? {
        counts.thread_goals = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "thread_goals",
                destination: "thread_goals",
                columns: &[
                    "thread_id",
                    "goal_id",
                    "objective",
                    "status",
                    "token_budget",
                    "tokens_used",
                    "time_used_seconds",
                    "created_at_ms",
                    "updated_at_ms",
                ],
                conflict_columns: &["thread_id"],
                boolean_columns: &[],
                integer_columns: &[
                    "token_budget",
                    "tokens_used",
                    "time_used_seconds",
                    "created_at_ms",
                    "updated_at_ms",
                ],
                owner_column: Some("thread_id"),
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        counts.thread_goal_continuation_deferrals = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "thread_goal_continuation_deferrals",
                destination: "thread_goal_continuation_deferrals",
                columns: &["thread_id"],
                conflict_columns: &["thread_id"],
                boolean_columns: &[],
                integer_columns: &[],
                owner_column: Some("thread_id"),
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        sqlite.close().await;
    }

    if let Some(sqlite) = open_sqlite(sqlite_home, "memories_1.sqlite").await? {
        counts.stage1_outputs = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "stage1_outputs",
                destination: "stage1_outputs",
                columns: &[
                    "thread_id",
                    "source_updated_at",
                    "raw_memory",
                    "rollout_summary",
                    "rollout_slug",
                    "generated_at",
                    "usage_count",
                    "last_usage",
                    "selected_for_phase2",
                    "selected_for_phase2_source_updated_at",
                ],
                conflict_columns: &["thread_id"],
                boolean_columns: &["selected_for_phase2"],
                integer_columns: &[
                    "source_updated_at",
                    "generated_at",
                    "usage_count",
                    "last_usage",
                    "selected_for_phase2_source_updated_at",
                ],
                owner_column: Some("thread_id"),
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        counts.memory_jobs = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "jobs",
                destination: "jobs",
                columns: &[
                    "kind",
                    "job_key",
                    "status",
                    "worker_id",
                    "ownership_token",
                    "started_at",
                    "finished_at",
                    "lease_until",
                    "retry_at",
                    "retry_remaining",
                    "last_error",
                    "input_watermark",
                    "last_success_watermark",
                ],
                conflict_columns: &["kind", "job_key"],
                boolean_columns: &[],
                integer_columns: &[
                    "started_at",
                    "finished_at",
                    "lease_until",
                    "retry_at",
                    "retry_remaining",
                    "input_watermark",
                    "last_success_watermark",
                ],
                owner_column: None,
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        sqlite.close().await;
    }

    if let Some(sqlite) = open_sqlite(sqlite_home, "logs_2.sqlite").await? {
        counts.logs = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "logs",
                destination: "logs",
                columns: &[
                    "id",
                    "ts",
                    "ts_nanos",
                    "level",
                    "target",
                    "feedback_log_body",
                    "module_path",
                    "file",
                    "line",
                    "thread_id",
                    "process_uuid",
                    "estimated_bytes",
                ],
                conflict_columns: &["id"],
                boolean_columns: &[],
                integer_columns: &["id", "ts", "ts_nanos", "line", "estimated_bytes"],
                owner_column: Some("thread_id"),
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        sqlite.close().await;
    }

    if let Some(sqlite) = open_sqlite(sqlite_home, "thread_history_1.sqlite").await? {
        counts.thread_turns = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "thread_turns",
                destination: "thread_turns",
                columns: &[
                    "thread_id",
                    "turn_id",
                    "rollout_ordinal",
                    "status",
                    "error_json",
                    "started_at",
                    "completed_at",
                    "duration_ms",
                    "first_user_item_id",
                    "final_agent_item_id",
                    "rollout_byte_offset",
                    "rollout_end_ordinal",
                    "rollout_end_byte_offset",
                ],
                conflict_columns: &["thread_id", "turn_id"],
                boolean_columns: &[],
                integer_columns: &[
                    "rollout_ordinal",
                    "started_at",
                    "completed_at",
                    "duration_ms",
                    "rollout_byte_offset",
                    "rollout_end_ordinal",
                    "rollout_end_byte_offset",
                ],
                owner_column: Some("thread_id"),
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        counts.thread_items = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "thread_items",
                destination: "thread_items",
                columns: &[
                    "thread_id",
                    "turn_id",
                    "item_id",
                    "rollout_ordinal",
                    "created_at_ms",
                    "item_json",
                    "item_type",
                ],
                conflict_columns: &["thread_id", "turn_id", "item_id"],
                boolean_columns: &[],
                integer_columns: &["rollout_ordinal", "created_at_ms"],
                owner_column: Some("thread_id"),
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        counts.thread_history_projection_state = copy_simple_table(
            postgres,
            schema_name,
            &sqlite,
            TableCopy {
                source: "thread_history_projection_state",
                destination: "thread_history_projection_state",
                columns: &[
                    "thread_id",
                    "next_rollout_byte_offset",
                    "next_rollout_ordinal",
                ],
                conflict_columns: &["thread_id"],
                boolean_columns: &[],
                integer_columns: &["next_rollout_byte_offset", "next_rollout_ordinal"],
                owner_column: Some("thread_id"),
            },
            &owned_thread_ids,
            include_global,
        )
        .await?;
        sqlite.close().await;
    }

    Ok(counts)
}

pub(super) async fn verify_counts(
    postgres: &PgPool,
    schema_name: &str,
    expected: &MigrationCounts,
) -> anyhow::Result<()> {
    let checks = [
        ("threads", expected.threads),
        ("thread_dynamic_tools", expected.thread_dynamic_tools),
        ("thread_spawn_edges", expected.thread_spawn_edges),
        ("backfill_state", expected.backfill_state),
        (
            "remote_control_enrollments",
            expected.remote_control_enrollments,
        ),
        (
            "external_agent_config_imports",
            expected.external_agent_config_imports,
        ),
        ("legacy_agent_jobs", expected.agent_jobs),
        ("legacy_agent_job_items", expected.agent_job_items),
        ("thread_goals", expected.thread_goals),
        (
            "thread_goal_continuation_deferrals",
            expected.thread_goal_continuation_deferrals,
        ),
        ("stage1_outputs", expected.stage1_outputs),
        ("jobs", expected.memory_jobs),
        ("logs", expected.logs),
        ("thread_turns", expected.thread_turns),
        ("thread_items", expected.thread_items),
        (
            "thread_history_projection_state",
            expected.thread_history_projection_state,
        ),
    ];

    for (table_name, expected_count) in checks {
        let sql = format!(
            "SELECT COUNT(*) FROM {}",
            schema::table(schema_name, table_name)
        );
        let actual: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .fetch_one(postgres)
            .await?;
        if u64::try_from(actual)? != expected_count {
            anyhow::bail!(
                "count mismatch for {table_name}: source={expected_count}, destination={actual}"
            );
        }
    }
    Ok(())
}

async fn open_sqlite(home: &Path, filename: &str) -> anyhow::Result<Option<SqlitePool>> {
    let path = home.join(filename);
    if !tokio::fs::try_exists(&path).await? {
        return Ok(None);
    }
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .read_only(true)
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .with_context(|| format!("open source SQLite database {}", path.display()))?;
    Ok(Some(pool))
}

async fn copy_threads(
    postgres: &PgPool,
    schema_name: &str,
    sqlite: &SqlitePool,
    owned_thread_ids: &HashSet<String>,
) -> anyhow::Result<u64> {
    if !table_exists(sqlite, "threads").await? {
        return Ok(0);
    }
    let columns = [
        "id",
        "rollout_path",
        "created_at",
        "updated_at",
        "source",
        "model_provider",
        "cwd",
        "title",
        "sandbox_policy",
        "approval_mode",
        "tokens_used",
        "has_user_event",
        "archived",
        "archived_at",
        "git_sha",
        "git_branch",
        "git_origin_url",
        "cli_version",
        "first_user_message",
        "agent_nickname",
        "agent_role",
        "memory_mode",
        "model",
        "reasoning_effort",
        "agent_path",
        "created_at_ms",
        "updated_at_ms",
        "thread_source",
        "preview",
        "recency_at",
        "recency_at_ms",
        "history_mode",
        "name",
        "is_pinned",
    ];
    copy_simple_table(
        postgres,
        schema_name,
        sqlite,
        TableCopy {
            source: "threads",
            destination: "threads",
            columns: &columns,
            conflict_columns: &["id"],
            boolean_columns: &["has_user_event", "archived", "is_pinned"],
            integer_columns: &[
                "created_at",
                "updated_at",
                "tokens_used",
                "archived_at",
                "created_at_ms",
                "updated_at_ms",
                "recency_at",
                "recency_at_ms",
            ],
            owner_column: Some("id"),
        },
        owned_thread_ids,
        false,
    )
    .await
}

struct TableCopy<'a> {
    source: &'a str,
    destination: &'a str,
    columns: &'a [&'a str],
    conflict_columns: &'a [&'a str],
    boolean_columns: &'a [&'a str],
    integer_columns: &'a [&'a str],
    owner_column: Option<&'a str>,
}

async fn copy_simple_table(
    postgres: &PgPool,
    schema_name: &str,
    sqlite: &SqlitePool,
    table: TableCopy<'_>,
    owned_thread_ids: &HashSet<String>,
    include_global: bool,
) -> anyhow::Result<u64> {
    if !table_exists(sqlite, table.source).await? {
        return Ok(0);
    }

    let select_columns = select_columns(sqlite, table.source, table.columns).await?;
    let select_sql = format!(
        "SELECT {} FROM \"{}\"",
        select_columns.join(", "),
        table.source
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(select_sql))
        .fetch_all(sqlite)
        .await?;
    let mut selected_rows = Vec::new();
    for row in rows {
        if row_belongs_to_user(&row, table.owner_column, owned_thread_ids, include_global)? {
            selected_rows.push(row);
        }
    }
    let rows = selected_rows;
    if rows.is_empty() {
        return Ok(0);
    }
    let selected_count = rows.len();

    let destination = schema::table(schema_name, table.destination);
    let all_columns = table.columns.to_vec();
    let parameters = (1..=all_columns.len())
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let conflict_columns = table.conflict_columns.to_vec();
    let updates = table
        .columns
        .iter()
        .filter(|column| !table.conflict_columns.contains(column))
        .map(|column| format!("\"{column}\" = EXCLUDED.\"{column}\""))
        .collect::<Vec<_>>();
    let conflict_action = if updates.is_empty() {
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {}", updates.join(", "))
    };
    let insert_sql = format!(
        "INSERT INTO {destination} ({}) VALUES ({parameters}) ON CONFLICT ({}) {conflict_action}",
        quoted(&all_columns),
        quoted(&conflict_columns),
    );

    let boolean_columns = table.boolean_columns;
    let mut transaction = postgres.begin().await?;
    for row in rows {
        let mut query = sqlx::query(sqlx::AssertSqlSafe(insert_sql.clone()));
        for column in table.columns {
            if boolean_columns.contains(column) {
                let value: Option<i64> = row.try_get(*column)?;
                query = query.bind(value.map(|item| item != 0));
                continue;
            }
            if table.integer_columns.contains(column) {
                let value: Option<i64> = row.try_get(*column)?;
                query = query.bind(value);
                continue;
            }
            let value: Option<String> = row.try_get(*column)?;
            query = query.bind(value);
        }
        query.execute(&mut *transaction).await?;
    }
    transaction.commit().await?;
    Ok(u64::try_from(selected_count)?)
}

fn row_belongs_to_user(
    row: &sqlx::sqlite::SqliteRow,
    owner_column: Option<&str>,
    owned_thread_ids: &HashSet<String>,
    include_global: bool,
) -> anyhow::Result<bool> {
    let Some(owner_column) = owner_column else {
        return Ok(include_global);
    };
    let owner: Option<String> = row.try_get(owner_column)?;
    Ok(match owner {
        Some(owner) => owned_thread_ids.contains(&owner),
        None => include_global,
    })
}

async fn select_columns(
    sqlite: &SqlitePool,
    table: &str,
    columns: &[&str],
) -> anyhow::Result<Vec<String>> {
    let existing = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(format!(
        "SELECT name FROM pragma_table_info('{table}')"
    )))
    .fetch_all(sqlite)
    .await?;
    Ok(columns
        .iter()
        .map(|column| {
            if existing.iter().any(|item| item == column) {
                format!("\"{column}\"")
            } else {
                if table == "threads" && *column == "is_pinned" {
                    "0 AS \"is_pinned\"".to_string()
                } else {
                    format!("NULL AS \"{column}\"")
                }
            }
        })
        .collect())
}

async fn table_exists(sqlite: &SqlitePool, table: &str) -> anyhow::Result<bool> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
    )
    .bind(table)
    .fetch_optional(sqlite)
    .await?
    .is_some())
}

fn quoted(columns: &[&str]) -> String {
    columns
        .iter()
        .map(|column| format!("\"{column}\""))
        .collect::<Vec<_>>()
        .join(", ")
}
