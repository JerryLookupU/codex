use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use codex_app_server_protocol::ThreadHistoryChangeSet;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::TurnStatus;
use codex_protocol::ThreadId;
use codex_protocol::models::MessagePhase;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use codex_protocol::protocol::SessionMetaLine;

use super::LocalThreadStore;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

mod read;
mod search;
mod segment_paging;

pub(super) use read::list_items;
pub(super) use read::list_turns;
pub(super) use search::search_thread_occurrences;

pub(super) async fn append_rollout_items(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    items: Vec<RolloutItem>,
) -> ThreadStoreResult<()> {
    if items.is_empty() {
        return Ok(());
    }
    let start_offset = next_rollout_byte_offset(store, thread_id).await?;
    let first_ordinal = next_rollout_ordinal(store, thread_id).await?;
    let mut next_offset = start_offset;
    let mut projections = Vec::with_capacity(items.len());
    for (index, item) in items.into_iter().enumerate() {
        let ordinal = first_ordinal
            .checked_add(u64::try_from(index).map_err(thread_history_error)?)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "PostgreSQL rollout ordinal overflow".to_string(),
            })?;
        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let line = RolloutLine {
            timestamp,
            ordinal: Some(ordinal),
            item,
        };
        let raw_json = serde_json::to_string(&line).map_err(thread_history_error)?;
        let start = next_offset;
        next_offset = next_offset
            .checked_add(u64::try_from(raw_json.len() + 1).map_err(thread_history_error)?)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "PostgreSQL rollout byte position overflow".to_string(),
            })?;
        let created_at_ms = DateTime::parse_from_rfc3339(line.timestamp.as_str())
            .map(|value| value.timestamp_millis())
            .map_err(thread_history_error)?;
        projections.push(ProjectedRolloutLine {
            ordinal,
            start_byte_offset: start,
            end_byte_offset: next_offset,
            created_at_ms,
            line_type: rollout_line_type(&line),
            payload_type: rollout_payload_type(&line),
            raw_json,
            changes: codex_app_server_protocol::project_rollout_line(&line),
        });
    }
    apply_projection(store, thread_id, start_offset, next_offset, projections).await
}

pub(super) async fn load_rollout_items(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Vec<RolloutItem>> {
    let pool = store.thread_history_db().await?;
    let rows = codex_state::db::query(
        "SELECT CAST(raw_json AS TEXT) AS raw_json FROM thread_rollout_events WHERE thread_id = ? ORDER BY rollout_ordinal",
    )
    .bind(thread_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(thread_history_error)?;
    rows.into_iter()
        .map(|row| {
            use sqlx::Row;
            let raw = row
                .try_get::<String, _>("raw_json")
                .map_err(thread_history_error)?;
            parse_postgres_rollout_line(&raw)
                .map(|line| line.item)
                .map_err(thread_history_error)
        })
        .collect()
}

pub(super) async fn load_session_meta(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<Option<SessionMetaLine>> {
    let pool = store.thread_history_db().await?;
    let row = codex_state::db::query(
        "SELECT CAST(raw_json AS TEXT) AS raw_json FROM thread_rollout_events WHERE thread_id = ? AND line_type = 'session_meta' ORDER BY rollout_ordinal LIMIT 1",
    )
    .bind(thread_id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(thread_history_error)?;
    row.map(|row| {
        use sqlx::Row;
        let raw = row
            .try_get::<String, _>("raw_json")
            .map_err(thread_history_error)?;
        let line = parse_postgres_rollout_line(&raw).map_err(thread_history_error)?;
        match line.item {
            RolloutItem::SessionMeta(meta) => Ok(meta),
            _ => Err(ThreadStoreError::Internal {
                message: format!(
                    "thread {thread_id} has an invalid PostgreSQL session metadata row"
                ),
            }),
        }
    })
    .transpose()
}

fn parse_postgres_rollout_line(raw: &str) -> serde_json::Result<RolloutLine> {
    let value = serde_json::from_str::<serde_json::Value>(raw)?;
    let object = value.as_object().ok_or_else(|| {
        serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "PostgreSQL rollout row is not a JSON object",
        ))
    })?;
    let null = serde_json::Value::Null;
    let timestamp = object
        .get("timestamp")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "PostgreSQL rollout row has no timestamp",
            ))
        })?
        .to_string();
    let ordinal = object.get("ordinal").and_then(serde_json::Value::as_u64);
    let line_type = object.get("type").unwrap_or(&null);
    let payload = object.get("payload").unwrap_or(&null);
    let item_json = format!(
        r#"{{"type":{},"payload":{}}}"#,
        serde_json::to_string(line_type)?,
        serde_json::to_string(payload)?,
    );
    let item = serde_json::from_str::<RolloutItem>(&item_json)?;
    Ok(RolloutLine {
        timestamp,
        ordinal,
        item,
    })
}

fn rollout_line_type(line: &RolloutLine) -> String {
    serde_json::to_value(line)
        .ok()
        .and_then(|value| value.get("type")?.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_string())
}

fn rollout_payload_type(line: &RolloutLine) -> Option<String> {
    serde_json::to_value(line).ok().and_then(|value| {
        value
            .get("payload")
            .and_then(|payload| payload.get("type"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    })
}

/// A valid complete rollout line with its absolute byte span in durable JSONL.
///
/// `start_byte_offset..end_byte_offset` includes the terminating newline. Blank and rejected
/// lines do not produce a value here, but still advance later spans.
pub(super) struct ProjectedRolloutLine {
    pub ordinal: u64,
    pub start_byte_offset: u64,
    pub end_byte_offset: u64,
    pub created_at_ms: i64,
    pub line_type: String,
    pub payload_type: Option<String>,
    pub raw_json: String,
    pub changes: ThreadHistoryChangeSet,
}

pub(super) async fn next_rollout_byte_offset(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<u64> {
    if std::env::var("CODEX_STATE_DATABASE_URL").is_err() {
        let db_path = store.config.sqlite.thread_history_db_path();
        if !tokio::fs::try_exists(db_path.as_path())
            .await
            .map_err(thread_history_error)?
        {
            return Ok(0);
        }
    }

    let pool = store.thread_history_db().await?;
    let offset = codex_state::db::query_scalar::<i64>(
        "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(thread_history_error)?
    .unwrap_or(0);
    u64::try_from(offset).map_err(|_| ThreadStoreError::Internal {
        message: format!("thread history projection for {thread_id} has a negative byte offset"),
    })
}

pub(super) async fn next_rollout_ordinal(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<u64> {
    let pool = store.thread_history_db().await?;
    let ordinal = codex_state::db::query_scalar::<i64>(
        "SELECT next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id = ?",
    )
    .bind(thread_id.to_string())
    .fetch_optional(pool)
    .await
    .map_err(thread_history_error)?
    .unwrap_or(0);
    u64::try_from(ordinal).map_err(|_| ThreadStoreError::Internal {
        message: format!("thread history projection for {thread_id} has a negative ordinal"),
    })
}

pub(super) async fn apply_projection(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    start_offset: u64,
    next_offset: u64,
    projections: Vec<ProjectedRolloutLine>,
) -> ThreadStoreResult<()> {
    let pool = store.thread_history_db().await?;
    // Write the projected rows and advance the JSONL offset and ordinal in one transaction. If
    // SQLite fails, it stays behind the durable rollout instead of claiming data it did not
    // materialize.
    let mut transaction = pool.begin().await.map_err(thread_history_error)?;
    let thread_id = thread_id.to_string();
    let projection_state = codex_state::db::query_as::<(i64, i64)>(
        r#"
SELECT next_rollout_byte_offset, next_rollout_ordinal
FROM thread_history_projection_state
WHERE thread_id = ?
        "#,
    )
    .bind(thread_id.as_str())
    .fetch_optional(&mut *transaction)
    .await
    .map_err(thread_history_error)?;
    let (expected_offset, mut next_ordinal) = projection_state.unwrap_or((0, 0));
    let start_offset = sqlite_integer(start_offset, "rollout byte offset")?;
    if expected_offset != start_offset {
        return Err(ThreadStoreError::Internal {
            message: format!("thread history projection for {thread_id} is behind durable rollout"),
        });
    }

    for projection in projections {
        let ordinal = sqlite_integer(projection.ordinal, "rollout ordinal")?;
        if ordinal != next_ordinal {
            return Err(ThreadStoreError::Internal {
                message: format!(
                    "thread history projection for {thread_id} expected ordinal {next_ordinal}, got {ordinal}"
                ),
            });
        }
        apply_change_set(
            &mut transaction,
            thread_id.as_str(),
            ordinal,
            sqlite_integer(projection.start_byte_offset, "rollout byte offset")?,
            sqlite_integer(projection.end_byte_offset, "rollout byte offset")?,
            projection.created_at_ms,
            projection.changes,
        )
        .await?;
        if std::env::var("CODEX_STATE_DATABASE_URL").is_ok() {
            codex_state::db::query(
                r#"
INSERT INTO thread_rollout_events (
    thread_id,
    rollout_ordinal,
    rollout_byte_offset,
    rollout_end_byte_offset,
    created_at_ms,
    line_type,
    payload_type,
    raw_json
) VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS JSONB))
ON CONFLICT(thread_id, rollout_ordinal) DO UPDATE SET
    rollout_byte_offset = excluded.rollout_byte_offset,
    rollout_end_byte_offset = excluded.rollout_end_byte_offset,
    created_at_ms = excluded.created_at_ms,
    line_type = excluded.line_type,
    payload_type = excluded.payload_type,
    raw_json = excluded.raw_json
                "#,
            )
            .bind(thread_id.as_str())
            .bind(ordinal)
            .bind(sqlite_integer(
                projection.start_byte_offset,
                "rollout byte offset",
            )?)
            .bind(sqlite_integer(
                projection.end_byte_offset,
                "rollout byte offset",
            )?)
            .bind(projection.created_at_ms)
            .bind(projection.line_type)
            .bind(projection.payload_type)
            .bind(projection.raw_json)
            .execute(&mut *transaction)
            .await
            .map_err(thread_history_error)?;
        }
        next_ordinal = next_ordinal
            .checked_add(1)
            .ok_or_else(|| ThreadStoreError::Internal {
                message: "rollout ordinal exceeds SQLite integer range".to_string(),
            })?;
    }

    codex_state::db::query(
        r#"
INSERT INTO thread_history_projection_state (
    thread_id,
    next_rollout_byte_offset,
    next_rollout_ordinal
) VALUES (?, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
    next_rollout_byte_offset = excluded.next_rollout_byte_offset,
    next_rollout_ordinal = excluded.next_rollout_ordinal
        "#,
    )
    .bind(thread_id.as_str())
    .bind(sqlite_integer(next_offset, "rollout byte offset")?)
    .bind(next_ordinal)
    .execute(&mut *transaction)
    .await
    .map_err(thread_history_error)?;
    transaction.commit().await.map_err(thread_history_error)
}

pub(super) async fn delete_thread(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<()> {
    if std::env::var("CODEX_STATE_DATABASE_URL").is_err() {
        let db_path = store.config.sqlite.thread_history_db_path();
        if !tokio::fs::try_exists(db_path.as_path())
            .await
            .map_err(thread_history_delete_error)?
        {
            return Ok(());
        }
    }

    let pool = store.thread_history_db().await?;
    let mut transaction = pool.begin().await.map_err(thread_history_delete_error)?;
    let thread_id = thread_id.to_string();
    codex_state::db::query("DELETE FROM thread_items WHERE thread_id = ?")
        .bind(thread_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(thread_history_delete_error)?;
    codex_state::db::query("DELETE FROM thread_turns WHERE thread_id = ?")
        .bind(thread_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(thread_history_delete_error)?;
    codex_state::db::query("DELETE FROM thread_history_projection_state WHERE thread_id = ?")
        .bind(thread_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(thread_history_delete_error)?;
    transaction
        .commit()
        .await
        .map_err(thread_history_delete_error)
}

async fn apply_change_set(
    transaction: &mut sqlx::Transaction<'_, codex_state::db::Any>,
    thread_id: &str,
    rollout_ordinal: i64,
    rollout_byte_offset: i64,
    rollout_end_byte_offset: i64,
    created_at_ms: i64,
    changes: ThreadHistoryChangeSet,
) -> ThreadStoreResult<()> {
    for turn in changes.changed_turns {
        let turn_id = turn.turn_id;
        let error_json = turn
            .error
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(thread_history_error)?;
        let (terminal_ordinal, terminal_byte_offset) = match &turn.status {
            TurnStatus::Completed | TurnStatus::Interrupted | TurnStatus::Failed => {
                (Some(rollout_ordinal), Some(rollout_end_byte_offset))
            }
            TurnStatus::InProgress => (None, None),
        };
        // The same turn can appear again as it moves from started to completed. Update its latest
        // status, error, and timestamps, but keep the rollout ordinal from the first record that
        // created it.
        codex_state::db::query(
            r#"
INSERT INTO thread_turns (
    thread_id,
    turn_id,
    rollout_ordinal,
    rollout_byte_offset,
    rollout_end_ordinal,
    rollout_end_byte_offset,
    status,
    error_json,
    started_at,
    completed_at,
    duration_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(thread_id, turn_id) DO UPDATE SET
    rollout_end_ordinal = excluded.rollout_end_ordinal,
    rollout_end_byte_offset = excluded.rollout_end_byte_offset,
    status = excluded.status,
    error_json = excluded.error_json,
    started_at = excluded.started_at,
    completed_at = excluded.completed_at,
    duration_ms = excluded.duration_ms
WHERE thread_turns.rollout_end_ordinal IS NULL
  AND thread_turns.status = 'inProgress'
            "#,
        )
        .bind(thread_id)
        .bind(turn_id.as_str())
        .bind(rollout_ordinal)
        .bind(rollout_byte_offset)
        .bind(terminal_ordinal)
        .bind(terminal_byte_offset)
        .bind(turn_status(&turn.status))
        .bind(error_json)
        .bind(turn.started_at)
        .bind(turn.completed_at)
        .bind(turn.duration_ms)
        .execute(&mut **transaction)
        .await
        .map_err(thread_history_error)?;

        // Review turns can persist completed items before their turn lifecycle record. Fill the
        // summary IDs from those older item rows when the turn row finally arrives.
        codex_state::db::query(
            r#"
UPDATE thread_turns
SET
    first_user_item_id = COALESCE(
        first_user_item_id,
        (
            SELECT item_id
            FROM thread_items
            WHERE thread_id = ?
              AND turn_id = ?
              AND item_type = 'userMessage'
            ORDER BY rollout_ordinal
            LIMIT 1
        )
    ),
    final_agent_item_id = COALESCE(
        (
            SELECT item_id
            FROM thread_items
            WHERE thread_id = ?
              AND turn_id = ?
              AND item_type = 'agentMessage'
              AND item_json LIKE '%"phase":"final_answer"%'
            ORDER BY rollout_ordinal DESC
            LIMIT 1
        ),
        CASE
            WHEN status IN ('completed', 'interrupted', 'failed') THEN (
                SELECT item_id
                FROM thread_items
                WHERE thread_id = ?
                  AND turn_id = ?
                  AND item_type = 'agentMessage'
                  AND (
                    item_json LIKE '%"phase":null%'
                    OR item_json NOT LIKE '%"phase":%'
                  )
                ORDER BY rollout_ordinal DESC
                LIMIT 1
            )
        END,
        final_agent_item_id
    )
WHERE thread_id = ?
  AND turn_id = ?
  AND (
    rollout_end_ordinal = ?
    OR status = 'inProgress'
  )
            "#,
        )
        .bind(thread_id)
        .bind(turn_id.as_str())
        .bind(thread_id)
        .bind(turn_id.as_str())
        .bind(thread_id)
        .bind(turn_id.as_str())
        .bind(thread_id)
        .bind(turn_id.as_str())
        .bind(rollout_ordinal)
        .execute(&mut **transaction)
        .await
        .map_err(thread_history_error)?;
    }

    for item in changes.changed_items {
        let item_id = item.item.id().to_string();
        let item_type = match &item.item {
            ThreadItem::UserMessage { .. } => "userMessage",
            ThreadItem::AgentMessage { .. } => "agentMessage",
            _ => "",
        };
        let item_json = serde_json::to_string(&item.item).map_err(thread_history_error)?;
        // The same item can appear again with a newer snapshot. Replace its JSON, but keep the
        // creation ordinal and timestamp from the first record so item identity and age stay
        // stable. Track the latest snapshot separately for incremental replay.
        codex_state::db::query(
            r#"
INSERT INTO thread_items (
    thread_id,
    turn_id,
    item_id,
    rollout_ordinal,
    updated_at_ordinal,
    created_at_ms,
    item_type,
    item_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(thread_id, turn_id, item_id) DO UPDATE SET
    updated_at_ordinal = excluded.updated_at_ordinal,
    item_type = excluded.item_type,
    item_json = excluded.item_json
            "#,
        )
        .bind(thread_id)
        .bind(item.turn_id.as_str())
        .bind(item_id.as_str())
        .bind(rollout_ordinal)
        .bind(rollout_ordinal)
        .bind(created_at_ms)
        .bind(item_type)
        .bind(item_json)
        .execute(&mut **transaction)
        .await
        .map_err(thread_history_error)?;

        // Keep summary item IDs on the turn row so reads do not need to scan every item in the
        // turn.
        match item.item {
            ThreadItem::UserMessage { .. } => {
                codex_state::db::query(
                    r#"
UPDATE thread_turns
SET first_user_item_id = COALESCE(first_user_item_id, ?)
WHERE thread_id = ?
  AND turn_id = ?
  AND rollout_end_ordinal IS NULL
  AND status = 'inProgress'
                    "#,
                )
                .bind(item_id.as_str())
                .bind(thread_id)
                .bind(item.turn_id.as_str())
                .execute(&mut **transaction)
                .await
                .map_err(thread_history_error)?;
            }
            ThreadItem::AgentMessage {
                phase: Some(MessagePhase::FinalAnswer),
                ..
            } => {
                codex_state::db::query(
                    r#"
UPDATE thread_turns
SET final_agent_item_id = ?
WHERE thread_id = ?
  AND turn_id = ?
  AND rollout_end_ordinal IS NULL
  AND status = 'inProgress'
                    "#,
                )
                .bind(item_id.as_str())
                .bind(thread_id)
                .bind(item.turn_id.as_str())
                .execute(&mut **transaction)
                .await
                .map_err(thread_history_error)?;
            }
            ThreadItem::AgentMessage {
                phase: Some(MessagePhase::Commentary) | None,
                ..
            }
            | ThreadItem::HookPrompt { .. }
            | ThreadItem::Plan { .. }
            | ThreadItem::Reasoning { .. }
            | ThreadItem::CommandExecution { .. }
            | ThreadItem::FileChange { .. }
            | ThreadItem::McpToolCall { .. }
            | ThreadItem::DynamicToolCall { .. }
            | ThreadItem::CollabAgentToolCall { .. }
            | ThreadItem::SubAgentActivity { .. }
            | ThreadItem::WebSearch(_)
            | ThreadItem::ImageView { .. }
            | ThreadItem::Sleep(_)
            | ThreadItem::ImageGeneration(_)
            | ThreadItem::EnteredReviewMode { .. }
            | ThreadItem::ExitedReviewMode { .. }
            | ThreadItem::ContextCompaction { .. } => {}
        }
    }
    Ok(())
}

fn turn_status(status: &TurnStatus) -> &'static str {
    match status {
        TurnStatus::Completed => "completed",
        TurnStatus::Interrupted => "interrupted",
        TurnStatus::Failed => "failed",
        TurnStatus::InProgress => "inProgress",
    }
}

fn sqlite_integer(value: u64, field: &str) -> ThreadStoreResult<i64> {
    i64::try_from(value).map_err(|_| ThreadStoreError::Internal {
        message: format!("{field} exceeds SQLite integer range"),
    })
}

fn thread_history_error(err: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to access thread history: {err}"),
    }
}

impl From<sqlx::Error> for ThreadStoreError {
    fn from(err: sqlx::Error) -> Self {
        thread_history_error(err)
    }
}

fn thread_history_delete_error(err: impl std::fmt::Display) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("failed to delete thread history: {err}"),
    }
}

#[cfg(test)]
mod postgres_rollout_tests {
    use super::parse_postgres_rollout_line;
    use codex_protocol::protocol::EventMsg;
    use codex_protocol::protocol::RolloutItem;

    #[test]
    fn parses_jsonb_reordered_token_count_with_float() {
        let raw = r#"{
            "type": "event_msg",
            "ordinal": 15,
            "payload": {
                "info": null,
                "type": "token_count",
                "rate_limits": {
                    "credits": null,
                    "primary": {
                        "resets_at": null,
                        "used_percent": 33.0,
                        "window_minutes": 10080
                    },
                    "limit_id": "codex",
                    "plan_type": null,
                    "secondary": null,
                    "limit_name": null,
                    "individual_limit": null,
                    "spend_control_reached": null,
                    "rate_limit_reached_type": null
                }
            },
            "timestamp": "2026-07-23T18:27:14.448Z"
        }"#;

        let line = parse_postgres_rollout_line(raw).expect("parse PostgreSQL JSONB rollout");
        let RolloutItem::EventMsg(EventMsg::TokenCount(event)) = line.item else {
            panic!("expected token count event");
        };
        assert_eq!(
            event
                .rate_limits
                .and_then(|snapshot| snapshot.primary)
                .map(|window| window.used_percent),
            Some(33.0)
        );
    }
}
