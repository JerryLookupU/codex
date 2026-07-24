use super::LocalThreadStore;
use crate::CreateThreadParams;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use chrono::SecondsFormat;
use chrono::Utc;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::ThreadMemoryMode;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutRecorder;
use codex_rollout::RolloutRecorderParams;

pub(super) async fn create_thread(
    store: &LocalThreadStore,
    params: CreateThreadParams,
) -> ThreadStoreResult<RolloutRecorder> {
    let cwd = params
        .metadata
        .cwd
        .clone()
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: "local thread store requires a cwd".to_string(),
        })?;
    let config = RolloutConfig {
        codex_home: store.config.codex_home.clone(),
        sqlite: store.config.sqlite.clone(),
        cwd,
        model_provider_id: params.metadata.model_provider.clone(),
        generate_memories: matches!(params.metadata.memory_mode, ThreadMemoryMode::Enabled),
    };
    RolloutRecorder::new(
        &config,
        RolloutRecorderParams::new(
            params.thread_id,
            params.forked_from_id,
            params.parent_thread_id,
            params.source,
            params.thread_source,
            params.originator,
            params.base_instructions,
            params.dynamic_tools,
        )
        .with_session_id(params.session_id)
        .with_selected_capability_roots(params.selected_capability_roots)
        .with_multi_agent_version(params.multi_agent_version)
        .with_history_mode(params.history_mode)
        .with_subagent_history_start_ordinal(params.subagent_history_start_ordinal)
        .with_initial_window_id(params.initial_window_id),
    )
    .await
    .map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to initialize local thread recorder: {err}"),
    })
}

pub(super) async fn create_postgres_thread(
    store: &LocalThreadStore,
    params: CreateThreadParams,
) -> ThreadStoreResult<()> {
    let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let session_meta = SessionMetaLine {
        meta: SessionMeta {
            session_id: params.session_id,
            id: params.thread_id,
            forked_from_id: params.forked_from_id,
            parent_thread_id: params.parent_thread_id,
            timestamp,
            cwd: params.metadata.cwd.unwrap_or_default(),
            originator: params.originator,
            cli_version: env!("CARGO_PKG_VERSION").to_string(),
            agent_nickname: params.source.get_nickname(),
            agent_role: params.source.get_agent_role(),
            agent_path: params.source.get_agent_path().map(Into::into),
            source: params.source,
            thread_source: params.thread_source,
            model_provider: Some(params.metadata.model_provider),
            base_instructions: Some(params.base_instructions),
            dynamic_tools: (!params.dynamic_tools.is_empty()).then_some(params.dynamic_tools),
            selected_capability_roots: params.selected_capability_roots,
            memory_mode: matches!(params.metadata.memory_mode, ThreadMemoryMode::Disabled)
                .then_some("disabled".to_string()),
            history_mode: params.history_mode,
            history_base: None,
            subagent_history_start_ordinal: params.subagent_history_start_ordinal,
            multi_agent_version: params.multi_agent_version,
            context_window: Some(codex_protocol::protocol::SessionContextWindow::new(
                params.initial_window_id,
            )),
        },
        git: None,
    };
    store
        .insert_postgres_live_recorder(params.thread_id, Some(session_meta), params.history_mode)
        .await
}
