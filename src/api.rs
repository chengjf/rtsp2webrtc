use crate::config::Config;
use crate::stream::StreamManager;
use axum::extract::{Path, State};
use axum::Json;
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct ApiState {
    pub stream_manager: Arc<StreamManager>,
    pub config: Config,
    pub start_time: Instant,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub uptime_secs: u64,
    pub active_streams: usize,
    pub total_peers: usize,
}

#[derive(Serialize)]
pub struct StreamsResponse {
    pub streams: Vec<crate::stream::StreamSummary>,
}

pub async fn health(State(state): State<ApiState>) -> Json<HealthResponse> {
    let streams = state.stream_manager.list_streams().await;
    let active = streams.iter().filter(|s| s.connected).count();

    Json(HealthResponse {
        status: "ok",
        uptime_secs: state.start_time.elapsed().as_secs(),
        active_streams: active,
        total_peers: state.stream_manager.total_peers(),
    })
}

pub async fn list_streams(State(state): State<ApiState>) -> Json<StreamsResponse> {
    Json(StreamsResponse {
        streams: state.stream_manager.list_streams().await,
    })
}

pub async fn stream_detail(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<crate::stream::StreamDetail>, String> {
    state
        .stream_manager
        .stream_info(&id)
        .await
        .map(Json)
        .ok_or_else(|| "stream not found".into())
}
