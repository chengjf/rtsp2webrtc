use crate::config::Config;
use crate::stream::StreamManager;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::{Json, Router};
use axum::routing::get;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

#[derive(Clone)]
pub struct ApiState {
    pub stream_manager: Arc<StreamManager>,
    pub config: Config,
    pub start_time: Instant,
    pub last_create: Arc<Mutex<Instant>>,
}

// ── Request types ──

#[derive(Deserialize)]
pub struct CreateStreamRequest {
    pub url: String,
}

// ── Response types ──

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub uptime_secs: u64,
    pub configured_streams: usize,
    pub active_streams: usize,
    pub total_peers: usize,
}

#[derive(Serialize)]
pub struct StreamItem {
    pub id: String,
    pub name: String,
    pub url: String,
    pub dynamic: bool,
    pub subscribers: usize,
    pub connected: bool,
    pub codec: Option<String>,
    pub payload_type: Option<u8>,
}

#[derive(Serialize)]
pub struct StreamsResponse {
    pub streams: Vec<StreamItem>,
}

#[derive(Serialize)]
pub struct CreateStreamResponse {
    pub stream_id: String,
}

// ── Router ──

pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/health", get(health))
        .route("/api/streams", get(list_streams))
        .route("/api/streams", axum::routing::post(create_stream))
        .route("/api/streams/{id}", get(stream_detail))
        .route("/api/streams/{id}", axum::routing::delete(delete_stream))
}

// ── Handlers ──

pub async fn health(State(state): State<ApiState>) -> Json<HealthResponse> {
    let active = state.stream_manager.list_streams().await;
    Json(HealthResponse {
        status: "ok",
        uptime_secs: state.start_time.elapsed().as_secs(),
        configured_streams: state.config.streams.len(),
        active_streams: active.iter().filter(|s| s.connected).count(),
        total_peers: state.stream_manager.total_peers(),
    })
}

pub async fn list_streams(State(state): State<ApiState>) -> Json<StreamsResponse> {
    let runtime = state.stream_manager.list_streams().await;

    // Start with configured streams
    let mut items: Vec<StreamItem> = state
        .config
        .streams
        .iter()
        .map(|cfg| {
            let rt = runtime.iter().find(|r| r.id == cfg.id);
            StreamItem {
                id: cfg.id.clone(),
                name: if cfg.name.is_empty() { cfg.id.clone() } else { cfg.name.clone() },
                url: mask_url(&cfg.url),
                dynamic: false,
                subscribers: rt.map(|r| r.subscribers).unwrap_or(0),
                connected: rt.map(|r| r.connected).unwrap_or(false),
                codec: rt.and(Some("h264".to_string())),
                payload_type: rt.and(Some(96u8)),
            }
        })
        .collect();

    // Add dynamic streams (not in config)
    for rt in &runtime {
        if !state.config.streams.iter().any(|cfg| cfg.id == rt.id) {
            items.push(StreamItem {
                id: rt.id.clone(),
                name: format!("Dynamic ({})", &rt.id[..8]),
                url: "".to_string(),
                dynamic: true,
                subscribers: rt.subscribers,
                connected: rt.connected,
                codec: Some("h264".to_string()),
                payload_type: Some(96u8),
            });
        }
    }

    Json(StreamsResponse { streams: items })
}

pub async fn stream_detail(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<StreamItem>, String> {
    let runtime = state.stream_manager.list_streams().await;
    let rt = runtime.iter().find(|r| r.id == id);

    if let Some(cfg) = state.config.find_stream(&id) {
        return Ok(Json(StreamItem {
            id: cfg.id.clone(),
            name: if cfg.name.is_empty() { cfg.id.clone() } else { cfg.name.clone() },
            url: mask_url(&cfg.url),
            dynamic: false,
            subscribers: rt.map(|r| r.subscribers).unwrap_or(0),
            connected: rt.map(|r| r.connected).unwrap_or(false),
            codec: rt.and(Some("h264".to_string())),
            payload_type: rt.and(Some(96u8)),
        }));
    }

    if let Some(rt) = rt {
        return Ok(Json(StreamItem {
            id: rt.id.clone(),
            name: format!("Dynamic ({})", &rt.id[..8]),
            url: "".to_string(),
            dynamic: true,
            subscribers: rt.subscribers,
            connected: rt.connected,
            codec: Some("h264".to_string()),
            payload_type: Some(96u8),
        }));
    }

    Err("stream not found".to_string())
}

/// POST /api/streams — Create a dynamic stream from an RTSP URL.
pub async fn create_stream(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Json(req): Json<CreateStreamRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    check_auth(&state.config.server.api_key, &headers)?;

    // Rate limiting
    let rate_limit = state.config.limits.create_per_min;
    if rate_limit > 0 {
        let mut last = state.last_create.lock().unwrap();
        let min_interval = std::time::Duration::from_secs(60) / rate_limit;
        let elapsed = last.elapsed();
        if elapsed < min_interval {
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                format!("rate limit: {rate_limit}/min, retry in {}s", (min_interval - elapsed).as_secs() + 1),
            ));
        }
        *last = Instant::now();
    }

    if req.url.is_empty() || !req.url.starts_with("rtsp://") {
        return Err((StatusCode::BAD_REQUEST, "invalid RTSP URL".into()));
    }

    match state.stream_manager.create_dynamic(&req.url).await {
        Ok(stream_id) => Ok((
            StatusCode::CREATED,
            Json(CreateStreamResponse { stream_id }),
        )),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("{e}"))),
    }
}

/// DELETE /api/streams/:id — Stop a dynamic stream.
pub async fn delete_stream(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    check_auth(&state.config.server.api_key, &headers)?;

    if state.config.find_stream(&id).is_some() {
        return Err((
            StatusCode::FORBIDDEN,
            "cannot delete configured streams".into(),
        ));
    }

    match state.stream_manager.remove_stream(&id).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(e) => Err((StatusCode::NOT_FOUND, e)),
    }
}

/// If api_key is configured, validate the Authorization header.
fn check_auth(api_key: &str, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    if api_key.is_empty() {
        return Ok(());
    }
    let auth = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let expected = format!("Bearer {api_key}");
    if auth != expected {
        return Err((StatusCode::UNAUTHORIZED, "invalid API key".into()));
    }
    Ok(())
}

fn mask_url(url: &str) -> String {
    if let Ok(u) = url::Url::parse(url) {
        let mut masked = format!("{}://", u.scheme());
        if u.username().is_empty() {
            masked.push_str(u.host_str().unwrap_or("?"));
        } else {
            masked.push_str(&format!("***@{}", u.host_str().unwrap_or("?")));
        }
        masked.push_str(&format!(":{}", u.port().unwrap_or(554)));
        masked
    } else {
        url.to_string()
    }
}
