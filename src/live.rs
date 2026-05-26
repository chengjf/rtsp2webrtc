use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use bytes::Bytes;
use serde::Deserialize;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

const BROADCAST_CAP: usize = 128;
const IDLE_SECS: u64 = 30;

// ── LiveManager ───────────────────────────────────────────────────────────────

struct LiveEntry {
    tx: broadcast::Sender<Bytes>,
    /// FLV init data: file header + onMetaData + AVC sequence header.
    /// Replayed to late subscribers so flv.js can initialise its decoder.
    /// Contains NO video frames — no timestamp discontinuity for new subscribers.
    init_cache: Arc<Mutex<Vec<u8>>>,
    subscriber_count: usize,
    _child: tokio::process::Child,
    _relay: JoinHandle<()>,
    idle_timer: Option<JoinHandle<()>>,
}

pub struct LiveManager {
    streams: RwLock<HashMap<String, LiveEntry>>,
}

impl LiveManager {
    pub fn new() -> Self {
        Self { streams: RwLock::new(HashMap::new()) }
    }

    pub async fn subscribe(
        self: &Arc<Self>,
        rtsp_url: &str,
        mpegts: bool,
    ) -> Result<(Vec<u8>, broadcast::Receiver<Bytes>), String> {
        let key = stream_key(rtsp_url, mpegts);

        // Fast path: reuse existing FFmpeg
        {
            let mut streams = self.streams.write().await;
            if let Some(e) = streams.get_mut(&key) {
                if let Some(t) = e.idle_timer.take() {
                    t.abort();
                }
                e.subscriber_count += 1;
                let cache = e.init_cache.lock().unwrap().clone();
                let rx = e.tx.subscribe();
                info!("Live {}: reusing FFmpeg ({} subscribers)", mask_url(rtsp_url), e.subscriber_count);
                return Ok((cache, rx));
            }
        }

        // Slow path: start a new FFmpeg (outside the lock)
        let (tx, init_cache, child, relay) = spawn_ffmpeg(rtsp_url, mpegts).await?;

        // Re-check under write lock — concurrent first-subscriber race
        let mut streams = self.streams.write().await;
        if let Some(e) = streams.get_mut(&key) {
            if let Some(t) = e.idle_timer.take() {
                t.abort();
            }
            e.subscriber_count += 1;
            let cache = e.init_cache.lock().unwrap().clone();
            let rx = e.tx.subscribe();
            info!("Live {}: lost creation race, reusing existing FFmpeg", mask_url(rtsp_url));
            return Ok((cache, rx));
        }

        let cache = init_cache.lock().unwrap().clone();
        let rx = tx.subscribe();
        streams.insert(key, LiveEntry {
            tx,
            init_cache,
            subscriber_count: 1,
            _child: child,
            _relay: relay,
            idle_timer: None,
        });
        info!("Live {}: FFmpeg started", mask_url(rtsp_url));
        Ok((cache, rx))
    }

    pub async fn unsubscribe(self: &Arc<Self>, rtsp_url: &str, mpegts: bool) {
        let key = stream_key(rtsp_url, mpegts);

        let count = {
            let mut streams = self.streams.write().await;
            let Some(e) = streams.get_mut(&key) else { return };
            e.subscriber_count = e.subscriber_count.saturating_sub(1);
            e.subscriber_count
        };
        info!("Live {}: {} subscriber(s) remaining", mask_url(rtsp_url), count);

        if count == 0 {
            let this = Arc::clone(self);
            let log_url = rtsp_url.to_string();
            let key_timer = key.clone();
            let timer = tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(IDLE_SECS)).await;
                let mut streams = this.streams.write().await;
                if streams.get(&key_timer).map(|e| e.subscriber_count == 0).unwrap_or(false) {
                    info!("Live {}: idle timeout, stopping FFmpeg", mask_url(&log_url));
                    streams.remove(&key_timer);
                }
            });
            let mut streams = self.streams.write().await;
            if let Some(e) = streams.get_mut(&key) {
                e.idle_timer = Some(timer);
            }
        }
    }
}

// ── HTTP handler ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct LiveParams {
    pub url: String,
    /// `&mpegts=true` — H.265 MPEG-TS pass-through for mpegts.js + WebCodecs.
    /// Default (absent): H.264 FLV (transcode if needed) for flv.js / Jessibuca.
    #[serde(default)]
    pub mpegts: bool,
}

pub async fn handler(
    ws: WebSocketUpgrade,
    State(live_manager): State<Arc<LiveManager>>,
    Query(params): Query<LiveParams>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| run(socket, live_manager, params.url, params.mpegts))
}

async fn run(mut socket: WebSocket, live_manager: Arc<LiveManager>, rtsp_url: String, mpegts: bool) {
    let log_url = mask_url(&rtsp_url);
    info!("Live {log_url}: client connected");

    let (init_cache, mut rx) = match live_manager.subscribe(&rtsp_url, mpegts).await {
        Ok(pair) => pair,
        Err(e) => {
            error!("Live {log_url}: {e}");
            return;
        }
    };

    // Send FLV init data (file header + onMetaData + AVC sequence header).
    // Contains NO video frames — no timestamp discontinuity for new subscribers.
    // Empty for MPEG-TS mode (PAT/PMT repeat in-stream, no separate init needed).
    if !init_cache.is_empty() {
        if socket.send(Message::Binary(Bytes::from(init_cache))).await.is_err() {
            live_manager.unsubscribe(&rtsp_url, mpegts).await;
            return;
        }
    }

    // Forward live chunks
    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(data) => {
                        if socket.send(Message::Binary(data)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("Live {log_url}: lagged by {n} chunks");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("Live {log_url}: FFmpeg stream ended");
                        break;
                    }
                }
            }
            msg = socket.recv() => {
                match msg {
                    None | Some(Ok(Message::Close(_))) => break,
                    _ => {}
                }
            }
        }
    }

    info!("Live {log_url}: client disconnected");
    live_manager.unsubscribe(&rtsp_url, mpegts).await;
}

// ── FFmpeg spawn ──────────────────────────────────────────────────────────────

type SpawnResult = Result<
    (broadcast::Sender<Bytes>, Arc<Mutex<Vec<u8>>>, tokio::process::Child, JoinHandle<()>),
    String,
>;

async fn spawn_ffmpeg(rtsp_url: &str, mpegts: bool) -> SpawnResult {
    let mut cmd = tokio::process::Command::new("ffmpeg");
    cmd.args(["-loglevel", "error", "-rtsp_transport", "tcp", "-i", rtsp_url, "-an"]);

    if mpegts {
        // MPEG-TS mode: H.265 pass-through for mpegts.js + WebCodecs hardware decode.
        // MPEG-TS natively supports HEVC; no transcoding needed.
        info!("Live {}: H.265 pass-through → MPEG-TS (mpegts.js)", mask_url(rtsp_url));
        cmd.args(["-c:v", "copy", "-f", "mpegts", "pipe:1"]);
    } else {
        // FLV mode: transcode H.265 → H.264 if needed (flv.js / Jessibuca).
        let need_transcode = crate::rtsp::probe_is_h265(rtsp_url).await;
        info!(
            "Live {}: {}",
            mask_url(rtsp_url),
            if need_transcode { "H.265 → transcode to H.264 → FLV" } else { "H.264 → FLV copy" }
        );
        if need_transcode {
            cmd.args(["-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency"]);
        } else {
            cmd.args(["-c:v", "copy"]);
        }
        cmd.args(["-f", "flv", "pipe:1"]);
    }

    cmd.stdout(Stdio::piped()).stderr(Stdio::null()).kill_on_drop(true);

    let mut child = cmd.spawn()
        .map_err(|e| format!("failed to start FFmpeg: {e} (is ffmpeg installed?)"))?;

    let mut stdout = child.stdout.take().expect("stdout is piped");
    let (tx, _drain) = broadcast::channel(BROADCAST_CAP);
    let init_cache: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));

    let tx_relay = tx.clone();
    let cache_relay = Arc::clone(&init_cache);

    let relay = tokio::spawn(async move {
        let mut buf = vec![0u8; 8192];
        // MPEG-TS: PAT/PMT repeat in-band every ~1s, no separate init cache needed.
        // FLV: accumulate until AVC/HEVC sequence header found, then freeze cache.
        let mut init_complete = mpegts;

        loop {
            match stdout.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let data = Bytes::copy_from_slice(&buf[..n]);

                    if !init_complete {
                        let mut c = cache_relay.lock().unwrap();
                        c.extend_from_slice(&data);
                        if let Some(end) = flv_seq_hdr_end(&c) {
                            c.truncate(end);
                            init_complete = true;
                        }
                    }

                    let _ = tx_relay.send(data);
                }
            }
        }
    });

    Ok((tx, init_cache, child, relay))
}

// ── FLV parser ────────────────────────────────────────────────────────────────

/// Returns the byte offset just past the video sequence header tag (including
/// its trailing PreviousTagSize field), or None if the tag has not arrived yet.
///
/// FLV structure:
///   [9 bytes file header][4 bytes PreviousTagSize0]
///   [tag]* where each tag = [11 bytes tag header][N bytes payload][4 bytes PreviousTagSize]
///
/// Handles two video tag formats:
///
/// Standard FLV — H.264 AVC sequence header:
///   tag_type      = 0x09 (video)
///   payload[0]    = 0x17  (frame_type=1 keyframe, codec_id=7 AVC)
///   payload[1]    = 0x00  (AVCPacketType = sequence header)
///
/// Enhanced FLV — H.265/HEVC (and AV1/VP9) sequence header (FFmpeg 5.0+):
///   tag_type      = 0x09 (video)
///   payload[0]    bit 7 = 1 (IsExVideoHeader)
///                 bits 6-4 = FrameType (1 = keyframe)
///                 bits 3-0 = PacketType (0 = SequenceStart)
///   payload[1..5] = FourCC (e.g. b"hvc1" for HEVC)
fn flv_seq_hdr_end(data: &[u8]) -> Option<usize> {
    if data.len() < 13 {
        return None; // FLV file header not complete yet
    }
    let mut pos = 13; // skip 9-byte file header + 4-byte PreviousTagSize0
    loop {
        if pos + 11 > data.len() {
            return None; // tag header not complete yet
        }
        let tag_type = data[pos];
        let payload_size = ((data[pos + 1] as usize) << 16)
            | ((data[pos + 2] as usize) << 8)
            | (data[pos + 3] as usize);
        let tag_end = pos + 11 + payload_size + 4;

        if tag_end > data.len() {
            return None; // payload or trailing PreviousTagSize not complete yet
        }

        if tag_type == 0x09 && payload_size >= 2 {
            let p = pos + 11;
            let first = data[p];
            if first & 0x80 == 0 {
                // Standard FLV: H.264 AVC sequence header
                // 0x17 = keyframe (0x1_) + AVC codec (_7), 0x00 = SequenceHeader
                if first == 0x17 && data[p + 1] == 0x00 {
                    return Some(tag_end);
                }
            } else {
                // Enhanced FLV: IsExVideoHeader bit set
                // bits 3-0 = PacketType; 0x00 = SequenceStart
                if first & 0x0F == 0x00 {
                    return Some(tag_end);
                }
            }
        }

        pos = tag_end;
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Separate HashMap keys for FLV and MPEG-TS modes of the same RTSP URL,
/// so both can run concurrently with independent FFmpeg processes.
fn stream_key(rtsp_url: &str, mpegts: bool) -> String {
    if mpegts { format!("{rtsp_url}\x00mpegts") } else { rtsp_url.to_string() }
}

fn mask_url(url: &str) -> String {
    if let Ok(u) = url::Url::parse(url) {
        let mut s = format!("{}://", u.scheme());
        if !u.username().is_empty() {
            s.push_str(&format!("***@{}", u.host_str().unwrap_or("?")));
        } else {
            s.push_str(u.host_str().unwrap_or("?"));
        }
        if let Some(port) = u.port() {
            s.push_str(&format!(":{port}"));
        }
        s.push_str(u.path());
        s
    } else {
        "invalid-url".to_string()
    }
}
