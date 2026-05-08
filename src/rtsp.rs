use crate::error::{AppError, AppResult};
use crate::rtp_relay::{RtpPacket, RtpRelay};
use bytes::Bytes;
use retina::client::{PlayOptions, Playing, Session, SessionOptions, SetupOptions};
use retina::codec::{ParametersRef, VideoParametersCodec};
use retina::client::PacketItem;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use url::Url;

/// Extracted H264 codec info needed for WebRTC SDP negotiation.
#[derive(Clone, Debug)]
pub struct H264CodecInfo {
    pub payload_type: u8,
    /// Raw SPS NAL unit (including NAL header).
    pub sps: Vec<u8>,
    /// Raw PPS NAL unit (including NAL header).
    pub pps: Vec<u8>,
}

impl H264CodecInfo {
    /// Build the `a=fmtp:<pt>` line value for an H264 WebRTC SDP.
    ///
    /// Format: `profile-level-id=XXXXXX;packetization-mode=1;sprop-parameter-sets=...,...`
    pub fn fmtp_line(&self) -> String {
        let profile_level_id = if self.sps.len() >= 4 {
            // SPS[0] is the NAL header (0x67); profile-level-id is in bytes 1..4
            format!(
                "{:02x}{:02x}{:02x}",
                self.sps[1], self.sps[2], self.sps[3]
            )
        } else {
            // Fallback: baseline profile level 3.1
            "42e01f".to_string()
        };

        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let sprop = format!(
            "{},{}",
            b64.encode(&self.sps),
            b64.encode(&self.pps)
        );

        format!(
            "profile-level-id={profile_level_id};packetization-mode=1;sprop-parameter-sets={sprop}"
        )
    }
}

/// Manages the RTSP connection to a camera and feeds RTP packets
/// into the relay for distribution to WebRTC peers.
pub struct RtspPuller {
    handle: JoinHandle<()>,
    pub codec_info: H264CodecInfo,
}

impl RtspPuller {
    /// Start pulling from an RTSP URL and relay RTP packets.
    ///
    /// Returns the puller handle and the parsed H264 codec information
    /// needed for constructing WebRTC SDP offers.
    pub async fn start(
        rtsp_url: &str,
        relay: Arc<RtpRelay>,
    ) -> AppResult<Self> {
        let mut url = Url::parse(rtsp_url)
            .map_err(|e| AppError::Other(format!("invalid RTSP URL: {e}")))?;

        // Extract credentials from URL (retina requires them in SessionOptions, not URL)
        let mut options = SessionOptions::default();
        let user = url.username().to_string();
        let pass = url.password().unwrap_or("").to_string();
        let has_creds = !user.is_empty();

        // Strip credentials from URL — retina rejects URLs with embedded creds
        if has_creds {
            url.set_username("").ok();
            url.set_password(None).ok();
        }

        if has_creds {
            use retina::client::Credentials;
            options = options.creds(Some(Credentials {
                username: user,
                password: pass,
            }));
        }

        // Log without credentials
        let mut log_url = url.clone();
        log_url.set_username("admin").ok();
        log_url.set_password(Some("***")).ok();
        info!("Connecting to RTSP source: {log_url}");

        // Step 1: DESCRIBE — get stream info and codec parameters
        let mut session = Session::describe(url, options)
            .await
            .map_err(|e| AppError::Rtsp(e.to_string()))?;

        // Extract video track metadata while we have an immutable borrow.
        // Must do this before setup() which requires &mut self.
        let video_idx = session
            .streams()
            .iter()
            .position(|s| s.media() == "video")
            .ok_or_else(|| AppError::Rtsp("no video track in RTSP stream".into()))?;

        let video_stream = &session.streams()[video_idx];
        let payload_type = video_stream.rtp_payload_type();
        let encoding = video_stream.encoding_name().to_string();

        // Extract SPS/PPS (cloned into owned Vec<u8> so we drop the borrow)
        let (sps, pps) = match video_stream.parameters() {
            Some(ParametersRef::Video(vp)) => match vp.codec_params() {
                VideoParametersCodec::H264 { sps, pps } => {
                    info!(
                        "Extracted H264 parameters: SPS={} bytes, PPS={} bytes",
                        sps.len(),
                        pps.len()
                    );
                    (sps.to_vec(), pps.to_vec())
                }
                _ => {
                    warn!("Video track is not H264: {encoding}");
                    (vec![], vec![])
                }
            },
            _ => {
                warn!("No SPS/PPS in SDP for {encoding} — will need to extract from stream");
                (vec![], vec![])
            }
        };

        info!(
            "Video track found: {encoding}, payload_type={payload_type}, \
             clock_rate={}, stream_index={video_idx}",
            video_stream.clock_rate_hz(),
        );
        // Immutable borrow ends here — video_stream and parameters data dropped

        // Step 2: SETUP — configure transport for the video track
        session
            .setup(video_idx, SetupOptions::default())
            .await
            .map_err(|e| AppError::Rtsp(e.to_string()))?;

        info!("RTSP SETUP complete for track {video_idx}");

        // Step 3: PLAY — start media delivery
        let playing = session
            .play(PlayOptions::default())
            .await
            .map_err(|e| AppError::Rtsp(e.to_string()))?;

        info!("RTSP PLAY started, receiving RTP packets");

        let codec_info = H264CodecInfo {
            payload_type,
            sps,
            pps,
        };

        // Step 4: Spawn RTP relay task with reconnect logic + keepalive
        let reconnect_url = rtsp_url.to_string();
        let handle = tokio::spawn(async move {
            use futures_util::StreamExt;

            let mut backoff_ms = 1000;
            const MAX_BACKOFF_MS: u64 = 30_000;
            const KEEPALIVE_SECS: u64 = 55; // camera timeout is 60s

            let mut current = playing;

            loop {
                loop {
                    // Wrap each recv() with a keepalive timeout.
                    // If no RTP packet arrives within KEEPALIVE_SECS,
                    // the RTSP session may have timed out → reconnect.
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(KEEPALIVE_SECS),
                        current.next(),
                    )
                    .await
                    {
                        Ok(Some(Ok(PacketItem::Rtp(packet)))) => {
                            relay.relay(RtpPacket {
                                data: Bytes::copy_from_slice(packet.raw()),
                            });
                        }
                        Ok(Some(Err(e))) => {
                            error!("RTSP stream error: {e}");
                            break;
                        }
                        Ok(Some(_)) => {} // Rtcp or unknown
                        Ok(None) => {
                            info!("RTSP stream ended normally");
                            break;
                        }
                        Err(_elapsed) => {
                            warn!("RTSP keepalive timeout ({KEEPALIVE_SECS}s), reconnecting");
                            break;
                        }
                    }
                }

                // Reconnect with exponential backoff
                warn!(
                    "RTSP disconnected, reconnecting in {}ms...",
                    backoff_ms
                );
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);

                match reconnect(&reconnect_url, video_idx).await {
                    Ok(new_playing) => {
                        info!("RTSP reconnected successfully");
                        current = new_playing;
                        backoff_ms = 1000; // reset backoff
                    }
                    Err(e) => {
                        error!("RTSP reconnect failed: {e}");
                        continue; // try again with increased backoff
                    }
                }
            }
        });

        info!("RTSP puller active");

        Ok(Self {
            handle,
            codec_info,
        })
    }

    /// Abort the RTSP pull task.
    pub fn stop(&mut self) {
        self.handle.abort();
    }
}

impl Drop for RtspPuller {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Attempt to reconnect to an RTSP source.
/// Performs DESCRIBE → SETUP → PLAY and returns the playing session.
async fn reconnect(
    rtsp_url: &str,
    video_idx: usize,
) -> Result<Session<Playing>, AppError> {
    let url = Url::parse(rtsp_url)
        .map_err(|e| AppError::Other(format!("invalid RTSP URL: {e}")))?;

    // Extract credentials
    let user = url.username().to_string();
    let pass = url.password().unwrap_or("").to_string();
    let has_creds = !user.is_empty();

    let mut clean_url = url.clone();
    let mut options = SessionOptions::default();
    if has_creds {
        clean_url.set_username("").ok();
        clean_url.set_password(None).ok();
        options = options.creds(Some(retina::client::Credentials {
            username: user,
            password: pass,
        }));
    }

    let mut session = Session::describe(clean_url, options)
        .await
        .map_err(|e| AppError::Rtsp(e.to_string()))?;

    session
        .setup(video_idx, SetupOptions::default())
        .await
        .map_err(|e| AppError::Rtsp(e.to_string()))?;

    session
        .play(PlayOptions::default())
        .await
        .map_err(|e| AppError::Rtsp(e.to_string()))
}

