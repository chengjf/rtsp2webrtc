use crate::error::{AppError, AppResult};
use crate::rtp_relay::{RtpPacket, RtpRelay};
use crate::rtsp::H264CodecInfo;
use bytes::Bytes;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use tracing::{error, info};

/// Pulls an H.265 RTSP stream, transcodes it to H.264 via FFmpeg,
/// and relays the resulting H.264 RTP packets to WebRTC consumers.
pub struct FfmpegPuller {
    /// Keep child alive; `kill_on_drop(true)` ensures FFmpeg exits when dropped.
    _child: tokio::process::Child,
    handle: JoinHandle<()>,
    pub codec_info: H264CodecInfo,
}

impl FfmpegPuller {
    pub async fn start(rtsp_url: &str, relay: Arc<RtpRelay>) -> AppResult<Self> {
        // Bind the UDP socket first so the port is ready before FFmpeg starts sending.
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .map_err(|e| AppError::Other(format!("UDP bind failed: {e}")))?;
        let port = socket
            .local_addr()
            .map_err(|e| AppError::Other(format!("UDP local_addr: {e}")))?
            .port();

        info!("Transcoding H.265 → H.264 via FFmpeg (RTP on port {port})");

        let mut child = tokio::process::Command::new("ffmpeg")
            .args([
                "-loglevel",       "error",
                "-rtsp_transport", "tcp",
                "-i",              rtsp_url,
                "-an",
                "-vcodec",         "libx264",
                "-profile:v",      "baseline",
                "-level",          "3.1",
                "-preset",         "ultrafast",
                "-tune",           "zerolatency",
                "-g",              "60",
                "-f",              "rtp",
                &format!("rtp://127.0.0.1:{port}"),
            ])
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| AppError::Other(
                format!("failed to start ffmpeg: {e} (is ffmpeg installed?)")
            ))?;

        // Give FFmpeg time to connect to RTSP and begin transcoding.
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

        // If FFmpeg already exited, the RTSP URL or credentials are likely bad.
        if let Ok(Some(status)) = child.try_wait() {
            return Err(AppError::Other(format!(
                "FFmpeg exited early (status {status}) — check RTSP URL and credentials"
            )));
        }

        // Relay H.264 RTP packets from FFmpeg → broadcast channel
        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                match socket.recv(&mut buf).await {
                    Ok(n) if n > 0 => {
                        relay.relay(RtpPacket {
                            data: Bytes::copy_from_slice(&buf[..n]),
                        });
                    }
                    Ok(_) => {}
                    Err(e) => {
                        error!("FFmpeg RTP relay: recv error: {e}");
                        break;
                    }
                }
            }
        });

        // FFmpeg sends SPS/PPS in-band with every keyframe (default behaviour),
        // so sprop-parameter-sets can be left empty in the SDP offer.
        let codec_info = H264CodecInfo {
            payload_type: 96,
            sps: vec![],
            pps: vec![],
        };

        Ok(Self {
            _child: child,
            handle,
            codec_info,
        })
    }
}

impl Drop for FfmpegPuller {
    fn drop(&mut self) {
        self.handle.abort();
        // _child is killed automatically via kill_on_drop(true)
    }
}
