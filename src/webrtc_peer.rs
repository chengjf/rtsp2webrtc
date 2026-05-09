use crate::error::AppResult;
use crate::rtp_relay::RtpRelay;
use crate::rtsp::H264CodecInfo;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use rtp::packet::Packet;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_H264};
use webrtc_util::Unmarshal;
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_candidate::RTCIceCandidate;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::rtp_transceiver::RTCPFeedback;
use webrtc::rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType};
use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;
use webrtc::track::track_local::TrackLocal;

/// Messages flowing from the signaling channel into the WebRTC peer.
#[derive(Debug)]
pub enum PeerCommand {
    SetRemoteDescription(RTCSessionDescription),
    AddIceCandidate(String),
    Close,
}

/// Events emitted by the WebRTC peer back to signaling.
#[derive(Clone, Debug)]
pub enum PeerEvent {
    LocalDescription(RTCSessionDescription),
    IceCandidate(serde_json::Value),
    ConnectionEstablished,
    ConnectionFailed,
}

/// Wraps a single WebRTC peer connection for one browser client.
///
/// Creates a video track and feeds it from the shared RTP relay.
pub struct WebRtcPeer {
    cmd_tx: mpsc::UnboundedSender<PeerCommand>,
    task_handle: JoinHandle<()>,
}

impl WebRtcPeer {
    /// Create a new WebRTC peer and its video track.
    /// RTP packets from `relay` are written to the track.
    /// `codec_info` provides the H264 SPS/PPS for the SDP fmtp line.
    pub async fn new(
        relay: Arc<RtpRelay>,
        codec_info: &H264CodecInfo,
        event_tx: mpsc::UnboundedSender<PeerEvent>,
    ) -> AppResult<Self> {
        let fmtp_line = codec_info.fmtp_line();
        info!("H264 codec fmtp: {fmtp_line}");

        // Register the H264 codec in the media engine.
        // Include standard RTCP feedback entries so that Firefox and Chrome
        // both see the a=rtcp-fb lines they expect in the SDP offer.
        let rtcp_feedback = vec![
            RTCPFeedback { typ: "nack".to_string(),     parameter: "".to_string()   },
            RTCPFeedback { typ: "nack".to_string(),     parameter: "pli".to_string() },
            RTCPFeedback { typ: "ccm".to_string(),      parameter: "fir".to_string() },
            RTCPFeedback { typ: "goog-remb".to_string(), parameter: "".to_string()  },
        ];
        let mut media = MediaEngine::default();
        media.register_codec(
            RTCRtpCodecParameters {
                capability: RTCRtpCodecCapability {
                    mime_type: MIME_TYPE_H264.to_string(),
                    clock_rate: 90000,
                    channels: 0,
                    sdp_fmtp_line: fmtp_line.clone(),
                    rtcp_feedback: rtcp_feedback.clone(),
                },
                payload_type: codec_info.payload_type,
                ..Default::default()
            },
            RTPCodecType::Video,
        )?;

        let api = APIBuilder::new().with_media_engine(media).build();
        let pc = Arc::new(api.new_peer_connection(RTCConfiguration::default()).await?);

        // Create a static RTP track for H264 video
        let video_track = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: MIME_TYPE_H264.to_string(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: fmtp_line,
                rtcp_feedback: rtcp_feedback,
            },
            "video".to_string(),
            "camera".to_string(),
        ));

        // Add the track to the peer connection
        let rtp_sender = pc
            .add_track(Arc::clone(&video_track) as Arc<dyn TrackLocal + Send + Sync>)
            .await?;

        // Read RTCP packets (required by the webrtc-rs spec)
        tokio::spawn(async move {
            let mut rtcp_buf = vec![0u8; 1500];
            while let Ok((_, _)) = rtp_sender.read(&mut rtcp_buf).await {}
        });

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<PeerCommand>();

        // ── Register callbacks BEFORE set_local_description ──────────────────
        // ICE gathering is triggered by set_local_description; if callbacks are
        // registered after that call, early candidates are silently dropped.
        // This matters most on loopback (localhost) where Round-Trip Time ≈ 0 ms.

        // ICE candidate → forward to signaling
        let event_tx_ice = event_tx.clone();
        pc.on_ice_candidate(Box::new(
            move |candidate: Option<RTCIceCandidate>| {
                if let Some(c) = candidate {
                    if let Ok(init) = c.to_json() {
                        debug!("Local ICE candidate: {:?}", init.candidate);
                        if let Ok(json) = serde_json::to_value(&init) {
                            let _ = event_tx_ice.send(PeerEvent::IceCandidate(json));
                        }
                    }
                }
                Box::pin(async {})
            },
        ));

        // Peer connection state → forward to signaling
        let event_tx_conn = event_tx.clone();
        pc.on_peer_connection_state_change(Box::new(
            move |state: RTCPeerConnectionState| {
                info!("WebRTC state: {state}");
                match state {
                    RTCPeerConnectionState::Connected => {
                        info!("WebRTC peer connection established");
                        let _ = event_tx_conn.send(PeerEvent::ConnectionEstablished);
                    }
                    RTCPeerConnectionState::Failed => {
                        error!("WebRTC peer connection failed");
                        let _ = event_tx_conn.send(PeerEvent::ConnectionFailed);
                    }
                    _ => {}
                }
                Box::pin(async {})
            },
        ));

        // ICE connection state (diagnostic only)
        pc.on_ice_connection_state_change(Box::new(move |state| {
            info!("ICE state: {state}");
            Box::pin(async {})
        }));

        // ── Create offer AFTER callbacks are in place ─────────────────────────
        // Create SDP offer — the server has the media track and initiates
        let offer = pc.create_offer(None).await?;
        pc.set_local_description(offer.clone()).await?;
        info!("SDP offer created, sending to browser");
        let _ = event_tx.send(PeerEvent::LocalDescription(offer));


        // Clones for the spawned task
        let pc_task = Arc::clone(&pc);
        let track = Arc::clone(&video_track);
        let mut rtp_rx = relay.subscribe();
        let event_tx_task = event_tx.clone();

        let task_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // RTP packets from relay → write to WebRTC track
                    result = rtp_rx.recv() => {
                        match result {
                            Ok(packet) => {
                                let mut buf = packet.data.as_ref();
                                match Packet::unmarshal(&mut buf) {
                                    Ok(rtp) => {
                                        if let Err(e) = track.write_rtp_with_extensions(&rtp, &[]).await {
                                            error!("Failed to write RTP to track: {e}");
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to parse RTP packet: {e}");
                                    }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                warn!("RTP relay lagged by {n} packets");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                info!("RTP relay closed");
                                break;
                            }
                        }
                    }

                    // Commands from signaling
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(PeerCommand::SetRemoteDescription(answer)) => {
                                // Browser's answer to our offer
                                if let Err(e) = pc_task.set_remote_description(answer).await {
                                    error!("Failed to set remote description: {e}");
                                    let _ = event_tx_task.send(PeerEvent::ConnectionFailed);
                                    break;
                                }
                                info!("Remote description set — WebRTC connection proceeding");
                            }
                            Some(PeerCommand::AddIceCandidate(candidate_json)) => {
                                if let Ok(init) = serde_json::from_str(&candidate_json) {
                                    if let Err(e) = pc_task.add_ice_candidate(init).await {
                                        error!("Failed to add ICE candidate: {e}");
                                    }
                                }
                            }
                            Some(PeerCommand::Close) | None => {
                                break;
                            }
                        }
                    }
                }
            }

            // Cleanup
            if let Err(e) = pc_task.close().await {
                debug!("Error closing peer connection: {e}");
            }
        });


        Ok(Self {
            cmd_tx,
            task_handle,
        })
    }

    /// Send a command to this peer.
    pub fn send_command(&self, cmd: PeerCommand) {
        let _ = self.cmd_tx.send(cmd);
    }
}

impl Drop for WebRtcPeer {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(PeerCommand::Close);
        self.task_handle.abort();
    }
}
