use crate::error::AppResult;
use crate::rtp_relay::RtpRelay;
use crate::rtsp::H264CodecInfo;
use crate::stream::{StreamId, StreamManager};
use crate::webrtc_peer::{PeerCommand, PeerEvent, WebRtcPeer};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

/// JSON messages exchanged over the signaling WebSocket.
#[derive(serde::Deserialize, Debug)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "request_stream")]
    RequestStream,
    #[serde(rename = "sdp_answer")]
    SdpAnswer { sdp: String },
    #[serde(rename = "ice_candidate")]
    IceCandidate {
        candidate: String,
        #[serde(rename = "sdpMid")]
        sdp_mid: Option<String>,
        /// Firefox requires sdpMLineIndex; Chrome/Edge send it too.
        #[serde(rename = "sdpMLineIndex")]
        sdp_mline_index: Option<u16>,
    },
}

#[derive(serde::Serialize, Debug)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "sdp_offer")]
    SdpOffer { sdp: String },
    #[serde(rename = "ice_candidate")]
    IceCandidate {
        candidate: serde_json::Value,
        #[serde(rename = "sdpMid")]
        sdp_mid: String,
    },
    #[serde(rename = "error")]
    Error { message: String },
    #[serde(rename = "connected")]
    Connected,
}

/// Handle a new WebSocket signaling connection.
///
/// Flow:
/// 1. Browser connects via WebSocket
/// 2. Browser sends `request_stream`
/// 3. Server creates a WebRTC peer + offer
/// 4. SDP/ICE exchange proceeds
/// 5. Media flows through SRTP
pub async fn handle_signaling(
    ws: WebSocket,
    relay: Arc<RtpRelay>,
    codec_info: H264CodecInfo,
    stream_id: StreamId,
    stream_manager: Arc<StreamManager>,
) -> AppResult<()> {
    let (mut ws_tx, mut ws_rx) = ws.split();

    // Channel for WebRTC peer events → WebSocket
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<PeerEvent>();

    let relay_clone = Arc::clone(&relay);

    // Wait for the first message (must be request_stream)
    let first_msg = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        ws_rx.next(),
    )
    .await;

    let first_msg = match first_msg {
        Ok(Some(Ok(msg))) => msg,
        Ok(Some(Err(e))) => {
            error!("WebSocket error: {e}");
            return Err(crate::error::AppError::Signaling("WebSocket error".into()));
        }
        Ok(None) => {
            warn!("WebSocket closed before request");
            return Ok(());
        }
        Err(_) => {
            warn!("Timeout waiting for stream request");
            return Err(crate::error::AppError::Signaling("timeout".into()));
        }
    };

    let text = match first_msg {
        Message::Text(t) => t,
        _ => {
            send_json(
                &mut ws_tx,
                &ServerMessage::Error {
                    message: "expected text message".into(),
                },
            )
            .await;
            return Ok(());
        }
    };

    let client_msg: ClientMessage = match serde_json::from_str(&text) {
        Ok(msg) => msg,
        Err(e) => {
            send_json(
                &mut ws_tx,
                &ServerMessage::Error {
                    message: format!("invalid JSON: {e}"),
                },
            )
            .await;
            return Ok(());
        }
    };

    if !matches!(client_msg, ClientMessage::RequestStream) {
        send_json(
            &mut ws_tx,
            &ServerMessage::Error {
                message: "expected request_stream".into(),
            },
        )
        .await;
        return Ok(());
    }

    info!("Client requested stream, creating WebRTC peer");

    // Create WebRTC peer (creates answer automatically)
    let peer = match WebRtcPeer::new(relay_clone, &codec_info, event_tx).await {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to create WebRTC peer: {e}");
            send_json(
                &mut ws_tx,
                &ServerMessage::Error {
                    message: format!("failed to create peer: {e}"),
                },
            )
            .await;
            return Err(e);
        }
    };

    // Send connected message
    send_json(&mut ws_tx, &ServerMessage::Connected).await;

    // Spawn WebSocket reader task
    let peer_handle = Arc::new(tokio::sync::Mutex::new(peer));
    let peer_reader = Arc::clone(&peer_handle);

    let ws_reader = tokio::spawn(async move {
        while let Some(msg) = ws_rx.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    let msg: ClientMessage = match serde_json::from_str(&text) {
                        Ok(m) => m,
                        Err(_) => continue,
                    };

                    let peer = peer_reader.lock().await;
                    match msg {
                        ClientMessage::SdpAnswer { sdp } => {
                            info!("Received SDP answer");
                            let desc = RTCSessionDescription::answer(sdp).unwrap_or_else(|e| {
                                error!("Invalid SDP answer: {e}");
                                RTCSessionDescription::answer("".to_string()).unwrap()
                            });
                            peer.send_command(PeerCommand::SetRemoteDescription(desc));
                        }
                        ClientMessage::IceCandidate {
                            candidate,
                            sdp_mid,
                            sdp_mline_index,
                        } => {
                            let candidate_json = serde_json::json!({
                                "candidate": candidate,
                                "sdpMid": sdp_mid.unwrap_or_default(),
                                "sdpMLineIndex": sdp_mline_index.unwrap_or(0),
                            });
                            peer.send_command(PeerCommand::AddIceCandidate(
                                candidate_json.to_string(),
                            ));
                        }
                        _ => {}
                    }
                }
                Ok(Message::Close(_)) => break,
                Err(e) => {
                    error!("WebSocket read error: {e}");
                    break;
                }
                _ => {}
            }
        }
    });

    // Forward WebRTC peer events → WebSocket (main loop)
    while let Some(event) = event_rx.recv().await {
        match event {
            PeerEvent::LocalDescription(desc) => {
                send_json(
                    &mut ws_tx,
                    &ServerMessage::SdpOffer { sdp: desc.sdp },
                )
                .await;
            }
            PeerEvent::IceCandidate(json) => {
                let sdp_mid = json["sdpMid"].as_str().unwrap_or("0").to_string();
                send_json(
                    &mut ws_tx,
                    &ServerMessage::IceCandidate {
                        candidate: json,
                        sdp_mid,
                    },
                )
                .await;
            }
            PeerEvent::ConnectionEstablished => {
                info!("WebRTC connection established, media flowing");
            }
            PeerEvent::ConnectionFailed => {
                send_json(
                    &mut ws_tx,
                    &ServerMessage::Error {
                        message: "connection failed".into(),
                    },
                )
                .await;
                break;
            }
        }
    }

    ws_reader.abort();
    stream_manager.unsubscribe(&stream_id).await;
    Ok(())
}

async fn send_json(
    tx: &mut futures_util::stream::SplitSink<
        axum::extract::ws::WebSocket,
        axum::extract::ws::Message,
    >,
    msg: &ServerMessage,
) {
    if let Ok(json) = serde_json::to_string(msg) {
        if let Err(e) = tx.send(Message::Text(json.into())).await {
            warn!("Failed to send WebSocket message: {e}");
        }
    }
}

