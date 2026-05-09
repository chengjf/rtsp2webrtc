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

fn normalize_remote_answer_sdp(sdp: &str) -> String {
    let lines: Vec<String> = sdp
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .split('\n')
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect();

    let first_media_index = lines
        .iter()
        .position(|line| line.starts_with("m="))
        .unwrap_or(lines.len());

    let session_lines = &lines[..first_media_index];
    let has_session_ufrag = session_lines
        .iter()
        .any(|line| line.starts_with("a=ice-ufrag:"));
    let has_session_pwd = session_lines
        .iter()
        .any(|line| line.starts_with("a=ice-pwd:"));

    if has_session_ufrag && has_session_pwd {
        return lines.join("\r\n") + "\r\n";
    }

    let media_ufrag = (!has_session_ufrag)
        .then(|| {
            lines.iter()
                .skip(first_media_index)
                .find(|line| line.starts_with("a=ice-ufrag:"))
                .cloned()
        })
        .flatten();
    let media_pwd = (!has_session_pwd)
        .then(|| {
            lines.iter()
                .skip(first_media_index)
                .find(|line| line.starts_with("a=ice-pwd:"))
                .cloned()
        })
        .flatten();

    if media_ufrag.is_none() && media_pwd.is_none() {
        return lines.join("\r\n") + "\r\n";
    }

    let mut normalized = Vec::with_capacity(lines.len() + 2);
    normalized.extend(lines[..first_media_index].iter().cloned());
    if let Some(ufrag) = media_ufrag {
        normalized.push(ufrag);
    }
    if let Some(pwd) = media_pwd {
        normalized.push(pwd);
    }
    normalized.extend(lines[first_media_index..].iter().cloned());

    normalized.join("\r\n") + "\r\n"
}

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
                            let raw_has_ufrag = sdp.contains("a=ice-ufrag:");
                            let raw_has_pwd = sdp.contains("a=ice-pwd:");
                            let sdp = normalize_remote_answer_sdp(&sdp);
                            let normalized_has_ufrag = sdp.contains("a=ice-ufrag:");
                            let normalized_has_pwd = sdp.contains("a=ice-pwd:");
                            info!(
                                "Received SDP answer (raw ice-ufrag={raw_has_ufrag}, raw ice-pwd={raw_has_pwd}, normalized ice-ufrag={normalized_has_ufrag}, normalized ice-pwd={normalized_has_pwd})"
                            );
                            if !normalized_has_ufrag || !normalized_has_pwd {
                                warn!("Browser SDP answer is missing ICE credentials after normalization");
                            }
                            match RTCSessionDescription::answer(sdp) {
                                Ok(desc) => peer.send_command(PeerCommand::SetRemoteDescription(desc)),
                                Err(e) => {
                                    error!("Invalid SDP answer after normalization: {e}");
                                    break;
                                }
                            }
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

#[cfg(test)]
mod tests {
    use super::normalize_remote_answer_sdp;

    #[test]
    fn promotes_media_level_ice_credentials_to_session_level() {
        let sdp = concat!(
            "v=0\r\n",
            "o=mozilla...THIS_IS_SDPARTA-99.0 0 0 IN IP4 0.0.0.0\r\n",
            "s=-\r\n",
            "t=0 0\r\n",
            "a=group:BUNDLE 0\r\n",
            "a=msid-semantic: WMS *\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 126\r\n",
            "c=IN IP4 0.0.0.0\r\n",
            "a=mid:0\r\n",
            "a=recvonly\r\n",
            "a=rtcp-mux\r\n",
            "a=ice-ufrag:firefoxUfrag\r\n",
            "a=ice-pwd:firefoxPwd\r\n",
            "a=fingerprint:sha-256 11:22:33\r\n",
            "a=setup:active\r\n",
        );

        let normalized = normalize_remote_answer_sdp(sdp);
        let first_media = normalized.find("m=video").unwrap();
        let session_part = &normalized[..first_media];

        assert!(session_part.contains("a=ice-ufrag:firefoxUfrag\r\n"));
        assert!(session_part.contains("a=ice-pwd:firefoxPwd\r\n"));
        assert_eq!(normalized.matches("a=ice-ufrag:firefoxUfrag").count(), 2);
        assert_eq!(normalized.matches("a=ice-pwd:firefoxPwd").count(), 2);
    }

    #[test]
    fn keeps_existing_session_level_ice_credentials() {
        let sdp = concat!(
            "v=0\n",
            "o=- 1 1 IN IP4 127.0.0.1\n",
            "s=-\n",
            "t=0 0\n",
            "a=ice-ufrag:sessionUfrag\n",
            "a=ice-pwd:sessionPwd\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\n",
            "a=mid:0\n",
        );

        let normalized = normalize_remote_answer_sdp(sdp);

        assert_eq!(normalized.matches("a=ice-ufrag:sessionUfrag").count(), 1);
        assert_eq!(normalized.matches("a=ice-pwd:sessionPwd").count(), 1);
        assert!(normalized.contains("\r\n"));
    }

    #[test]
    fn only_backfills_missing_session_level_credential() {
        let sdp = concat!(
            "v=0\n",
            "o=- 1 1 IN IP4 127.0.0.1\n",
            "s=-\n",
            "t=0 0\n",
            "a=ice-ufrag:sessionUfrag\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\n",
            "a=mid:0\n",
            "a=ice-ufrag:mediaUfrag\n",
            "a=ice-pwd:mediaPwd\n",
        );

        let normalized = normalize_remote_answer_sdp(sdp);
        let first_media = normalized.find("m=video").unwrap();
        let session_part = &normalized[..first_media];

        assert_eq!(normalized.matches("a=ice-ufrag:sessionUfrag").count(), 1);
        assert!(session_part.contains("a=ice-pwd:mediaPwd\r\n"));
        assert_eq!(normalized.matches("a=ice-pwd:mediaPwd").count(), 2);
    }
}

