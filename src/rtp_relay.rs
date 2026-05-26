use bytes::Bytes;
use tokio::sync::broadcast;
use tracing::debug;

/// A raw RTP packet received from an RTSP source.
/// Stored as raw bytes to preserve all header fields (marker, CSRC, etc.).
#[derive(Clone, Debug)]
pub struct RtpPacket {
    pub data: Bytes,
}

/// Relays RTP packets from an RTSP source to multiple WebRTC consumers
/// via a broadcast channel. Each consumer receives a copy.
pub struct RtpRelay {
    tx: broadcast::Sender<RtpPacket>,
}

impl RtpRelay {
    pub fn new(capacity: usize) -> Self {
        let (tx, mut drain) = broadcast::channel(capacity);
        // Keep a drain receiver alive so send() never returns Closed.
        // Periodically drain to prevent the channel from filling up.
        tokio::spawn(async move {
            loop {
                match drain.recv().await {
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Self { tx }
    }

    /// Create a new subscriber that receives RTP packets.
    pub fn subscribe(&self) -> broadcast::Receiver<RtpPacket> {
        self.tx.subscribe()
    }

    /// Send an RTP packet to all subscribers.
    pub fn relay(&self, packet: RtpPacket) {
        if let Err(e) = self.tx.send(packet) {
            debug!("No active RTP subscribers: {e}");
        }
    }

}
