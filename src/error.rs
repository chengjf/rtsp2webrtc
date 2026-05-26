use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("RTSP error: {0}")]
    Rtsp(String),

    #[error("WebRTC error: {0}")]
    WebRtc(String),

    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    #[error("Signaling error: {0}")]
    Signaling(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel closed")]
    ChannelClosed,

    #[error("{0}")]
    Other(String),

    /// Sentinel: RTSP stream is H.265 — caller should switch to FFmpeg transcoding.
    #[error("H.265 stream detected — FFmpeg transcoding required")]
    H265Required,
}

impl From<webrtc::error::Error> for AppError {
    fn from(e: webrtc::error::Error) -> Self {
        AppError::WebRtc(e.to_string())
    }
}

pub type AppResult<T> = Result<T, AppError>;
