# RTSP → WebRTC Gateway

Low-latency RTSP-to-WebRTC streaming gateway. Relays IP camera RTSP streams to browsers via native WebRTC — no plugins, no transcoding.

> 中文版: [README.md](README.md)

---

## Key Features

- **Zero-transcode RTP relay** — No frame decoding, no re-encoding. Minimal CPU and latency.
- **Multi-client fanout** — One RTSP pull → N concurrent browser viewers
- **Lazy start + idle auto-stop** — Only pulls from camera when someone is watching; stops 30s after last viewer leaves
- **Dynamic streams** — Clients submit RTSP URLs via API; no server-side preconfiguration needed
- **Reconnect + keepalive** — Exponential backoff auto-reconnect; 55s keepalive timeout prevents camera session expiry
- **Auth & rate limiting** — Optional API key + configurable creation rate limits
- **Monitoring ready** — `/health`, `/metrics` (Prometheus), structured logging

---

## Build

### Linux

```bash
# Prerequisites: Rust + GCC
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
sudo apt install build-essential    # Debian/Ubuntu
# or: sudo dnf install gcc          # Fedora
# or: sudo pacman -S gcc            # Arch

# Build
git clone <repo-url> && cd rtsp2webRTC
cargo build --release
```

### Windows (MSVC)

```powershell
# Prerequisites: Rust + Visual Studio Build Tools
# 1. Install Rust: https://rustup.rs (choose msvc toolchain)
# 2. Install VS Build Tools: https://visualstudio.microsoft.com/downloads/
#    Select "Desktop development with C++" workload

# Build
git clone <repo-url> && cd rtsp2webRTC
cargo build --release
```

### Windows (GNU)

```bash
# Prerequisites: Rust (gnu toolchain) + MinGW-w64
rustup default stable-x86_64-pc-windows-gnu
# Install MinGW-w64 via Chocolatey:
choco install mingw -y
# Or download: https://www.mingw-w64.org

# Add MinGW to PATH, then build
export PATH="/c/ProgramData/mingw64/mingw64/bin:$PATH"
cargo build --release
```

> **Note:** The `ring` crate (used by webrtc-rs for DTLS) requires a C compiler. On Linux this is GCC; on Windows use either MSVC toolchain or GNU + MinGW-w64.

### Cross-compile (Linux → Windows)

```bash
rustup target add x86_64-pc-windows-gnu
sudo apt install mingw-w64
cargo build --release --target x86_64-pc-windows-gnu
```

---

## Quick Start

### 1. Configuration

Create `config.toml` (or override via `CONFIG_PATH` env var):

```toml
[server]
bind_addr = "0.0.0.0:3000"
# api_key = "changeme"

[limits]
max_peers = 50
max_per_stream = 20
create_per_min = 10

[logging]
format = "text"    # "text" or "json"

[cors]
allowed_origins = ["*"]
```

### 2. Start Server

```bash
cargo run --release
# Or with log level
RUST_LOG=info cargo run --release
```

### 3. Open Browser

- **Debug page**: Open `web/debugging/index.html` — full-featured (stream list, log panel), ideal for development & debugging
- **Demo page**: Open `web/demo/demo.html` — minimal example showing the simplest integration

Verify the gateway URL is `http://localhost:3000`, enter an RTSP URL, and click "Play".

---

## Usage

### Via Demo Page

Open `web/demo/demo.html`, enter an RTSP URL, and click Play.

### Via API

Client submits an RTSP URL; the server spins up the relay on demand:

```bash
curl -X POST http://localhost:3000/api/streams \
  -H "Content-Type: application/json" \
  -d '{"url":"rtsp://admin:pass@192.168.1.100:554/stream"}'
```

Returns `{"stream_id":"uuid"}`. Then connect via WebSocket:

```
ws://localhost:3000/ws?stream=uuid
```

---

## REST API

All responses are `application/json`.

### Health Check

```http
GET /health
```

```json
{
  "status": "ok",
  "uptime_secs": 3600,
  "configured_streams": 0,
  "active_streams": 1,
  "total_peers": 3
}
```

### List Streams

```http
GET /api/streams
```

```json
{
  "streams": [
    {
      "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "name": "Dynamic (a1b2c3d4)",
      "url": "rtsp://***@192.168.1.100:554",
      "dynamic": true,
      "subscribers": 2,
      "connected": true,
      "codec": "h264",
      "payload_type": 96
    }
  ]
}
```

### Create Dynamic Stream

```http
POST /api/streams
Authorization: Bearer changeme    # if api_key is configured
Content-Type: application/json

{ "url": "rtsp://admin:pass@192.168.1.100:554/stream" }
```

```json
{ "stream_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890" }
```

### Stream Detail

```http
GET /api/streams/a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

### Delete Dynamic Stream

```http
DELETE /api/streams/a1b2c3d4-e5f6-7890-abcd-ef1234567890
Authorization: Bearer changeme
```

→ `204 No Content`

### Prometheus Metrics

```http
GET /metrics
```

---

## WebSocket Signaling Protocol

### Client → Server

```json
{ "type": "request_stream" }
{ "type": "sdp_answer", "sdp": "v=0\r\n..." }
{ "type": "ice_candidate", "candidate": "...", "sdpMid": "0" }
```

### Server → Client

```json
{ "type": "connected" }
{ "type": "sdp_offer", "sdp": "v=0\r\n..." }
{ "type": "ice_candidate", "candidate": {...}, "sdpMid": "0" }
{ "type": "error", "message": "..." }
```

### Connection Parameters

| Parameter | Required | Description |
|-----------|:--------:|-------------|
| `stream` | Yes | Stream ID (UUID, returned by POST /api/streams) |
| `key` | No | API key (required if `api_key` is configured on the server). |

Example: `ws://localhost:3000/ws?stream=a1b2c3d4-e5f6-7890-abcd-ef1234567890&key=changeme`

---

## Architecture

```
RTSP Camera ──→ RtspPuller ──→ RtpRelay (broadcast) ──┬──→ WebRTC Peer 1 ──→ Browser 1
                                           ├──→ WebRTC Peer 2 ──→ Browser 2
                                           └──→ WebRTC Peer N ──→ Browser N
```

- **RtspPuller** — retina crate connects to camera, receives raw RTP packets
- **RtpRelay** — tokio broadcast channel: one RTP stream → N WebRTC tracks
- **WebRtcPeer** — webrtc-rs manages PeerConnection, creates H264 video track
- **StreamManager** — stream registry, lifecycle, subscriber counting, connection limits
- **Signaling** — WebSocket SDP/ICE exchange

---

## Configuration Reference

| Field | Type | Default | Description |
|------|------|------|------|
| `server.bind_addr` | string | `0.0.0.0:3000` | HTTP/WS listen address |
| `server.api_key` | string | (empty) | API auth key; disabled if empty |
| `limits.max_peers` | int | 50 | Global max WebRTC connections |
| `limits.max_per_stream` | int | 20 | Max viewers per stream |
| `limits.create_per_min` | int | 0 | Max dynamic stream creates/min, 0=unlimited |
| `cors.allowed_origins` | []string | [] | `["*"]` for all, empty=same-origin |
| `logging.format` | string | text | `"text"` or `"json"` |
| `tls.cert` | path | — | TLS certificate path |
| `tls.key` | path | — | TLS private key path |

---

## Frontend Integration

The player is an ES module with zero DOM dependencies — embeddable in any frontend project:

```js
import { RtspWebRTCPlayer } from './rtsp-webrtc-player.js';

const player = new RtspWebRTCPlayer({
  gateway: 'http://localhost:3000',
  videoElement: document.querySelector('video'),
});
player.onStateChange = (s) => console.log('state:', s);

await player.createAndPlay('rtsp://admin:pass@192.168.1.100:554/stream');
// Or play an existing stream: await player.play('uuid-from-api');

player.stop();
```

The player file is at `web/demo/rtsp-webrtc-player.js` — copy it into your project. The host page must load [adapter.js](https://github.com/webrtc/adapter) (WebRTC browser normalization shim).

## Roadmap

See [docs/roadmap.md](docs/roadmap.md)

## Tech Stack

Rust · Tokio · Axum · webrtc-rs · retina · WebSocket · H264
