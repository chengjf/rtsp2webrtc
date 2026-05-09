# RTSP → WebRTC Gateway

低延迟 RTSP 转 WebRTC 流媒体网关。将 IP 摄像头 RTSP 流实时转发为浏览器原生 WebRTC 播放，无需插件、无需转码。

> English version: [README_EN.md](README_EN.md)

---

## 核心特性

- **零转码 RTP 转发** — 不解码帧，不重编码，最低 CPU 开销和延迟
- **多客户端共享** — 一路 RTSP 拉流，N 个浏览器同时观看
- **懒加载 + 空闲自动停止** — 有观众才拉流，所有人离开后 30 秒自动断开摄像头
- **动态流创建** — 客户端通过 API 提交 RTSP URL，无需服务端预配置
- **断线重连 + 保活** — 指数退避自动重连，55 秒超时保活防止摄像头断开会话
- **API 鉴权/限流** — 可选的 API key + 创建频率限制
- **监控就绪** — `/health`、`/metrics` (Prometheus)、结构化日志

---

## 编译

### Linux

```bash
# 依赖：Rust + GCC
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
sudo apt install build-essential    # Debian/Ubuntu
# 或: sudo dnf install gcc          # Fedora
# 或: sudo pacman -S gcc            # Arch

# 编译
git clone <repo-url> && cd rtsp2webRTC
cargo build --release
```

### Windows (MSVC)

```powershell
# 依赖：Rust + Visual Studio Build Tools
# 1. 安装 Rust：https://rustup.rs（选择 msvc 工具链）
# 2. 安装 VS Build Tools：https://visualstudio.microsoft.com/downloads/
#    勾选"使用 C++ 的桌面开发"工作负载

# 编译
git clone <repo-url> && cd rtsp2webRTC
cargo build --release
```

### Windows (GNU)

```bash
# 依赖：Rust（gnu 工具链）+ MinGW-w64
rustup default stable-x86_64-pc-windows-gnu
# 通过 Chocolatey 安装 MinGW-w64：
choco install mingw -y
# 或手动下载：https://www.mingw-w64.org

# 将 MinGW 加入 PATH，然后编译
export PATH="/c/ProgramData/mingw64/mingw64/bin:$PATH"
cargo build --release
```

> **说明：** `ring` crate（webrtc-rs 的 DTLS 依赖）需要 C 编译器。Linux 下用 GCC，Windows 下用 MSVC 工具链或 GNU + MinGW-w64。

### 交叉编译（Linux → Windows）

```bash
rustup target add x86_64-pc-windows-gnu
sudo apt install mingw-w64
cargo build --release --target x86_64-pc-windows-gnu
```

---

## 快速开始

### 1. 配置文件

创建 `config.toml`（或被 `CONFIG_PATH` 环境变量覆盖）：

```toml
[server]
bind_addr = "0.0.0.0:3000"
# api_key = "changeme"

[limits]
max_peers = 50
max_per_stream = 20
create_per_min = 10

[logging]
format = "text"    # "text" 或 "json"

[cors]
allowed_origins = ["*"]
```

### 2. 启动服务

```bash
cargo run --release
# 或设置日志级别
RUST_LOG=info cargo run --release
```

### 3. 打开浏览器

- **调试页面**：打开 `web/debugging/index.html`，功能完整（流列表、日志面板等），适合开发调试
- **Demo 页面**：打开 `web/demo/demo.html`，极简示例，展示播放器最简接入方式

确认网关地址为 `http://localhost:3000`，输入 RTSP URL 点击"创建并播放"即可。

---

## 使用方式

### 通过 Demo 页面

打开 `web/demo/demo.html`，输入 RTSP URL 点击 Play 即可观看。

### 通过 API

客户端提交 RTSP URL，服务端自动拉起转流任务：

```bash
curl -X POST http://localhost:3000/api/streams \
  -H "Content-Type: application/json" \
  -d '{"url":"rtsp://admin:pass@192.168.1.100:554/stream"}'
```

返回 `{"stream_id":"uuid"}`，然后浏览器连接：

```
ws://localhost:3000/ws?stream=uuid
```

---

## REST API

所有响应均为 `application/json`。

### 健康检查

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

### 流列表

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

### 创建动态流

```http
POST /api/streams
Authorization: Bearer changeme    # 如果配置了 api_key
Content-Type: application/json

{ "url": "rtsp://admin:pass@192.168.1.100:554/stream" }
```

```json
{ "stream_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890" }
```

### 流详情

```http
GET /api/streams/a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

### 删除动态流

```http
DELETE /api/streams/a1b2c3d4-e5f6-7890-abcd-ef1234567890
Authorization: Bearer changeme
```

→ `204 No Content`

### Prometheus 指标

```http
GET /metrics
```

---

## WebSocket 信令协议

### 客户端 → 服务端

```json
{ "type": "request_stream" }
{ "type": "sdp_answer", "sdp": "v=0\r\n..." }
{ "type": "ice_candidate", "candidate": "...", "sdpMid": "0" }
```

### 服务端 → 客户端

```json
{ "type": "connected" }
{ "type": "sdp_offer", "sdp": "v=0\r\n..." }
{ "type": "ice_candidate", "candidate": {...}, "sdpMid": "0" }
{ "type": "error", "message": "..." }
```

### 连接参数

| 参数 | 必填 | 说明 |
|------|:--:|------|
| `stream` | 是 | 流 ID（UUID，由 POST /api/streams 返回） |
| `key` | 否 | API key（若服务端配置了 `api_key` 则必填） |

示例：`ws://localhost:3000/ws?stream=a1b2c3d4-e5f6-7890-abcd-ef1234567890&key=changeme`

---

## 架构

```
RTSP Camera ──→ RtspPuller ──→ RtpRelay (broadcast) ──┬──→ WebRTC Peer 1 ──→ Browser 1
                                           ├──→ WebRTC Peer 2 ──→ Browser 2
                                           └──→ WebRTC Peer N ──→ Browser N
```

- **RtspPuller** — retina 库连接摄像头，接收原始 RTP 包
- **RtpRelay** — tokio broadcast channel 实现一路 RTP → N 个 WebRTC track
- **WebRtcPeer** — webrtc-rs 管理 PeerConnection，创建 H264 video track
- **StreamManager** — 流注册中心，管理生命周期、订阅者计数、连接限制
- **Signaling** — WebSocket SDP/ICE 交换

---

## 配置参考

| 字段 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `server.bind_addr` | string | `0.0.0.0:3000` | HTTP/WS 监听地址 |
| `server.api_key` | string | (空) | API 鉴权密钥，空则不启用 |
| `limits.max_peers` | int | 50 | 全局最大 WebRTC 连接数 |
| `limits.max_per_stream` | int | 20 | 单流最大观众数 |
| `limits.create_per_min` | int | 0 | 每分钟最大创建流次数，0=不限 |
| `cors.allowed_origins` | []string | [] | `["*"]` 全部放行，空=同源限制 |
| `logging.format` | string | text | `"text"` 或 `"json"` |
| `tls.cert` | path | — | TLS 证书路径 |
| `tls.key` | path | — | TLS 私钥路径 |

---

## 前端集成

播放器封装为 ES module，零 DOM 依赖，可嵌入任何前端工程：

```js
import { RtspWebRTCPlayer } from './rtsp-webrtc-player.js';

const player = new RtspWebRTCPlayer({
  gateway: 'http://localhost:3000',
  videoElement: document.querySelector('video'),
});
player.onStateChange = (s) => console.log('state:', s);

await player.createAndPlay('rtsp://admin:pass@192.168.1.100:554/stream');
// 或播放已有流: await player.play('uuid-from-api');

player.stop();
```

播放器文件位于 `web/demo/rtsp-webrtc-player.js`，拷贝到项目中即可使用。需确保宿主页面加载了 [adapter.js](https://github.com/webrtc/adapter)（WebRTC 浏览器标准化 shim）。

## 开发路线

见 [docs/roadmap.md](docs/roadmap.md)

## 技术栈

Rust · Tokio · Axum · webrtc-rs · retina · WebSocket · H264
