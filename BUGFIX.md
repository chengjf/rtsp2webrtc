# Bug Fix 清单

## ✅ 已修复

---

### BUG-001 · 部分 Chrome 浏览器无法播放（`invalid named curve`）

**状态**：✅ 已修复（commit `64de171`）

**现象**

Chrome 116+ 打开页面后，日志出现：

```
WARN webrtc::peer_connection::peer_connection_internal: Failed to start manager dtls: invalid named curve
INFO rtsp2webRTC::webrtc_peer: WebRTC state: Failed
```

视频始终黑屏，WebRTC 连接状态变为 `failed`。Edge 浏览器可正常播放。

**根因**

Chrome 116 起在 DTLS ClientHello 的 `supported_groups` 扩展中，将后量子混合
密钥协商算法 **X25519KYBER768Draft00**（`0x6399`）放在列表首位。

`webrtc-dtls 0.11` 的 `flight0.rs` 盲目取 `elliptic_curves[0]`，该值经
`NamedCurve::from(0x6399)` 映射为 `NamedCurve::Unsupported`，随后
`generate_keypair()` 返回 `ErrInvalidNamedCurve`，DTLS 握手立即失败。

Edge 的同版本 Chromium 内核默认把 `X25519(0x001d)` 排在首位，所以不受影响。

**修复方案**

在 `patches/webrtc-dtls/src/flight/flight0.rs` 中，将盲取首个曲线的逻辑改为
遍历客户端曲线列表，选出第一个服务端**实际支持**的曲线（X25519 / P-256 / P-384）：

```rust
// 修复前
state.named_curve = e.elliptic_curves[0];

// 修复后
state.named_curve = e
    .elliptic_curves
    .iter()
    .copied()
    .find(|c| !matches!(c, NamedCurve::Unsupported))
    .unwrap_or(e.elliptic_curves[0]);
```

通过 `Cargo.toml` 的 `[patch.crates-io]` 指向本地修改版：

```toml
[patch.crates-io]
webrtc-dtls = { path = "patches/webrtc-dtls" }
```

---

### BUG-002 · 本机 Chrome 无法播放（ICE candidate 竞态）

**状态**：✅ 已修复（commit `64de171`）

**现象**

服务端与浏览器在同一台机器上时，Chrome 打开页面后连接状态停在 `connecting`
或直接变为 `failed`，视频黑屏。从另一台机器访问则正常。

**根因**

`ws.onmessage` 声明为 `async`，但 WebSocket 不会等待前一个 handler 完成再触发
下一个，多条消息实际并发执行。

服务端发完 SDP offer 后几乎立刻发出 ICE candidates（本机延迟 ~0 ms）。此时
`sdp_offer` handler 的 `setRemoteDescription → createAnswer → setLocalDescription`
仍在异步执行，`ice_candidate` handler 已并发跑起来，在 remote description 尚未
设置时调用 `addIceCandidate()`，Chrome 抛出 `InvalidStateError`，被原来的
`catch (e) {}` 静默吞掉，server 的所有 ICE candidates 全部丢失。

远端机器因网络延迟（>10 ms），ICE candidates 到达时 offer 早已处理完，
所以没有触发该竞态。

**修复方案**

在 `web/index.html` 中：

1. **消息队列串行化**：将 `ws.onmessage` 改为 promise chain，保证消息按顺序
   串行处理，彻底消除并发竞态：

   ```js
   ws.onmessage = (event) => {
     msgQueue = msgQueue.then(() => handleMessage(event.data)).catch(...);
   };
   ```

2. **ICE candidate 暂存队列**：在 `setRemoteDescription` 完成前到达的
   candidate 先存入 `pendingCandidates[]`，offer 处理完毕后统一 flush，
   双重保障：

   ```js
   remoteDescReady = true;
   for (const c of pendingCandidates) {
     await pc.addIceCandidate(c);
   }
   pendingCandidates = [];
   ```

---

## 🔲 待修复

---

### BUG-003 · Firefox 浏览器无法播放

**状态**：✅ 已修复（代码侧兼容，待 Firefox 实机回归）

**现象**

Firefox 打开页面后视频无法播放。

**服务端错误日志**

```text
Failed to set remote description: set_remote_description called with no ice-ufrag
```

**根因**

根因分为两层：

1. **浏览器侧发送了“过早的 answer SDP”**

   页面代码原先在：

   ```js
   const answer = await pc.createAnswer();
   await pc.setLocalDescription(answer);
   ws.send(JSON.stringify({ type: 'sdp_answer', sdp: answer.sdp }));
   ```

   Firefox 中 `createAnswer()` 返回的 `answer.sdp` 可能仍是**未补全 ICE 凭据**的旧内容；
   真正稳定可用的 answer 应以 `await pc.setLocalDescription(answer)` 之后的
   `pc.localDescription.sdp` 为准。

   Chromium 系浏览器对此更宽松，因此 Chrome / Edge 上不容易暴露。

2. **服务端对 Firefox answer 额外做兼容补强**

3. **服务端同样不能发送 `create_offer()` 的原始 SDP**

   服务端原先在 `src/webrtc_peer.rs` 中使用：

   ```rust
   let offer = pc.create_offer(None).await?;
   pc.set_local_description(offer.clone()).await?;
   event_tx.send(PeerEvent::LocalDescription(offer));
   ```

   这会把 `create_offer()` 的原始 SDP 直接发给浏览器，而不是发送
   `set_local_description()` 之后的最终本地描述。

   `webrtc-rs` 自身测试也采用了：

   - `set_local_description(...)`
   - 然后读取 `local_description()`
   - 再把该 SDP 交给对端

   如果 offer 本身缺少 `a=ice-ufrag` / `a=ice-pwd`，Firefox 就可能继续生成一个同样
   缺少 ICE 凭据的 answer，最终在服务端 `set_remote_description()` 时失败。

   即使 Firefox 已返回 ICE 凭据，它们也可能只位于 `m=video` 媒体级属性中。
   为规避 `webrtc-rs 0.12` 在这条 answer 处理链上的兼容性边角，服务端会将
   媒体级 `a=ice-ufrag` / `a=ice-pwd` 提升到 session-level 后再解析。

**修复方案**

修复分两部分：

### 1. 浏览器端：发送 `pc.localDescription.sdp`

在 `web/index.html` 中，改为：

- `await pc.setLocalDescription(answer)` 之后发送 `pc.localDescription.sdp`
- 若 Firefox 尚未立刻写入 ICE 凭据，则短暂轮询等待，优先发送已包含
  `a=ice-ufrag` / `a=ice-pwd` 的版本
- 页面日志中增加 `Answer 已发送 (ice-ufrag=..., ice-pwd=...)` 诊断信息

### 2. 服务端：保留 answer SDP 规范化兜底

在 `src/signaling.rs` 中新增 `normalize_remote_answer_sdp()`：

1. 统一浏览器回传 SDP 的换行格式为 `\r\n`
2. 若 session-level 缺少 `a=ice-ufrag` / `a=ice-pwd`，则从首个媒体段提取同名属性
3. 在首个 `m=` 之前补回 session-level ICE 凭据，再交给 `RTCSessionDescription::answer()`

### 3. 服务端：发送 `local_description()` 里的最终 offer

在 `src/webrtc_peer.rs` 中，改为：

- `create_offer()` 后调用 `set_local_description()`
- 轮询 `local_description()`，优先等待包含 `a=ice-ufrag` / `a=ice-pwd` 的版本
- 发送最终 `local_description()`，而不是原始 `create_offer()` 结果
- 服务端日志中增加 `SDP offer created, sending to browser (ice-ufrag=..., ice-pwd=...)`

这样即使 Firefox 仅在媒体级携带 ICE 凭据，服务端也能稳定解析并继续后续 ICE/DTLS
流程。

同时移除了原先“SDP 解析失败后回退为空 SDP”的危险兜底逻辑，避免把真实兼容性问题
掩盖成后续的误导性错误。

**验证结果**

- ✅ `cargo test` 通过
- ✅ 新增单元测试，覆盖“媒体级 ICE 凭据提升到 session 级”的场景
- 🔲 待使用 Firefox 浏览器进行实机回归验证

