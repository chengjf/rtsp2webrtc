/**
 * RTSP-to-WebRTC Player — reusable ES module.
 *
 * Encapsulates WebSocket signaling, WebRTC peer connection lifecycle,
 * ICE candidate management, and browser compatibility workarounds
 * (Firefox SDP ICE credentials, sdpMLineIndex normalization, etc.).
 *
 * Zero DOM dependencies. All external communication via callbacks.
 *
 * Usage:
 *   import { RtspWebRTCPlayer } from './rtsp-webrtc-player.js';
 *   const player = new RtspWebRTCPlayer({ gateway: 'http://localhost:3000' });
 *   player.onTrack = stream => video.srcObject = stream;
 *   await player.createAndPlay('rtsp://...');
 */

const DEFAULT_ICE_SERVERS = [{ urls: 'stun:stun.l.google.com:19302' }];

export class RtspWebRTCPlayer {
  /**
   * @param {Object} options
   * @param {string} options.gateway      — Gateway base URL, e.g. "http://localhost:3000"
   * @param {HTMLVideoElement} [options.videoElement] — If provided, auto-sets srcObject on track
   * @param {RTCIceServer[]} [options.iceServers]     — Default: Google STUN
   * @param {string} [options.apiKey]                 — If server requires auth
   */
  constructor({ gateway, videoElement, iceServers, apiKey } = {}) {
    if (!gateway) throw new Error('gateway is required');
    this._gateway = gateway.replace(/\/$/, '');
    this._wsBase = this._gateway.replace(/^http/, 'ws');
    this._videoElement = videoElement || null;
    this._iceServers = iceServers || DEFAULT_ICE_SERVERS;
    this._apiKey = apiKey || '';

    // Internal state
    this._pc = null;
    this._ws = null;
    this._streamId = null;
    this._state = 'idle';

    // ICE candidate queues
    this._pendingCandidates = [];
    this._remoteDescReady = false;
    this._pendingLocalCandidates = [];
    this._localAnswerSent = false;
    this._localIceWaiters = [];

    // Serialized WebSocket message processing (prevents BUG-002 race)
    this._msgQueue = Promise.resolve();

    // Callbacks — set by user after construction
    this.onStateChange = null;  // (state: 'connecting'|'connected'|'disconnected'|'error') => void
    this.onTrack = null;        // (stream: MediaStream) => void
    this.onError = null;        // (error: Error) => void
    this.onLog = null;          // (level: 'info'|'warn'|'error', message: string) => void
  }

  get streamId() { return this._streamId; }
  get state() { return this._state; }

  // ── Public API ──────────────────────────────────────────────

  /**
   * Create a dynamic stream via POST /api/streams, then connect and play.
   * @param {string} rtspUrl
   */
  async createAndPlay(rtspUrl) {
    this.stop();
    this._setState('connecting');
    this._log('info', `POST ${this._gateway}/api/streams …`);

    const res = await fetch(`${this._gateway}/api/streams`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: rtspUrl }),
    });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.message || `create stream failed (${res.status})`);
    }
    this._log('info', `stream created: ${data.stream_id}`);
    await this.play(data.stream_id);
  }

  /**
   * Play an existing stream by its UUID.
   * @param {string} streamId
   */
  async play(streamId) {
    this.stop();
    this._streamId = streamId;
    this._setState('connecting');
    this._connect();
  }

  /**
   * Disconnect WebSocket and close PeerConnection.  Idempotent.
   */
  stop() {
    if (this._pc) {
      this._pc.close();
      this._pc = null;
    }
    if (this._ws) {
      this._ws.close();
      this._ws = null;
    }
    this._remoteDescReady = false;
    this._pendingCandidates = [];
    this._pendingLocalCandidates = [];
    this._localAnswerSent = false;
    this._localIceWaiters = [];
    this._msgQueue = Promise.resolve();
    if (this._state !== 'idle') {
      this._setState('disconnected');
    }
    this._streamId = null;
    this._state = 'idle';
    this._log('info', 'disconnected');
  }

  // ── WebSocket & Signaling ───────────────────────────────────

  _connect() {
    let url = `${this._wsBase}/ws?stream=${this._streamId}`;
    if (this._apiKey) url += `&key=${encodeURIComponent(this._apiKey)}`;

    this._log('info', `WebSocket connecting: ${url}`);
    this._msgQueue = Promise.resolve();
    this._ws = new WebSocket(url);

    this._ws.onopen = () => {
      this._log('info', 'connected, requesting stream...');
      this._ws.send(JSON.stringify({ type: 'request_stream' }));
    };

    this._ws.onmessage = (event) => {
      this._msgQueue = this._msgQueue.then(() => this._handleMessage(event.data)).catch(e => {
        this._log('error', `message handler error: ${e}`);
      });
    };

    this._ws.onerror = () => this._log('error', 'WebSocket error');
    this._ws.onclose = (e) => {
      this._log('info', `WebSocket closed (code=${e.code})`);
      if (this._state === 'connected') {
        this._setState('disconnected');
      }
    };
  }

  async _handleMessage(data) {
    const msg = JSON.parse(data);
    this._log('info', `← ${msg.type}`);

    switch (msg.type) {
      case 'connected':
        this._remoteDescReady = false;
        this._pendingCandidates = [];
        this._pendingLocalCandidates = [];
        this._localAnswerSent = false;
        this._localIceWaiters = [];

        this._pc = new RTCPeerConnection({ iceServers: this._iceServers });

        this._pc.ontrack = (e) => {
          this._log('info', `video track ready: ${e.track.kind}`);
          if (this._videoElement) {
            this._videoElement.srcObject = e.streams[0];
          }
          if (this.onTrack) this.onTrack(e.streams[0]);
        };

        this._pc.onicecandidate = (e) => {
          this._notifyLocalIceProgress();
          if (e.candidate) {
            const message = {
              type: 'ice_candidate',
              candidate: e.candidate.candidate,
              sdpMid: e.candidate.sdpMid,
              sdpMLineIndex: e.candidate.sdpMLineIndex ?? 0,
            };
            if (this._localAnswerSent && this._ws && this._ws.readyState === WebSocket.OPEN) {
              this._ws.send(JSON.stringify(message));
            } else {
              this._pendingLocalCandidates.push(message);
            }
          } else {
            this._log('info', 'local ICE gathering complete');
          }
        };

        this._pc.onicegatheringstatechange = () => {
          this._log('info', `local ICE gathering: ${this._pc.iceGatheringState}`);
          this._notifyLocalIceProgress();
          if (this._localAnswerSent) this._flushPendingLocalCandidates();
        };

        this._pc.onconnectionstatechange = () => {
          const s = this._pc.connectionState;
          this._log(s === 'connected' ? 'info' : s === 'failed' ? 'error' : 'info',
            `connection state: ${s}`);
          if (s === 'connected') this._setState('connected');
          if (s === 'failed') this._setState('error');
        };

        this._pc.oniceconnectionstatechange = () => {
          this._log('info', `ICE state: ${this._pc.iceConnectionState}`);
        };
        break;

      case 'sdp_offer': {
        const hasUfrag = /(^|\r\n|\n)a=ice-ufrag:/.test(msg.sdp);
        const hasPwd = /(^|\r\n|\n)a=ice-pwd:/.test(msg.sdp);
        this._log('info', `received SDP offer (ice-ufrag=${hasUfrag}, ice-pwd=${hasPwd})`);

        await this._pc.setRemoteDescription({ type: 'offer', sdp: msg.sdp });
        const localSdp = await this._buildAnswerSdpWithIce();

        const localHasUfrag = /(^|\r\n|\n)a=ice-ufrag:/.test(localSdp);
        const localHasPwd = /(^|\r\n|\n)a=ice-pwd:/.test(localSdp);
        this._localAnswerSent = true;

        this._ws.send(JSON.stringify({ type: 'sdp_answer', sdp: localSdp }));
        this._flushPendingLocalCandidates();
        this._log('info',
          `answer sent (ice-ufrag=${localHasUfrag}, ice-pwd=${localHasPwd}, ` +
          `iceGathering=${this._pc.iceGatheringState}, signaling=${this._pc.signalingState})`);

        // Flush queued remote ICE candidates
        this._remoteDescReady = true;
        for (const c of this._pendingCandidates) {
          try {
            await this._pc.addIceCandidate(new RTCIceCandidate(c));
          } catch (e) {
            this._log('error', `addIceCandidate(queued) failed: ${e}`);
          }
        }
        this._pendingCandidates = [];
        break;
      }

      case 'ice_candidate': {
        const init = msg.candidate && typeof msg.candidate === 'object'
          ? msg.candidate
          : { candidate: msg.candidate, sdpMid: msg.sdpMid, sdpMLineIndex: msg.sdpMLineIndex ?? 0 };

        if (!init?.candidate) break;

        // Firefox requires sdpMLineIndex to be a number
        if (init.sdpMLineIndex == null) init.sdpMLineIndex = 0;

        if (this._remoteDescReady) {
          try {
            await this._pc.addIceCandidate(new RTCIceCandidate(init));
          } catch (e) {
            this._log('error', `addIceCandidate failed: ${e}`);
          }
        } else {
          this._pendingCandidates.push(init);
          this._log('info', `ICE candidate queued (${this._pendingCandidates.length})`);
        }
        break;
      }

      case 'error':
        this._log('error', `server error: ${msg.message}`);
        if (this.onError) this.onError(new Error(msg.message));
        break;
    }
  }

  // ── SDP / ICE helpers ────────────────────────────────────────

  _hasIceCredentials(sdp) {
    return /(^|\r\n|\n)a=ice-ufrag:/.test(sdp) && /(^|\r\n|\n)a=ice-pwd:/.test(sdp);
  }

  _notifyLocalIceProgress() {
    const waiters = this._localIceWaiters;
    this._localIceWaiters = [];
    for (const resolve of waiters) resolve();
  }

  _waitForNextLocalIceProgress(timeoutMs = 250) {
    return new Promise(resolve => {
      let finished = false;
      const done = () => {
        if (finished) return;
        finished = true;
        clearTimeout(timer);
        const idx = this._localIceWaiters.indexOf(done);
        if (idx >= 0) this._localIceWaiters.splice(idx, 1);
        resolve();
      };
      const timer = setTimeout(done, timeoutMs);
      this._localIceWaiters.push(done);
    });
  }

  _flushPendingLocalCandidates() {
    if (!this._localAnswerSent || !this._ws || this._ws.readyState !== WebSocket.OPEN) return;
    for (const candidate of this._pendingLocalCandidates) {
      this._ws.send(JSON.stringify(candidate));
    }
    if (this._pendingLocalCandidates.length) {
      this._log('info', `local ICE candidates sent (${this._pendingLocalCandidates.length})`);
    }
    this._pendingLocalCandidates = [];
  }

  async _waitForLocalDescriptionWithIce(timeoutMs = 4000) {
    const started = Date.now();
    let latest = this._pc.localDescription?.sdp || '';

    while (Date.now() - started < timeoutMs) {
      latest = this._pc.localDescription?.sdp || latest;
      if (latest && this._hasIceCredentials(latest)) return latest;
      if (this._pc.iceGatheringState === 'complete' && latest) return latest;
      await this._waitForNextLocalIceProgress(250);
    }

    return this._pc.localDescription?.sdp || latest;
  }

  /**
   * Build an SDP answer with ICE credentials.
   *
   * Three paths for cross-browser compatibility (see BUGFIX.md BUG-003):
   * 1. Implicit setLocalDescription() populates ICE credentials (Chrome/Edge).
   * 2. Explicit createAnswer() fallback for engines that don't populate fast enough.
   * 3. restartIce() as last resort (Firefox sometimes needs an ICE restart).
   */
  async _buildAnswerSdpWithIce() {
    // Path 1: browser-managed answer generation
    await this._pc.setLocalDescription();
    let sdp = await this._waitForLocalDescriptionWithIce();
    if (this._hasIceCredentials(sdp)) return sdp;

    // Path 2: explicit createAnswer fallback
    if (this._pc.signalingState === 'have-remote-offer') {
      this._log('info', 'answer lacks ICE credentials, trying explicit createAnswer fallback');
      try {
        const explicitAnswer = await this._pc.createAnswer();
        await this._pc.setLocalDescription(explicitAnswer);
        sdp = await this._waitForLocalDescriptionWithIce();
        if (this._hasIceCredentials(sdp)) return sdp;
      } catch (e) {
        this._log('error', `explicit createAnswer fallback failed: ${e}`);
      }
    }

    // Path 3: restart ICE
    if (typeof this._pc.restartIce === 'function') {
      this._log('info', 'answer still lacks ICE credentials, trying restartIce()');
      try {
        this._pc.restartIce();
        await this._waitForNextLocalIceProgress(500);
        sdp = await this._waitForLocalDescriptionWithIce(2000);
      } catch (e) {
        this._log('error', `restartIce failed: ${e}`);
      }
    }

    return sdp;
  }

  // ── Internal helpers ─────────────────────────────────────────

  _setState(s) {
    if (this._state !== s) {
      this._state = s;
      if (this.onStateChange) this.onStateChange(s);
    }
  }

  _log(level, message) {
    if (this.onLog) this.onLog(level, message);
  }
}
