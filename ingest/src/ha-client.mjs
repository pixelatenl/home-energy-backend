// HaClient: WebSocket client for the Home Assistant WebSocket API.
// Handles authentication with a long-lived access token, fetching the
// current states (get_states) and subscribing to state_changed events.
// On connection loss it reconnects with exponential backoff and
// subscribes again.
//
// Uses the built-in WebSocket of Node.js (Node >= 22), so no ws package
// is needed.

export class HaClient {
  /**
   * @param {object} options
   * @param {string} options.url - for example ws://localhost:8123/api/websocket
   * @param {string} options.token - long-lived access token from Home Assistant
   * @param {(states: Array) => void} options.onStates - baseline states after (re)connection
   * @param {(state: object) => void} options.onStateChanged - new_state object per event
   * @param {(...args: any[]) => void} options.log
   */
  constructor(options) {
    this.url = options.url;
    this.token = options.token;
    this.onStates = options.onStates;
    this.onStateChanged = options.onStateChanged;
    this.log = options.log ?? console.log;
    this.ws = null;
    this.nextId = 1;
    /** @type {Map<number, {resolve: Function, reject: Function}>} */
    this.pending = new Map();
    this.subscribed = false;
    this.backoffMs = 1000;
    this.stopped = false;
  }

  connect() {
    if (this.stopped) return;
    this.log("Connecting to Home Assistant:", this.url);
    this.ws = new WebSocket(this.url);

    this.ws.onopen = () => {
      // The server sends auth_required on its own; nothing to do here.
    };

    this.ws.onmessage = (event) => {
      let msg;
      try {
        msg = JSON.parse(event.data);
      } catch {
        return;
      }
      this.handleMessage(msg);
    };

    this.ws.onclose = () => {
      this.pending.clear();
      this.subscribed = false;
      if (this.stopped) return;
      const wait = this.backoffMs;
      this.backoffMs = Math.min(this.backoffMs * 2, 60000);
      this.log("Connection closed; reconnecting in", wait, "ms");
      setTimeout(() => this.connect(), wait);
    };

    this.ws.onerror = () => {
      // onclose follows; the reconnection logic is handled there.
    };
  }

  stop() {
    this.stopped = true;
    if (this.ws) this.ws.close();
  }

  handleMessage(msg) {
    if (msg.type === "auth_required") {
      this.send({ type: "auth", access_token: this.token });
      return;
    }
    if (msg.type === "auth_ok") {
      this.backoffMs = 1000;
      this.log("Authentication successful");
      this.afterAuth().catch((e) => this.log("Error while setting up subscription:", e.message));
      return;
    }
    if (msg.type === "auth_invalid") {
      this.log("Authentication failed: token is invalid or expired");
      this.stop();
      return;
    }
    if (msg.type === "event") {
      if (msg.event?.event_type === "state_changed" && msg.event?.data?.new_state) {
        this.onStateChanged(msg.event.data.new_state);
      }
      return;
    }
    if (msg.type === "result") {
      const p = this.pending.get(msg.id);
      if (p === undefined) return;
      this.pending.delete(msg.id);
      if (msg.success) {
        p.resolve(msg.result);
      } else {
        p.reject(new Error(msg.error?.message ?? "unknown API error"));
      }
    }
  }

  async afterAuth() {
    // 1. Fetch the current states as a baseline (includes last_updated per entity).
    const states = await this.command({ type: "get_states" });
    this.onStates(states ?? []);
    // 2. Subscribe to state changes (needed again after reconnection).
    await this.command({ type: "subscribe_events", event_type: "state_changed" });
    this.subscribed = true;
    this.log("Subscribed to state_changed");
  }

  /** Send a command and wait for its result (correlation by id). */
  command(payload) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.send({ ...payload, id });
    });
  }

  send(obj) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(obj));
    }
  }
}
