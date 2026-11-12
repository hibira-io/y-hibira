// In-memory stand-in for a Pusher server and pusher-js clients: client
// events reach every other subscriber of the channel, never the sender.
export class Bus {
  constructor() {
    this.clients = new Set();
  }
}

class Emitter {
  constructor() {
    this.handlers = new Map();
  }
  bind(ev, fn) {
    if (!this.handlers.has(ev)) this.handlers.set(ev, new Set());
    this.handlers.get(ev).add(fn);
  }
  unbind(ev, fn) {
    this.handlers.get(ev)?.delete(fn);
  }
  emit(ev, ...args) {
    for (const fn of [...(this.handlers.get(ev) ?? [])]) fn(...args);
  }
}

class FakeChannel extends Emitter {
  constructor(pusher, name) {
    super();
    this.pusher = pusher;
    this.name = name;
    this.subscribed = false;
  }
  trigger(event, data) {
    if (!this.subscribed) return false;
    const json = JSON.stringify(data);
    this.pusher.sent.push({ at: Date.now(), event, bytes: json.length });
    if (json.length > this.pusher.maxBytes) throw new Error(`event too big: ${json.length}`);
    for (const other of this.pusher.bus.clients) {
      if (other === this.pusher || !other.online) continue;
      const ch = other.channels.get(this.name);
      if (!ch?.subscribed) continue;
      const meta = this.name.startsWith('presence-') ? { user_id: this.pusher.userId } : {};
      setTimeout(() => ch.subscribed && ch.emit(event, JSON.parse(json), meta), other.latency);
    }
    return true;
  }
}

export class FakePusher {
  constructor(bus, { userId = 'u', latency = 1, maxBytes = 4096 } = {}) {
    this.bus = bus;
    this.userId = userId;
    this.latency = latency;
    this.maxBytes = maxBytes;
    this.online = true;
    this.channels = new Map();
    this.sent = [];
    this.connection = new Emitter();
    bus.clients.add(this);
  }
  subscribe(name) {
    let ch = this.channels.get(name);
    if (ch) return ch;
    ch = new FakeChannel(this, name);
    this.channels.set(name, ch);
    setTimeout(() => this._succeed(ch), 1);
    return ch;
  }
  _succeed(ch) {
    if (!this.online || !this.channels.has(ch.name)) return;
    ch.subscribed = true;
    ch.emit('pusher:subscription_succeeded', {});
    for (const other of this.bus.clients) {
      if (other === this) continue;
      other.channels.get(ch.name)?.emit('pusher:member_added', { id: this.userId });
    }
  }
  unsubscribe(name) {
    const ch = this.channels.get(name);
    if (!ch) return;
    ch.subscribed = false;
    this.channels.delete(name);
    this._left(name);
  }
  _left(name) {
    for (const other of this.bus.clients) {
      if (other === this) continue;
      other.channels.get(name)?.emit('pusher:member_removed', { id: this.userId });
    }
  }
  drop() {
    this.online = false;
    for (const ch of this.channels.values()) {
      ch.subscribed = false;
      this._left(ch.name);
    }
    this.connection.emit('state_change', { previous: 'connected', current: 'connecting' });
  }
  restore() {
    this.online = true;
    this.connection.emit('state_change', { previous: 'connecting', current: 'connected' });
    for (const ch of this.channels.values()) setTimeout(() => this._succeed(ch), 1);
  }
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function until(pred, ms = 3000) {
  const end = Date.now() + ms;
  while (!pred()) {
    if (Date.now() > end) throw new Error('timeout');
    await sleep(5);
  }
}
