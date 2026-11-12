// Yjs provider for Hibira Realtime and any Pusher-compatible server.
//
// A shared document is a private- or presence- channel. Peers talk to each
// other with client events (one event name, client-yjs), so the app needs
// client events turned on and nothing else runs on the server:
//
//   - when a peer subscribes it sends its state vector (sync step 1);
//   - each peer that has something the newcomer lacks answers with the
//     missing updates and its own state vector (sync step 2), after a short
//     random delay; a peer that sees another answer covering everything it
//     has stays quiet, so a big room does not answer N times;
//   - the newcomer applies the first answer and sends back whatever the
//     answering peer lacks, so edits made offline reach everyone;
//   - after that every local change is sent as an update, and awareness
//     (cursors, names, selections) as awareness updates.
//
// Client events are not echoed to the sender and are not stored by the
// server, so the document lives in the peers: keep your own copy (see the
// README) for when nobody is connected.
//
// Servers limit client events per connection (10 per second on Hibira and
// Pusher) and per size (4 KB on Hibira Free, 10 KB on Pusher), so the
// provider sends at most maxRate events a second, merges updates waiting
// for their turn into one, and splits anything bigger than maxEventBytes
// into chunks.

import * as Y from 'yjs';
import * as awarenessProtocol from 'y-protocols/awareness';
import { ObservableV2 } from 'lib0/observable';
import { toBase64, fromBase64 } from 'lib0/buffer';

export const EVENT = 'client-yjs';

const DEFAULTS = {
  maxRate: 8,
  maxEventBytes: 4000,
  answerDelay: 250,
  syncTimeout: 1500,
  chunkTimeout: 30000,
};

// Bytes a frame takes besides its d field: JSON keys, ids, the channel and
// event names pusher-js wraps it in. Generous on purpose.
const FRAME_OVERHEAD = 400;

export class HibiraProvider extends ObservableV2 {
  /**
   * Subscribe to a channel and keep the document in sync on it.
   * @param {import('pusher-js').default} pusher
   * @param {string} channelName a private- or presence- channel
   * @param {Y.Doc} doc
   * @param {HibiraProviderOptions} [options]
   */
  constructor(pusher, channelName, doc, options = {}) {
    super();
    checkChannelName(channelName);
    this.doc = doc;
    this.channelName = channelName;
    this._ownsAwareness = !options.awareness;
    this.awareness = options.awareness ?? new awarenessProtocol.Awareness(doc);
    this.synced = false;
    this.status = 'disconnected';
    this._opts = { ...DEFAULTS, ...options };
    this._id = String(doc.clientID);
    this._pusher = pusher;
    this._channel = null;
    this._ownsChannel = false;
    this._queue = [];
    this._frames = [];
    this._sentAt = [];
    this._pumpTimer = null;
    this._answers = new Map();
    this._chunks = new Map();
    this._users = new Map();
    this._syncTimer = null;
    this._chunkSeq = 0;
    this._destroyed = false;

    this._onDocUpdate = (update, origin) => {
      if (origin !== this) this._enqueueUpdate(update);
    };
    this._onAwarenessUpdate = ({ added, updated, removed }, origin) => {
      if (origin === this) return;
      this._enqueueAwareness(added.concat(updated, removed));
    };
    this._onEvent = (data, metadata) => this._receive(data, metadata);
    this._onSubscribed = () => this._startSync();
    this._onMemberRemoved = (member) => this._memberRemoved(member);
    this._onConnectionState = ({ current }) => {
      if (current !== 'connected') this._lost();
    };
    this._onUnload = () => this._sayGoodbye();

    doc.on('update', this._onDocUpdate);
    this.awareness.on('update', this._onAwarenessUpdate);
    if (typeof window !== 'undefined' && window.addEventListener) {
      window.addEventListener('pagehide', this._onUnload);
    }
    if (options.connect !== false) this.connect();
  }

  /**
   * Keep a document in sync on a channel you already subscribed to, for
   * example with Laravel Echo: `HibiraProvider.fromChannel(Echo.private('doc.42').subscription, doc)`.
   * Create it right after subscribing so no event is missed.
   * @param {object} channel pusher-js PrivateChannel or PresenceChannel
   * @param {Y.Doc} doc
   * @param {HibiraProviderOptions} [options]
   */
  static fromChannel(channel, doc, options = {}) {
    const provider = new HibiraProvider(channel.pusher ?? null, channel.name, doc, { ...options, connect: false });
    provider._attach(channel, false);
    return provider;
  }

  /** Subscribe (again) and start syncing. */
  connect() {
    if (this._destroyed || this._channel) return;
    if (!this._pusher) throw new Error('HibiraProvider: no pusher client to subscribe with');
    this._attach(this._pusher.subscribe(this.channelName), true);
  }

  /** Tell peers you left, stop syncing and unsubscribe (if the provider subscribed). */
  disconnect() {
    if (!this._channel) return;
    this._sayGoodbye();
    const channel = this._channel;
    channel.unbind(EVENT, this._onEvent);
    channel.unbind('pusher:subscription_succeeded', this._onSubscribed);
    channel.unbind('pusher:member_removed', this._onMemberRemoved);
    this._pusher?.connection?.unbind('state_change', this._onConnectionState);
    if (this._ownsChannel) this._pusher.unsubscribe(this.channelName);
    this._channel = null;
    this._ownsChannel = false;
    this._lost();
  }

  destroy() {
    if (this._destroyed) return;
    this.disconnect();
    this._destroyed = true;
    this.doc.off('update', this._onDocUpdate);
    this.awareness.off('update', this._onAwarenessUpdate);
    if (this._ownsAwareness) this.awareness.destroy();
    if (typeof window !== 'undefined' && window.removeEventListener) {
      window.removeEventListener('pagehide', this._onUnload);
    }
    super.destroy();
  }

  _attach(channel, owns) {
    checkChannelName(channel.name);
    this._channel = channel;
    this._ownsChannel = owns;
    channel.bind(EVENT, this._onEvent);
    channel.bind('pusher:subscription_succeeded', this._onSubscribed);
    channel.bind('pusher:member_removed', this._onMemberRemoved);
    this._pusher?.connection?.bind('state_change', this._onConnectionState);
    this._setStatus('connecting');
    if (channel.subscribed) this._startSync();
  }

  _setStatus(status) {
    if (this.status === status) return;
    this.status = status;
    this.emit('status', [{ status }]);
  }

  _setSynced(synced) {
    if (this.synced === synced) return;
    this.synced = synced;
    this.emit('synced', [synced]);
    this.emit('sync', [synced]);
  }

  _startSync() {
    this._setStatus('connected');
    this._setSynced(false);
    this._queue = [];
    this._frames = [];
    this._push({ k: 's1', d: Y.encodeStateVector(this.doc) });
    this._enqueueAwareness([this.doc.clientID]);
    clearTimeout(this._syncTimer);
    this._syncTimer = setTimeout(() => this._setSynced(true), this._opts.syncTimeout);
  }

  // Connection lost: peers may change while we are away, so forget their
  // awareness and sync again on the next subscription_succeeded.
  _lost() {
    clearTimeout(this._syncTimer);
    clearTimeout(this._pumpTimer);
    this._pumpTimer = null;
    for (const t of this._answers.values()) clearTimeout(t.timer);
    this._answers.clear();
    this._chunks.clear();
    this._queue = [];
    this._frames = [];
    const remote = [...this.awareness.getStates().keys()].filter((c) => c !== this.doc.clientID);
    if (remote.length) awarenessProtocol.removeAwarenessStates(this.awareness, remote, this);
    this._users.clear();
    this._setSynced(false);
    this._setStatus(this._channel ? 'connecting' : 'disconnected');
  }

  _sayGoodbye() {
    if (!this._channel?.subscribed) return;
    const msg = {
      k: 'a',
      d: awarenessProtocol.encodeAwarenessUpdate(this.awareness, [this.doc.clientID], new Map([[this.doc.clientID, null]])),
    };
    // Best effort and out of turn: the page is going away.
    for (const frame of this._frames_of(msg)) this._channel.trigger(EVENT, frame);
  }

  _memberRemoved(member) {
    const gone = [];
    for (const [client, user] of this._users) {
      if (user === member?.id) {
        gone.push(client);
        this._users.delete(client);
      }
    }
    if (gone.length) awarenessProtocol.removeAwarenessStates(this.awareness, gone, this);
  }

  // Outgoing ---------------------------------------------------------------

  _enqueueUpdate(update) {
    if (!this._channel?.subscribed) return; // the next sync carries it
    const pending = this._queue.find((m) => m.k === 'u');
    if (pending) pending.d = Y.mergeUpdates([pending.d, update]);
    else this._push({ k: 'u', d: update });
  }

  _enqueueAwareness(clients) {
    if (!this._channel?.subscribed) return;
    const pending = this._queue.find((m) => m.k === 'a');
    if (pending) for (const c of clients) pending.clients.add(c);
    else this._push({ k: 'a', clients: new Set(clients) });
  }

  _push(msg) {
    this._queue.push(msg);
    this._pump();
  }

  _pump() {
    if (this._pumpTimer) return;
    while (this._channel?.subscribed && (this._frames.length || this._queue.length)) {
      // At most maxRate sends in any 1 s window (sliding, so a burst never
      // goes past what the server counts).
      const now = Date.now();
      while (this._sentAt.length && this._sentAt[0] <= now - 1000) this._sentAt.shift();
      if (this._sentAt.length >= this._opts.maxRate) {
        const wait = this._sentAt[0] + 1001 - now;
        this._pumpTimer = setTimeout(() => {
          this._pumpTimer = null;
          this._pump();
        }, wait);
        return;
      }
      if (!this._frames.length) {
        const msg = this._queue.shift();
        if (msg.k === 'a') {
          msg.d = awarenessProtocol.encodeAwarenessUpdate(this.awareness, [...msg.clients]);
          delete msg.clients;
        }
        this._frames.push(...this._frames_of(msg));
      }
      this._sentAt.push(now);
      this._channel.trigger(EVENT, this._frames.shift());
    }
  }

  _frames_of(msg) {
    const base = { f: this._id, k: msg.k };
    if (msg.t) base.t = msg.t;
    if (msg.v) base.v = toBase64(msg.v);
    const d = toBase64(msg.d);
    const room = this._opts.maxEventBytes - FRAME_OVERHEAD - (base.v?.length ?? 0);
    if (room <= 0) throw new Error('HibiraProvider: maxEventBytes is too small');
    if (d.length <= room) return [{ ...base, d }];
    const n = Math.ceil(d.length / room);
    const x = `${this._id}.${++this._chunkSeq}`;
    const frames = [];
    for (let i = 0; i < n; i++) frames.push({ ...base, x, i, n, d: d.slice(i * room, (i + 1) * room) });
    return frames;
  }

  // Incoming ---------------------------------------------------------------

  _receive(frame, metadata) {
    if (!frame || typeof frame !== 'object' || frame.f === this._id || typeof frame.d !== 'string') return;
    if (metadata?.user_id != null) {
      const client = Number(frame.f);
      if (Number.isFinite(client)) this._users.set(client, metadata.user_id);
    }
    if (frame.t && frame.t !== this._id) {
      if (frame.k === 's2') this._answeredByOther(frame);
      return;
    }
    let msg = frame;
    if (frame.n > 1) {
      msg = this._assemble(frame);
      if (!msg) return;
    }
    try {
      this._handle(msg);
    } catch (err) {
      this.emit('error', [err]);
    }
  }

  // Someone else answered a newcomer: skip our own answer when theirs holds
  // every update we have. Every chunk carries v, so no need to assemble.
  _answeredByOther(frame) {
    const answer = this._answers.get(frame.t);
    if (!answer || typeof frame.v !== 'string') return;
    if (covers(fromBase64(frame.v), Y.encodeStateVector(this.doc))) {
      clearTimeout(answer.timer);
      this._answers.delete(frame.t);
    }
  }

  _assemble(frame) {
    const key = frame.x;
    let entry = this._chunks.get(key);
    if (!entry) {
      entry = { parts: new Array(frame.n), got: 0, frame };
      entry.timer = setTimeout(() => this._chunks.delete(key), this._opts.chunkTimeout);
      this._chunks.set(key, entry);
    }
    if (entry.parts[frame.i] === undefined) {
      entry.parts[frame.i] = frame.d;
      entry.got++;
    }
    if (frame.v) entry.frame = frame;
    if (entry.got < frame.n) return null;
    clearTimeout(entry.timer);
    this._chunks.delete(key);
    return { ...entry.frame, d: entry.parts.join('') };
  }

  _handle(msg) {
    const from = msg.f;
    switch (msg.k) {
      case 's1': {
        const theirs = fromBase64(msg.d);
        const answer = this._answers.get(from);
        if (answer) clearTimeout(answer.timer);
        const timer = setTimeout(() => {
          this._answers.delete(from);
          // Always answer: deletes do not show in a state vector, so equal
          // vectors can still hide a delete the newcomer missed.
          this._push({ k: 's2', t: from, d: Y.encodeStateAsUpdate(this.doc, theirs), v: Y.encodeStateVector(this.doc) });
        }, Math.random() * this._opts.answerDelay);
        this._answers.set(from, { timer });
        this._enqueueAwareness([this.doc.clientID]);
        return;
      }
      case 's2': {
        const vector = msg.v ? fromBase64(msg.v) : null;
        Y.applyUpdate(this.doc, fromBase64(msg.d), this);
        if (!this.synced) {
          if (vector) {
            const missing = Y.encodeStateAsUpdate(this.doc, vector);
            if (!isEmptyUpdate(missing)) this._enqueueUpdate(missing);
          }
          clearTimeout(this._syncTimer);
          this._setSynced(true);
        }
        return;
      }
      case 'u':
        Y.applyUpdate(this.doc, fromBase64(msg.d), this);
        return;
      case 'a':
        awarenessProtocol.applyAwarenessUpdate(this.awareness, fromBase64(msg.d), this);
        return;
      default:
    }
  }
}

function checkChannelName(name) {
  const ok = (name.startsWith('private-') && !name.startsWith('private-encrypted-')) ||
    (name.startsWith('presence-') && !name.startsWith('presence-large-'));
  if (!ok) {
    throw new Error(`HibiraProvider: "${name}" must be a private- or presence- channel (client events are not allowed on public, private-encrypted- or presence-large- channels)`);
  }
}

// An update with no structs and no deletes encodes as [0, 0].
function isEmptyUpdate(update) {
  return update.length <= 2 && update.every((b) => b === 0);
}

// True when state vector a has seen every clock of state vector b.
function covers(a, b) {
  const sa = Y.decodeStateVector(a);
  for (const [client, clock] of Y.decodeStateVector(b)) {
    if ((sa.get(client) ?? 0) < clock) return false;
  }
  return true;
}

/**
 * @typedef {object} HibiraProviderOptions
 * @property {boolean} [connect=true] subscribe right away
 * @property {import('y-protocols/awareness').Awareness} [awareness] share an existing Awareness
 * @property {number} [maxRate=8] client events sent per second at most
 * @property {number} [maxEventBytes=4000] bigger messages are split into chunks
 * @property {number} [answerDelay=250] ms; peers answer a newcomer after a random delay up to this
 * @property {number} [syncTimeout=1500] ms without an answer after which you are alone and synced
 * @property {number} [chunkTimeout=30000] ms to wait for the rest of a split message
 */
