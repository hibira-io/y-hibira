import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as Y from 'yjs';
import { HibiraProvider } from '../index.js';
import { Bus, FakePusher, sleep, until } from './fake.mjs';

const CH = 'private-doc-1';
const fast = { answerDelay: 20, syncTimeout: 150 };

function peer(bus, opts = {}, pusherOpts = {}) {
  const doc = new Y.Doc();
  const pusher = new FakePusher(bus, pusherOpts);
  const provider = new HibiraProvider(pusher, opts.channel ?? CH, doc, { ...fast, ...opts });
  return { doc, pusher, provider, text: doc.getText('t') };
}

const synced = (p) => until(() => p.provider.synced);

test('rejects channels without client events', () => {
  const bus = new Bus();
  const doc = new Y.Doc();
  for (const name of ['doc', 'private-encrypted-doc', 'presence-large-doc']) {
    assert.throws(() => new HibiraProvider(new FakePusher(bus), name, doc), /private- or presence-/);
  }
});

test('alone: synced after syncTimeout', async () => {
  const bus = new Bus();
  const a = peer(bus);
  const events = [];
  a.provider.on('synced', (s) => events.push(s));
  assert.equal(a.provider.synced, false);
  await synced(a);
  assert.deepEqual(events, [true]);
  assert.equal(a.provider.status, 'connected');
  a.provider.destroy();
});

test('late joiner gets the document and edits flow both ways', async () => {
  const bus = new Bus();
  const a = peer(bus);
  await synced(a);
  a.text.insert(0, 'hello');
  const b = peer(bus);
  await synced(b);
  await until(() => b.text.toString() === 'hello');
  b.text.insert(5, ' world');
  await until(() => a.text.toString() === 'hello world');
  a.text.delete(0, 6);
  await until(() => b.text.toString() === 'world');
  a.provider.destroy();
  b.provider.destroy();
});

test('newcomer pushes edits it made before connecting', async () => {
  const bus = new Bus();
  const a = peer(bus);
  a.text.insert(0, 'A');
  await synced(a);
  const b = peer(bus, { connect: false });
  b.text.insert(0, 'B');
  b.provider.connect();
  await synced(b);
  await until(() => a.text.toString().length === 2 && a.text.toString() === b.text.toString());
  a.provider.destroy();
  b.provider.destroy();
});

test('offline edits, including deletes, merge after a reconnect', async () => {
  const bus = new Bus();
  const a = peer(bus);
  const b = peer(bus);
  await synced(a);
  a.text.insert(0, 'abcdef');
  await until(() => b.text.toString() === 'abcdef');

  b.pusher.drop();
  await until(() => !b.provider.synced);
  a.text.delete(0, 3); // a delete does not move a's state vector
  b.text.insert(6, 'XYZ');
  b.pusher.restore();
  await synced(b);
  await until(() => a.text.toString() === 'defXYZ' && b.text.toString() === 'defXYZ');
  a.provider.destroy();
  b.provider.destroy();
});

test('big updates are split into chunks under maxEventBytes', async () => {
  const bus = new Bus();
  const opts = { maxEventBytes: 1200 };
  const a = peer(bus, opts, { maxBytes: 1200 });
  const b = peer(bus, opts, { maxBytes: 1200 });
  await synced(a);
  await synced(b);
  const big = 'x'.repeat(5000) + Math.random();
  a.text.insert(0, big);
  await until(() => b.text.toString() === big);
  assert.ok(a.pusher.sent.length > 4, `sent ${a.pusher.sent.length}`);
  assert.ok(a.pusher.sent.every((s) => s.bytes <= 1200));

  const c = peer(bus, opts, { maxBytes: 1200 });
  await until(() => c.text.toString() === big);
  for (const p of [a, b, c]) p.provider.destroy();
});

test('never sends more than maxRate events a second, merging updates', async () => {
  const bus = new Bus();
  const a = peer(bus, { maxRate: 5 });
  const b = peer(bus);
  await synced(a);
  await synced(b);
  await sleep(1100); // let the token bucket fill back up
  a.pusher.sent = [];
  for (let i = 0; i < 100; i++) a.text.insert(i, String(i % 10));
  await until(() => b.text.toString() === a.text.toString());
  await sleep(50);
  const times = a.pusher.sent.map((s) => s.at);
  for (let i = 0; i < times.length; i++) {
    const inWindow = times.filter((t) => t >= times[i] && t < times[i] + 1000).length;
    assert.ok(inWindow <= 5, `${inWindow} events within a second`);
  }
  assert.ok(a.pusher.sent.length < 100, 'updates were merged');
  a.provider.destroy();
  b.provider.destroy();
});

test('awareness reaches peers and goes away on disconnect', async () => {
  const bus = new Bus();
  const a = peer(bus);
  const b = peer(bus);
  await synced(a);
  await synced(b);
  a.provider.awareness.setLocalStateField('user', { name: 'Alice' });
  await until(() => b.provider.awareness.getStates().get(a.doc.clientID)?.user?.name === 'Alice');

  const c = peer(bus);
  await until(() => c.provider.awareness.getStates().get(a.doc.clientID)?.user?.name === 'Alice');

  a.provider.disconnect();
  await until(() => !b.provider.awareness.getStates().has(a.doc.clientID));
  await until(() => !c.provider.awareness.getStates().has(a.doc.clientID));
  for (const p of [a, b, c]) p.provider.destroy();
});

test('presence channel: awareness of a member that drops is removed', async () => {
  const bus = new Bus();
  const channel = 'presence-doc-1';
  const a = peer(bus, { channel }, { userId: 'alice' });
  const b = peer(bus, { channel }, { userId: 'bob' });
  await synced(a);
  await synced(b);
  a.provider.awareness.setLocalStateField('user', { name: 'Alice' });
  await until(() => b.provider.awareness.getStates().has(a.doc.clientID));
  a.pusher.drop(); // no goodbye: the connection just died
  await until(() => !b.provider.awareness.getStates().has(a.doc.clientID));
  a.provider.destroy();
  b.provider.destroy();
});

test('a room answers a newcomer about once, not once per peer', async () => {
  const bus = new Bus();
  const peers = [];
  for (let i = 0; i < 6; i++) {
    peers.push(peer(bus, { answerDelay: 200 }));
  }
  await Promise.all(peers.map(synced));
  peers[0].text.insert(0, 'shared');
  await until(() => peers.every((p) => p.text.toString() === 'shared'));
  await sleep(300);
  for (const p of peers) p.pusher.sent = [];

  const n = peer(bus);
  await until(() => n.text.toString() === 'shared');
  await sleep(300);
  const answers = peers.reduce((sum, p) => sum + p.pusher.sent.length, 0);
  // Each peer sends its awareness (1 each); answers should be ~1, not 6.
  assert.ok(answers <= peers.length + 3, `${answers} events from the room`);
  for (const p of [...peers, n]) p.provider.destroy();
});

test('fromChannel works with a channel subscribed elsewhere (Laravel Echo)', async () => {
  const bus = new Bus();
  const a = peer(bus);
  a.text.insert(0, 'echo');
  await synced(a);
  const pusher = new FakePusher(bus);
  const channel = pusher.subscribe(CH);
  await until(() => channel.subscribed);
  const doc = new Y.Doc();
  const b = HibiraProvider.fromChannel(channel, doc, fast);
  await until(() => doc.getText('t').toString() === 'echo');
  b.destroy();
  assert.ok(pusher.channels.has(CH), 'fromChannel does not unsubscribe a channel it did not subscribe');
  a.provider.destroy();
});

test('destroy stops listening to the document', async () => {
  const bus = new Bus();
  const a = peer(bus);
  const b = peer(bus);
  await synced(a);
  await synced(b);
  a.provider.destroy();
  a.text.insert(0, 'after');
  await sleep(100);
  assert.equal(b.text.toString(), '');
  b.provider.destroy();
});
