// End-to-end test against a running Hibira (or any Pusher-compatible) server
// with client events enabled. Skipped unless YJS_E2E_KEY is set:
//
//   YJS_E2E_HOST=127.0.0.1 YJS_E2E_PORT=6001 YJS_E2E_TLS=0 \
//   YJS_E2E_APP_ID=yjs YJS_E2E_KEY=... YJS_E2E_SECRET=... npm run test:e2e
import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as Y from 'yjs';
import PusherClient from 'pusher-js';
import PusherServer from 'pusher';
import { HibiraProvider } from '../../index.js';

const env = process.env;
const skip = !env.YJS_E2E_KEY && 'set YJS_E2E_KEY to run';
const host = env.YJS_E2E_HOST ?? '127.0.0.1';
const port = Number(env.YJS_E2E_PORT ?? 6001);
const tls = env.YJS_E2E_TLS === '1';

const server = skip
  ? null
  : new PusherServer({ appId: env.YJS_E2E_APP_ID, key: env.YJS_E2E_KEY, secret: env.YJS_E2E_SECRET, host, port: String(port), useTLS: tls });

function client(userId) {
  return new PusherClient(env.YJS_E2E_KEY, {
    cluster: 'mt1',
    wsHost: host,
    wsPort: port,
    wssPort: port,
    forceTLS: tls,
    enabledTransports: ['ws'],
    channelAuthorization: {
      customHandler: ({ socketId, channelName }, cb) => {
        try {
          const auth = channelName.startsWith('presence-')
            ? server.authorizeChannel(socketId, channelName, { user_id: userId, user_info: { name: userId } })
            : server.authorizeChannel(socketId, channelName);
          cb(null, auth);
        } catch (err) {
          cb(err, null);
        }
      },
    },
  });
}

async function until(pred, ms = 10000) {
  const end = Date.now() + ms;
  while (!pred()) {
    if (Date.now() > end) throw new Error('timeout');
    await new Promise((r) => setTimeout(r, 20));
  }
}

test('documents and awareness sync on a real server', { skip, timeout: 60000 }, async () => {
  const channel = `presence-yjs-e2e${Date.now()}`;
  const peers = [];
  const errors = [];
  const join = (user) => {
    const pusher = client(user);
    pusher.connection.bind('error', (e) => errors.push(e));
    const doc = new Y.Doc();
    const provider = new HibiraProvider(pusher, channel, doc);
    const p = { user, pusher, doc, provider, text: doc.getText('t') };
    peers.push(p);
    return p;
  };
  try {
    const a = join('alice');
    await until(() => a.provider.synced);
    // ~12 KB of text: more than one 4 KB client event, so it is chunked.
    const big = Array.from({ length: 600 }, (_, i) => `line ${i}`).join('\n');
    a.text.insert(0, big);

    const b = join('bob');
    await until(() => b.provider.synced && b.text.toString() === big);

    b.text.insert(0, 'B:');
    await until(() => a.text.toString().startsWith('B:line 0'));

    a.provider.awareness.setLocalStateField('user', { name: 'Alice' });
    await until(() => b.provider.awareness.getStates().get(a.doc.clientID)?.user?.name === 'Alice');

    // Bob goes offline, both edit, Bob comes back.
    b.pusher.disconnect();
    await until(() => !b.provider.synced);
    a.text.delete(0, 2);
    b.text.insert(b.text.length, '\nend');
    b.pusher.connect();
    await until(() => b.provider.synced);
    const expected = big + '\nend';
    await until(() => a.text.toString() === expected && b.text.toString() === expected);

    // A third peer joins a room with two peers and gets everything.
    const c = join('carol');
    await until(() => c.text.toString() === expected);

    // Alice leaves; the others drop her awareness.
    a.provider.destroy();
    await until(() => !c.provider.awareness.getStates().has(a.doc.clientID));
    assert.deepEqual(errors, []);
  } finally {
    for (const p of peers) {
      p.provider.destroy();
      p.pusher.disconnect();
    }
  }
});
