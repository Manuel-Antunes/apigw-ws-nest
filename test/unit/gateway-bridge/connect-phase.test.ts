/* The connect phase — refusing, identifying and rehydrating at $connect. */

import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ConnectHook,
  ConnectionGoneError,
  ConnectionStore,
  ConnectionRejectedError,
  GLOBAL_ROOM,
  GatewayClient,
  enqueueBroadcast,
} from '../../../src';
import { connectEvent, disconnectEvent, frameEvent, nextId } from '../../helpers/events';
import { SpyPublisher, SpyStore, bridgeWith, recorder, sleep, storeFrom2x } from '../../helpers/fakes';

beforeEach(() => {
  vi.spyOn(console, 'error').mockImplementation(() => {});
  vi.spyOn(console, 'warn').mockImplementation(() => {});
});

const statusFor = async (hook: ConnectHook, connectTimeout?: number) => {
  const { bridge } = bridgeWith({ hooks: [hook], connectTimeout });
  return bridge.dispatch(connectEvent(nextId()));
};

describe('refusing a connection', () => {
  it('writes nothing — no META, no global room, none of the recorded joins', async () => {
    const { bridge, store } = bridgeWith({
      hooks: [async client => {
        await client.join('x');
        throw new ConnectionRejectedError(403, 'not you');
      }],
    });
    const id = nextId();
    expect(await bridge.dispatch(connectEvent(id))).toEqual({ statusCode: 403, body: 'not you' });
    expect(await store.get(id)).toBeNull();
    expect(await store.membersOf(GLOBAL_ROOM)).not.toContain(id);
    expect(await store.membersOf('x')).not.toContain(id);
  });

  it.each<[string, () => unknown, number]>([
    ['a ConnectionRejectedError', () => new ConnectionRejectedError(429), 429],
    ['a 5xx ConnectionRejectedError', () => new ConnectionRejectedError(503), 503],
    ['a ConnectionRejectedError with a nonsense status', () => new ConnectionRejectedError(99), 500],
    ['an HttpException (getStatus)', () => ({ getStatus: () => 401, message: 'Unauthorized' }), 401],
    ['a 5xx HttpException', () => ({ getStatus: () => 502 }), 500],
    ['a WsException with { status }', () => ({ getError: () => ({ status: 403 }), message: 'x' }), 403],
    ['a WsException with { statusCode }', () => ({ getError: () => ({ statusCode: 429 }) }), 429],
    ['any other WsException', () => ({ getError: () => 'nope', message: 'nope' }), 401],
    ['a WsException with a 5xx status', () => ({ getError: () => ({ status: 500 }) }), 401],
    ['a plain Error (fail closed)', () => new Error('db down'), 500],
    ['a thrown string', () => 'oops', 500],
  ])('maps %s to its status', async (_what, error, status) => {
    const res = await statusFor(() => {
      throw error();
    });
    expect(res.statusCode).toBe(status);
  });

  it('answers 403 for client.disconnect(), and stops the remaining hooks', async () => {
    const later = vi.fn();
    const { bridge } = bridgeWith({ hooks: [client => void client.disconnect(), later] });
    expect(await bridge.dispatch(connectEvent(nextId()))).toEqual({ statusCode: 403, body: 'connection refused' });
    expect(later).not.toHaveBeenCalled();
  });

  it('answers 503 when the hooks outlive connectTimeout, naming the step that hung', async () => {
    async function slowIdentityProvider() {
      await sleep(200);
    }
    const res = await statusFor(slowIdentityProvider, 30);
    expect(res).toEqual({ statusCode: 503, body: 'connect failed' });
    expect(console.error).toHaveBeenCalledWith('[connect: slowIdentityProvider] timed out after 30ms');
  });

  it('sends a 4xx message (truncated) but never a 5xx one', async () => {
    const long = await statusFor(() => {
      throw new ConnectionRejectedError(400, 'x'.repeat(500));
    });
    expect(long.body).toHaveLength(200);
    const withoutMessage = await statusFor(() => {
      throw { getStatus: () => 403 };
    });
    expect(withoutMessage.body).toBe('connection rejected');
    const secret = await statusFor(() => {
      throw new Error('password=hunter2');
    });
    expect(secret.body).toBe('connect failed');
  });

  it('logs a 5xx with the hook or protocol that threw, and a 4xx not at all', async () => {
    await statusFor(() => {
      throw new ConnectionRejectedError(401);
    });
    expect(console.error).not.toHaveBeenCalled();
    const anonymous = bridgeWith({ hooks: [Object.defineProperty(() => { throw new Error('x'); }, 'name', { value: '' })] });
    await anonymous.bridge.dispatch(connectEvent(nextId()));
    expect(console.error).toHaveBeenCalledWith('[connect: onConnect]', expect.any(Error));
    const viaProtocol = bridgeWith({
      protocols: [
        { name: 'phoenix', handleFrame: async () => false, onConnect: () => { throw new Error('y'); } },
        { handleFrame: async () => false, onConnect: () => {} },
      ],
    });
    await viaProtocol.bridge.dispatch(connectEvent(nextId()));
    expect(console.error).toHaveBeenCalledWith('[connect: phoenix]', expect.any(Error));
  });

  it('runs builder hooks first, then protocols in routing order, stopping at a refusal', async () => {
    const order: string[] = [];
    const { bridge } = bridgeWith({
      protocols: [
        { name: 'nest', fallback: true, handleFrame: async () => false, onConnect: () => void order.push('nest') },
        { name: 'phoenix', handleFrame: async () => false, onConnect: client => {
          order.push('phoenix');
          void client.disconnect();
        } },
        { name: 'unreached', handleFrame: async () => false, onConnect: () => void order.push('unreached') },
      ],
      hooks: [() => void order.push('hook-1'), () => void order.push('hook-2')],
    });
    await bridge.dispatch(connectEvent(nextId()));
    expect(order).toEqual(['hook-1', 'hook-2', 'phoenix']);
  });
});

describe('the handshake hooks see', () => {
  it('carries headers, query, every offered subprotocol and the negotiated one', async () => {
    let seen: any;
    const { bridge } = bridgeWith({
      protocols: [{ subprotocol: 'graphql-transport-ws', handleFrame: async () => false }],
      hooks: [client => void (seen = structuredClone(client.handshake))],
    });
    const res = await bridge.dispatch(
      connectEvent(nextId(), {
        headers: { Authorization: 'Bearer abc', 'Sec-WebSocket-Protocol': 'graphql-transport-ws, bearer.abc' },
        queryStringParameters: { token: 't1' },
      }),
    );
    expect(seen).toMatchObject({
      headers: { authorization: 'Bearer abc' },
      query: { token: 't1' },
      subprotocols: ['graphql-transport-ws', 'bearer.abc'],
      subprotocol: 'graphql-transport-ws',
      sourceIp: '203.0.113.7',
    });
    expect(res.headers).toEqual({ 'Sec-WebSocket-Protocol': 'graphql-transport-ws' });
  });
});

describe('accepting a connection', () => {
  const when = new Date('2026-01-02T03:04:05.000Z');
  const identify: ConnectHook = client => {
    client.data.user = { id: 'u1', roles: ['admin'] };
    client.data.since = when;
    client.data.dropped = undefined;
  };

  it('persists client.data JSON-normalised, with sourceIp and connectedAt', async () => {
    const { bridge, store } = bridgeWith({ hooks: [identify] });
    const id = nextId();
    expect(await bridge.dispatch(connectEvent(id))).toEqual({ statusCode: 200 });
    expect(await store.get(id)).toEqual({
      connectedAt: 1_700_000_000_000,
      sourceIp: '203.0.113.7',
      data: { user: { id: 'u1', roles: ['admin'] }, since: when.toISOString() },
    });
  });

  it('leaves data out of META when nothing was set', async () => {
    const { bridge, store } = bridgeWith({ hooks: [() => {}] });
    const id = nextId();
    await bridge.dispatch(connectEvent(id, { requestContext: { identity: {} } }));
    expect(await store.get(id)).toEqual({ connectedAt: 1_700_000_000_000 });
  });

  it('makes the connecting instance\'s client look like every other instance\'s', async () => {
    let connecting!: GatewayClient;
    const { bridge } = bridgeWith({
      hooks: [client => {
        connecting = client;
        identify(client, null as any);
      }],
    });
    await bridge.dispatch(
      connectEvent(nextId(), { headers: { authorization: 'Bearer secret' }, queryStringParameters: { t: '1' } }),
    );
    expect(connecting.phase).toBe('open');
    expect(connecting.data.since).toBe(when.toISOString());
    expect(connecting.handshake).toEqual({
      headers: {}, query: {}, subprotocols: [], connectedAt: 1_700_000_000_000, sourceIp: '203.0.113.7',
    });
    expect(() => (connecting.data.user.id = 'u2')).toThrow(TypeError);
    expect(() => ((connecting as any).data = {})).toThrow(TypeError);
  });

  it('keeps the authorizer context, which API Gateway repeats on every route', async () => {
    let connecting!: GatewayClient;
    const { bridge } = bridgeWith({ hooks: [client => void (connecting = client)] });
    await bridge.dispatch(connectEvent(nextId(), { requestContext: { authorizer: { principalId: 'p' } } }));
    expect(connecting.handshake.authorizer).toEqual({ principalId: 'p' });
  });

  it('applies the recorded memberships in order, after META', async () => {
    const { bridge, store } = bridgeWith({
      hooks: [async client => {
        await client.join('user:42');
        await client.join('temp');
        await client.leave('temp');
      }],
    });
    const id = nextId();
    await bridge.dispatch(connectEvent(id));
    expect(await store.membersOf('user:42')).toContain(id);
    expect(await store.membersOf('temp')).not.toContain(id);
  });

  it('keeps a subprotocol socket out of the global room, as without hooks', async () => {
    const { bridge, store } = bridgeWith({
      protocols: [{ subprotocol: 'graphql-transport-ws', handleFrame: async () => false }],
      hooks: [() => {}],
    });
    const id = nextId();
    await bridge.dispatch(connectEvent(id, { headers: { 'sec-websocket-protocol': 'graphql-transport-ws' } }));
    expect(await store.membersOf(GLOBAL_ROOM)).not.toContain(id);
    expect((await store.get(id))!.subprotocol).toBe('graphql-transport-ws');
  });

  it.each<[string, ConnectHook]>([
    ['over maxConnectionData', client => void (client.data.blob = 'x'.repeat(100))],
    ['not a plain object', client => void ((client as any).data = ['a'])],
    ['not serialisable', client => void (client.data.n = 1n)],
  ])('answers 500 and stores nothing when data is %s', async (_what, hook) => {
    const { bridge, store } = bridgeWith({ hooks: [hook], maxConnectionData: 64 });
    const id = nextId();
    expect(await bridge.dispatch(connectEvent(id))).toEqual({ statusCode: 500, body: 'connect failed' });
    expect(await store.get(id)).toBeNull();
    expect(console.error).toHaveBeenCalledWith('[connect: accept]', expect.any(Error));
  });

  it('treats undefined data as empty', async () => {
    const { bridge, store } = bridgeWith({ hooks: [client => void ((client as any).data = undefined)] });
    const id = nextId();
    expect((await bridge.dispatch(connectEvent(id))).statusCode).toBe(200);
    expect((await store.get(id))!.data).toBeUndefined();
  });

  it('removes a half-recorded connection when a write fails', async () => {
    const store = new SpyStore();
    vi.spyOn(store, 'join').mockRejectedValue(new Error('throttled'));
    const { bridge } = bridgeWith({ store, hooks: [() => {}] });
    const id = nextId();
    expect((await bridge.dispatch(connectEvent(id))).statusCode).toBe(500);
    expect(await store.get(id)).toBeNull();
  });

  it('answers 500 even when the cleanup of a failed accept fails too', async () => {
    const store = new SpyStore();
    vi.spyOn(store, 'add').mockRejectedValue(new Error('down'));
    vi.spyOn(store, 'remove').mockRejectedValue(new Error('still down'));
    const { bridge } = bridgeWith({ store, hooks: [() => {}] });
    expect((await bridge.dispatch(connectEvent(nextId()))).statusCode).toBe(500);
  });

  it('runs the hooks in a dispatch scope of their own: an enqueued broadcast is awaited', async () => {
    const published: unknown[] = [];
    const { bridge } = bridgeWith({
      // Not awaited by the hook — the scope is what holds the dispatch open.
      hooks: [client => enqueueBroadcast(bridge.server.emit('joined', client.connectionId))],
    });
    (bridge as any).bus.publish = async (m: unknown) => {
      await sleep(10);
      published.push(m);
    };
    await bridge.dispatch(connectEvent('hello'));
    expect(published).toEqual([expect.objectContaining({ kind: 'room', event: 'joined', data: 'hello' })]);
  });
});

describe('frames after $connect', () => {
  async function accepted() {
    const store = new SpyStore();
    const rec = recorder();
    const { bridge } = bridgeWith({
      store, protocols: [rec.protocol], hooks: [client => void (client.data.user = 'ada')],
    });
    const id = nextId();
    await bridge.dispatch(connectEvent(id));
    return { store, rec, bridge, id };
  }

  it('need no store read on the instance that accepted', async () => {
    const { store, rec, bridge, id } = await accepted();
    const reads = store.gets;
    const announced = vi.fn();
    bridge.server.on('connection', announced);
    await bridge.dispatch(frameEvent(id, {}));
    await bridge.dispatch(frameEvent(id, {}));
    expect(store.gets).toBe(reads);
    expect(announced).toHaveBeenCalledOnce(); // Nest hears about it on the first frame
    expect(rec.seen[0].client.data).toEqual({ user: 'ada' });
  });

  it('rehydrate client.data on a fresh instance, frozen, with a reduced handshake', async () => {
    const { store, id } = await accepted();
    const rec = recorder();
    const { bridge: other } = bridgeWith({ store, protocols: [rec.protocol], hooks: [() => {}] });
    await other.dispatch(frameEvent(id, {}, { requestContext: { authorizer: { p: 1 } } }));
    const client = rec.seen[0].client;
    expect(client.rehydrated).toBe(true);
    expect(client.data).toEqual({ user: 'ada' });
    expect(client.handshake).toEqual({
      headers: {}, query: {}, subprotocols: [], connectedAt: 1_700_000_000_000,
      sourceIp: '203.0.113.7', authorizer: { p: 1 },
    });
    expect(() => (client.data.user = 'eve')).toThrow(TypeError);
  });

  it('cost one read and one announcement for concurrent frames on a fresh instance', async () => {
    const { store, id } = await accepted();
    const rec = recorder();
    const { bridge: fresh } = bridgeWith({ store, protocols: [rec.protocol], hooks: [() => {}] });
    const announced = vi.fn();
    fresh.server.on('connection', announced);
    const reads = store.gets;
    await Promise.all([fresh.dispatch(frameEvent(id, { n: 1 })), fresh.dispatch(frameEvent(id, { n: 2 }))]);
    expect(store.gets - reads).toBe(1);
    expect(announced).toHaveBeenCalledOnce();
    expect(rec.seen[0].client).toBe(rec.seen[1].client);
  });

  it('prefer a client accepted on this instance while the read was in flight', async () => {
    const store = new SpyStore();
    let release!: () => void;
    const gate = new Promise<void>(resolve => (release = resolve));
    const get = store.get.bind(store);
    vi.spyOn(store, 'get').mockImplementation(async id => {
      await gate;
      return get(id);
    });
    const rec = recorder();
    const { bridge } = bridgeWith({ store, protocols: [rec.protocol], hooks: [() => {}] });
    const frame = bridge.dispatch(frameEvent('racy', {}));
    await bridge.dispatch(connectEvent('racy'));
    release();
    await frame;
    expect(rec.seen[0].client.rehydrated).toBe(false);
  });

  it('from a connection nobody accepted are refused and closed', async () => {
    const rec = recorder();
    const { bridge, publisher } = bridgeWith({ protocols: [rec.protocol], hooks: [() => {}] });
    expect(await bridge.dispatch(frameEvent('never-connected', {}))).toEqual({
      statusCode: 403, body: 'unknown connection',
    });
    expect(rec.seen).toHaveLength(0);
    expect((publisher as SpyPublisher).disconnected).toEqual(['never-connected']);
    expect(console.warn).toHaveBeenCalledWith(expect.stringContaining('never-connected'));
  });

  it('from an unknown connection are refused even when closing it fails, or is impossible', async () => {
    const failing = new SpyPublisher();
    failing.disconnect = async () => { throw new Error('denied'); };
    const { bridge } = bridgeWith({ publisher: failing, hooks: [() => {}] });
    expect((await bridge.dispatch(frameEvent('x', {}))).statusCode).toBe(403);
    expect(console.error).toHaveBeenCalledWith('[disconnect]', expect.any(Error));

    const { bridge: noClose } = bridgeWith({ publisher: { toConnection: async () => {} }, hooks: [() => {}] });
    expect((await noClose.dispatch(frameEvent('x', {}))).statusCode).toBe(403);
  });
});

describe('disconnecting', () => {
  it('shows client.data to onDisconnect on an instance that never saw the connection', async () => {
    const store = new SpyStore();
    const { bridge } = bridgeWith({ store, hooks: [client => void (client.data.user = 'ada')] });
    const id = nextId();
    await bridge.dispatch(connectEvent(id));
    const rec = recorder();
    const { bridge: other } = bridgeWith({ store, protocols: [rec.protocol], hooks: [() => {}] });
    await other.dispatch(disconnectEvent(id, 'client went away'));
    expect(rec.disconnects[0].client!.data).toEqual({ user: 'ada' });
    expect(rec.disconnects[0].reason).toBe('client went away');
    expect(await store.get(id)).toBeNull();

    const again = recorder();
    const { bridge: third } = bridgeWith({ store, protocols: [again.protocol], hooks: [() => {}] });
    await third.dispatch(disconnectEvent(id));
    expect(again.disconnects[0].client).toBeUndefined();
  });

  it('still cleans up when the connection cannot be read back', async () => {
    const store = new SpyStore();
    vi.spyOn(store, 'get').mockRejectedValue(new Error('read failed'));
    const rec = recorder();
    const { bridge } = bridgeWith({ store, protocols: [rec.protocol], hooks: [() => {}] });
    const remove = vi.spyOn(store, 'remove');
    await bridge.dispatch(disconnectEvent('x'));
    expect(rec.disconnects[0].client).toBeUndefined();
    expect(remove).toHaveBeenCalledWith('x');
    expect(console.error).toHaveBeenCalledWith('[disconnect: rehydrate]', expect.any(Error));
  });

  it('reaps a 410 with reason \'gone\', client.data still readable', async () => {
    const rec = recorder();
    const store = new SpyStore();
    const { bridge } = bridgeWith({
      store,
      protocols: [rec.protocol, {
        handleFrame: async () => false,
        fanout: {
          probe: async record => ({
            targets: [{ connectionId: String((record as any).target), key: 'k', send: async () => { throw new ConnectionGoneError('x'); } }],
          }),
        },
      }],
      hooks: [client => void (client.data.user = 'grace')],
    });
    const id = nextId();
    await bridge.dispatch(connectEvent(id));
    await bridge.flushRecord({ kind: 'probe', target: id, messageId: 'm', publishedAt: 1 } as any);
    expect(rec.disconnects[0]).toMatchObject({ id, reason: 'gone' });
    expect(rec.disconnects[0].client!.data).toEqual({ user: 'grace' });
    expect(await store.get(id)).toBeNull();
  });

  it('kicks from a rehydrated client too — the usual case, on a cold instance', async () => {
    const store = new SpyStore();
    const { bridge } = bridgeWith({ store, hooks: [client => void (client.data.user = 'cold')] });
    const id = nextId();
    await bridge.dispatch(connectEvent(id));
    const rec = recorder();
    const publisher = new SpyPublisher();
    const { bridge: cold } = bridgeWith({ store, publisher, protocols: [rec.protocol], hooks: [() => {}] });
    await cold.dispatch(frameEvent(id, {}));
    await rec.seen[0].client.disconnect();
    expect(rec.disconnects[0]).toMatchObject({ id, reason: 'server disconnect' });
    expect(rec.disconnects[0].client!.data).toEqual({ user: 'cold' });
    expect(publisher.disconnected).toEqual([id]);
  });

  it('kicks an open connection: cleanup with \'server disconnect\', then the socket closes', async () => {
    const rec = recorder();
    const { bridge, store, publisher } = bridgeWith({
      protocols: [rec.protocol], hooks: [client => void (client.data.user = 'kick-me')],
    });
    const id = nextId();
    await bridge.dispatch(connectEvent(id));
    await bridge.dispatch(frameEvent(id, {}));
    await rec.seen[0].client.disconnect();
    expect(rec.disconnects[0]).toMatchObject({ id, reason: 'server disconnect' });
    expect(rec.disconnects[0].client!.data).toEqual({ user: 'kick-me' });
    expect((publisher as SpyPublisher).disconnected).toEqual([id]);
    expect(await store.get(id)).toBeNull();
  });
});

describe('registration', () => {
  it('requires ConnectionStore.get() in the type', () => {
    const store: ConnectionStore = {
      add: async () => {},
      remove: async () => {},
      join: async () => {},
      leave: async () => {},
      membersOf: async () => [],
      get: async () => null,
    };
    const { get: _get, ...withoutGet } = store;
    // @ts-expect-error — a 2.x store, without get(), no longer compiles
    const from2x: ConnectionStore = withoutGet;
    expect(from2x.get).toBeUndefined();
  });

  it('refuses, at boot, a 2.x store that reaches a connect hook anyway', () => {
    const builder = () => GatewayBridgeBuilderFor(storeFrom2x());
    expect(() => builder().onConnect(() => {}).build()).toThrow(/onConnect\(\): connect hooks need ConnectionStore\.get\(\)/);
    expect(() =>
      builder().use({ name: 'p', handleFrame: async () => false, onConnect: () => {} }).build(),
    ).toThrow(/bridge\.use\(p\)/);
    expect(() => builder().use({ handleFrame: async () => false, onConnect: () => {} }).build()).toThrow(
      /bridge\.use\(protocol\).*required since apigw-ws-nest 3\.0/,
    );
    expect(() => builder().build()).not.toThrow();
  });

  it('reports whether a connect phase runs', () => {
    expect(bridgeWith().bridge.hasConnectHooks).toBe(false);
    expect(bridgeWith({ hooks: [() => {}] }).bridge.hasConnectHooks).toBe(true);
    expect(
      bridgeWith({ protocols: [{ handleFrame: async () => false, onConnect: () => {} }] }).bridge.hasConnectHooks,
    ).toBe(true);
  });

  it('accepts a hook after build()', async () => {
    const { bridge } = bridgeWith();
    bridge.onConnect(() => {
      throw new ConnectionRejectedError(418);
    });
    expect((await bridge.dispatch(connectEvent(nextId()))).statusCode).toBe(418);
  });

  it('refuses to rehydrate through a store that lost get() after registration', async () => {
    const store: any = new SpyStore();
    const { bridge } = bridgeWith({ store, hooks: [() => {}] });
    store.get = undefined;
    expect(await bridge.dispatch(frameEvent('x', {}))).toEqual({ statusCode: 500, body: 'dispatch failed' });
  });
});

import { GatewayBridge } from '../../../src';
function GatewayBridgeBuilderFor(store: ConnectionStore) {
  return GatewayBridge.builder().provider('local').store(store);
}
