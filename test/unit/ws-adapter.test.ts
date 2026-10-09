/* The adapter, without booting Nest: a fake app and a fake HTTP adapter. The
 * lifecycle takeover needs Nest's container and lives in the integration tests. */

import { Readable } from 'node:stream';
import { BehaviorSubject, Subject, throwError } from 'rxjs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ApiGatewayWsAdapter,
  BoundHandler,
  ConnectionGoneError,
  GatewayClient,
  GatewayLifecycle,
  NestGatewayProtocol,
  runInDispatchScope,
} from '../../src';
import { connectEvent, nextId } from '../helpers/events';
import { SpyPublisher, bridgeWith, minimalStore, recorder, sleep, storeFrom2x } from '../helpers/fakes';

type Route = (req: any, res: any) => Promise<void>;

/** Just enough of INestApplication + HttpServer for the adapter. */
function fakeApp() {
  const routes = new Map<string, Route>();
  const replies: Array<{ res: any; body: unknown; status?: number }> = [];
  const headers: Array<[string, string]> = [];
  const http = {
    post: vi.fn((path: string, handler: Route) => void routes.set(path, handler)),
    reply: vi.fn((res: any, body: unknown, status?: number) => void replies.push({ res, body, status })),
    setHeader: vi.fn((_res: any, name: string, value: string) => void headers.push([name, value])),
  };
  const app = { getHttpAdapter: () => http } as any;
  return { app, http, routes, replies, headers };
}

const jsonRequest = (body: unknown, headers: Record<string, string> = {}) =>
  Object.assign(Readable.from([JSON.stringify(body)]), { headers });

beforeEach(() => {
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  vi.spyOn(console, 'error').mockImplementation(() => {});
});
afterEach(() => vi.unstubAllEnvs());

describe('NestGatewayProtocol', () => {
  it('is the fallback, and leaves a frame alone when no gateway bound handlers', async () => {
    const protocol = new NestGatewayProtocol();
    expect(protocol.fallback).toBe(true);
    const client = new GatewayClient('a', minimalStore(), new SpyPublisher());
    expect(await protocol.handleFrame({ event: 'x' }, client)).toBe(false);
    client.handleFrame = vi.fn(async () => {});
    expect(await protocol.handleFrame({ event: 'x', data: 1 }, client)).toBe(true);
    expect(client.handleFrame).toHaveBeenCalledWith({ event: 'x', data: 1 });
  });

  it('has no lifecycle hooks unless given gateways', () => {
    const legacy = new NestGatewayProtocol();
    expect(legacy.onConnect).toBeUndefined();
    expect(legacy.onDisconnect).toBeUndefined();
  });

  it('awaits every gateway\'s handleConnection in order, stopping at a refusal or a throw', async () => {
    const order: string[] = [];
    const gateways: GatewayLifecycle[] = [
      { name: 'NoHooks' },
      { name: 'A', connect: async () => void order.push('A') },
      { name: 'B', connect: client => { order.push('B'); return client.disconnect(); } },
      { name: 'C', connect: () => void order.push('C') },
    ];
    const protocol = new NestGatewayProtocol(gateways);
    const client = new GatewayClient('a', minimalStore(), new SpyPublisher(), { phase: 'connecting' });
    await protocol.onConnect!(client);
    expect(order).toEqual(['A', 'B']);

    const throwing = new NestGatewayProtocol([
      { name: 'X', connect: () => { throw new Error('no'); } },
      { name: 'Y', connect: () => void order.push('Y') },
    ]);
    await expect(throwing.onConnect!(new GatewayClient('b', minimalStore(), new SpyPublisher()))).rejects.toThrow('no');
    expect(order).not.toContain('Y');
  });

  it('calls every handleDisconnect, logging failures — a disconnect cannot be refused', async () => {
    const calls: string[] = [];
    const protocol = new NestGatewayProtocol([
      { name: 'Broken', disconnect: () => { throw new Error('x'); } },
      { name: 'NoHook' },
      { name: 'Ok', disconnect: (_client, reason) => void calls.push(reason) },
    ]);
    const client = new GatewayClient('a', minimalStore(), new SpyPublisher());
    await protocol.onDisconnect!('a', client, 'bye');
    await protocol.onDisconnect!('a', client);
    await protocol.onDisconnect!('a', undefined, 'never accepted');
    expect(calls).toEqual(['bye', 'disconnect']);
    expect(console.error).toHaveBeenCalledWith('[disconnect: Broken]', expect.any(Error));
  });
});

describe('ApiGatewayWsAdapter — the dispatch route', () => {
  it('registers no route by default, and the Nest protocol with its connect hook', () => {
    const { app, http } = fakeApp();
    const { bridge } = bridgeWith();
    new ApiGatewayWsAdapter(app, bridge);
    expect(http.post).not.toHaveBeenCalled();
    expect(bridge.hasConnectHooks).toBe(true); // lifecycle 'connect'
  });

  it('registers no connect hook under lifecycle \'legacy\'', () => {
    const { bridge } = bridgeWith();
    new ApiGatewayWsAdapter(fakeApp().app, bridge, { lifecycle: 'legacy' });
    expect(bridge.hasConnectHooks).toBe(false);
  });

  it('registers the route on the path asked for, or none at all', () => {
    const custom = fakeApp();
    new ApiGatewayWsAdapter(custom.app, bridgeWith().bridge, { dispatchPath: '/ws-events' });
    expect([...custom.routes.keys()]).toEqual(['/ws-events']);
    const none = fakeApp();
    new ApiGatewayWsAdapter(none.app, bridgeWith().bridge, { dispatchPath: false });
    expect(none.http.post).not.toHaveBeenCalled();
  });

  it('hands the parsed event to the bridge and forwards status, body and headers', async () => {
    const { app, routes, replies, headers } = fakeApp();
    const { bridge, store } = bridgeWith({
      protocols: [{ subprotocol: 'graphql-transport-ws', handleFrame: async () => false }],
    });
    new ApiGatewayWsAdapter(app, bridge, { dispatchPath: '/@dispatch' });
    const id = nextId();
    const res = {};
    await routes.get('/@dispatch')!(
      jsonRequest(connectEvent(id, { headers: { 'sec-websocket-protocol': 'graphql-transport-ws' } })),
      res,
    );
    expect(replies).toEqual([{ res, body: '', status: 200 }]);
    expect(headers).toEqual([['Sec-WebSocket-Protocol', 'graphql-transport-ws']]);
    expect(await store.get(id)).not.toBeNull();
  });

  it('uses an already-parsed body, and answers with the bridge\'s body', async () => {
    const { app, routes, replies } = fakeApp();
    const { bridge } = bridgeWith();
    vi.spyOn(bridge, 'dispatch').mockResolvedValue({ statusCode: 403, body: 'unknown connection' });
    new ApiGatewayWsAdapter(app, bridge, { dispatchPath: '/@dispatch' });
    await routes.get('/@dispatch')!({ body: connectEvent('a'), headers: {} }, {});
    expect(bridge.dispatch).toHaveBeenCalledWith(connectEvent('a'));
    expect(replies[0]).toMatchObject({ body: 'unknown connection', status: 403 });
  });

  it('answers 400 to a body that is not JSON, or a request that errors', async () => {
    const { app, routes, replies } = fakeApp();
    new ApiGatewayWsAdapter(app, bridgeWith().bridge, { dispatchPath: '/@dispatch' });
    await routes.get('/@dispatch')!(Object.assign(Readable.from(['{nope']), { headers: {} }), {});
    const broken = new Readable({ read() { this.destroy(new Error('socket hang up')); } });
    await routes.get('/@dispatch')!(Object.assign(broken, { headers: {} }), {});
    expect(replies.map(r => r.status)).toEqual([400, 400]);
    expect(replies[0].body).toBe('invalid JSON body');
  });

  it('treats an empty body as an empty event', async () => {
    const { app, routes } = fakeApp();
    const { bridge } = bridgeWith();
    const dispatch = vi.spyOn(bridge, 'dispatch').mockResolvedValue({ statusCode: 200 });
    new ApiGatewayWsAdapter(app, bridge, { dispatchPath: '/@dispatch' });
    await routes.get('/@dispatch')!(Object.assign(Readable.from([]), { headers: {} }), {});
    expect(dispatch).toHaveBeenCalledWith({});
  });

  describe('with a dispatch secret', () => {
    async function call(options: object, requestHeaders: Record<string, string>) {
      const { app, routes, replies } = fakeApp();
      const { bridge } = bridgeWith();
      const dispatch = vi.spyOn(bridge, 'dispatch').mockResolvedValue({ statusCode: 200 });
      new ApiGatewayWsAdapter(app, bridge, { dispatchPath: '/@dispatch', ...options });
      await routes.get('/@dispatch')!(jsonRequest(connectEvent('a'), requestHeaders), {});
      return { status: replies[0].status, dispatched: dispatch.mock.calls.length > 0 };
    }

    it('refuses a missing or wrong secret with 403, before the bridge sees anything', async () => {
      expect(await call({ dispatchSecret: 's' }, {})).toEqual({ status: 403, dispatched: false });
      expect(await call({ dispatchSecret: 's' }, { 'x-apigw-dispatch-secret': 'nope' })).toEqual({
        status: 403, dispatched: false,
      });
    });

    it('lets the right secret through', async () => {
      expect(await call({ dispatchSecret: 's' }, { 'x-apigw-dispatch-secret': 's' })).toEqual({
        status: 200, dispatched: true,
      });
    });

    it('reads the header the options name, case-insensitively', async () => {
      expect(
        await call({ dispatchSecret: 's', dispatchSecretHeader: 'X-Custom-Secret' }, { 'x-custom-secret': 's' }),
      ).toEqual({ status: 200, dispatched: true });
    });

    it('defaults the secret from APIGW_DISPATCH_SECRET', async () => {
      vi.resetModules();
      vi.stubEnv('APIGW_DISPATCH_SECRET', 'from-env');
      const fresh = await import('../../src');
      const { app, routes, replies } = fakeApp();
      const bridge = fresh.GatewayBridge.builder().provider('local').build();
      vi.spyOn(bridge, 'dispatch').mockResolvedValue({ statusCode: 200 });
      new fresh.ApiGatewayWsAdapter(app, bridge, { dispatchPath: fresh.DISPATCH_PATH });
      await routes.get('/@dispatch')!(jsonRequest({}, {}), {});
      await routes.get('/@dispatch')!(jsonRequest({}, { 'x-apigw-dispatch-secret': 'from-env' }), {});
      expect(replies.map(r => r.status)).toEqual([403, 200]);
    });
  });

  it('warns once per process about an open route in front of connect hooks', async () => {
    vi.resetModules();
    const fresh = await import('../../src');
    const bridge = () => fresh.GatewayBridge.builder().provider('local').build();
    const route = { dispatchPath: '/@dispatch' };
    new fresh.ApiGatewayWsAdapter(fakeApp().app, bridge());
    expect(console.warn).not.toHaveBeenCalled(); // no route: nothing to forge into
    new fresh.ApiGatewayWsAdapter(fakeApp().app, bridge(), { ...route, lifecycle: 'legacy' });
    expect(console.warn).not.toHaveBeenCalled(); // no hooks: no identity to steal
    new fresh.ApiGatewayWsAdapter(fakeApp().app, bridge(), { ...route, dispatchSecret: 's' });
    expect(console.warn).not.toHaveBeenCalled(); // protected
    new fresh.ApiGatewayWsAdapter(fakeApp().app, bridge(), route);
    new fresh.ApiGatewayWsAdapter(fakeApp().app, bridge(), route);
    expect(console.warn).toHaveBeenCalledOnce();
    expect(vi.mocked(console.warn).mock.calls[0][0]).toMatch(/without a dispatchSecret/);
  });

  it('refuses, at boot, a store that cannot rehydrate client.data', () => {
    const noGet = bridgeWith({ store: storeFrom2x() }).bridge;
    expect(() => new ApiGatewayWsAdapter(fakeApp().app, noGet)).toThrow(
      /bridge\.use\(nest\).*ConnectionStore\.get\(\)/,
    );
    expect(() => new ApiGatewayWsAdapter(fakeApp().app, noGet, { lifecycle: 'legacy' })).not.toThrow();
  });
});

describe('ApiGatewayWsAdapter — the WebSocketAdapter contract', () => {
  function setup() {
    const publisher = new SpyPublisher();
    const { bridge } = bridgeWith({ publisher });
    // 'legacy': create() under 'connect' needs Nest's container (integration tests).
    const adapter = new ApiGatewayWsAdapter(fakeApp().app, bridge, { lifecycle: 'legacy' });
    const client = new GatewayClient('c1', bridge.store, publisher);
    return { adapter, bridge, publisher, client };
  }
  const handler = (
    message: string,
    callback: (data: any, ack: (r: any) => void) => unknown,
    isAckHandledManually = false,
  ): BoundHandler => ({ message, methodName: message, callback: callback as any, isAckHandledManually });

  it('hands Nest the bridge\'s server as the socket server and the connection hub', () => {
    const { adapter, bridge } = setup();
    expect(adapter.create(0)).toBe(bridge.server);
    const callback = vi.fn();
    adapter.bindClientConnect(bridge.server, callback);
    bridge.server.emit('connection', 'client');
    expect(callback).toHaveBeenCalledWith('client');
    adapter.close();
    adapter.dispose();
  });

  it('merges routes across gateways and installs one frame processor', async () => {
    const { adapter, client, publisher } = setup();
    adapter.bindMessageHandlers(client, [handler('a', async () => ({ event: 'a', data: 1 }))], vi.fn());
    const processor = client.handleFrame;
    adapter.bindMessageHandlers(client, [handler('b', async () => ({ event: 'b', data: 2 }))], vi.fn());
    expect(client.handleFrame).toBe(processor);
    await client.handleFrame!({ event: 'a', data: null });
    await client.handleFrame!({ event: 'b', data: null });
    await client.handleFrame!({ event: 'unknown', data: null });
    expect(publisher.sent.map(f => f.event)).toEqual(['a', 'b']);
  });

  it('sends a plain return, but not a null one, nor one the handler already acked', async () => {
    const { adapter, client, publisher } = setup();
    adapter.bindMessageHandlers(
      client,
      [
        handler('nothing', async () => null),
        handler('acked', async (_data, ack) => {
          ack({ event: 'ack', data: 'early' });
          return { event: 'late', data: 'ignored' };
        }, true),
      ],
      vi.fn(),
    );
    await runInDispatchScope(async () => {
      await client.handleFrame!({ event: 'nothing', data: null });
      await client.handleFrame!({ event: 'acked', data: null });
    });
    expect(publisher.sent).toEqual([{ id: 'c1', event: 'ack', data: 'early' }]);
  });

  it('keeps an Observable return alive, delivering each non-null emission', async () => {
    const { adapter, client, publisher } = setup();
    const feed = new BehaviorSubject<any>({ event: 'feed', data: 0 });
    adapter.bindMessageHandlers(client, [handler('watch', async () => feed)], vi.fn());
    await runInDispatchScope(() => client.handleFrame!({ event: 'watch', data: null }));
    await runInDispatchScope(async () => {
      feed.next(null);
      feed.next({ event: 'feed', data: 1 });
    });
    expect(publisher.sent.map(f => f.data)).toEqual([0, 1]);
    expect(client.subscriptions).toHaveLength(1);
  });

  it('routes an Observable error and a failed send to onSendError', async () => {
    const { adapter, bridge, client } = setup();
    const onSendError = vi.spyOn(bridge, 'onSendError').mockResolvedValue();
    const failing = new Subject<any>();
    adapter.bindMessageHandlers(
      client,
      [
        handler('broken-stream', async () => throwError(() => new Error('stream died'))),
        handler('feed', async () => failing),
        handler('reply', async () => ({ event: 'r', data: 1 })),
        handler('acks', async (_data, ack) => ack({ event: 'a', data: 1 }), true),
      ],
      vi.fn(),
    );
    const gone = new ConnectionGoneError('c1');
    vi.spyOn(client, 'send').mockRejectedValue(gone);
    await runInDispatchScope(async () => {
      await client.handleFrame!({ event: 'broken-stream', data: null });
      await client.handleFrame!({ event: 'feed', data: null });
      failing.next({ event: 'x', data: 1 });
      await client.handleFrame!({ event: 'reply', data: null });
      await client.handleFrame!({ event: 'acks', data: null });
    });
    await sleep(1);
    expect(onSendError.mock.calls.map(([, err]) => (err as Error).message)).toEqual([
      'stream died', gone.message, gone.message, gone.message,
    ]);
  });
});

describe('ApiGatewayWsAdapter — a bridge with a recorder', () => {
  it('passes through frames nobody bound when there is no gateway', async () => {
    const rec = recorder();
    const { bridge } = bridgeWith({ protocols: [rec.protocol] });
    new ApiGatewayWsAdapter(fakeApp().app, bridge);
    await bridge.dispatch(connectEvent('a'));
    await bridge.dispatch({
      requestContext: { connectionId: 'a', eventType: 'MESSAGE', routeKey: '$default' },
      body: JSON.stringify({ event: 'x' }),
    });
    expect(rec.seen).toHaveLength(1);
  });

  it('refuses, by default, a frame from a connection $connect never accepted', async () => {
    const rec = recorder();
    const { bridge } = bridgeWith({ protocols: [rec.protocol] });
    new ApiGatewayWsAdapter(fakeApp().app, bridge);
    const res = await bridge.dispatch({
      requestContext: { connectionId: 'stranger', eventType: 'MESSAGE', routeKey: '$default' },
      body: JSON.stringify({ event: 'x' }),
    });
    expect(res.statusCode).toBe(403);
    expect(rec.seen).toHaveLength(0);
  });
});
