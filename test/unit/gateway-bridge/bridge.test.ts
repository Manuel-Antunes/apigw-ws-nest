import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  ApiGatewayPublisher,
  ConnectionGoneError,
  DynamoConnectionStore,
  DynamoDeliveryLedger,
  DynamoOutboxBus,
  GLOBAL_ROOM,
  GatewayBridge,
  GatewayClient,
  InMemoryConnectionStore,
  InMemoryDeliveryLedger,
  InlineMessageBus,
  LocalPublisher,
  ProtocolHandler,
} from '../../../src';
import { connectEvent, disconnectEvent, frameEvent, nextId, wsEvent } from '../../helpers/events';
import { SpyPublisher, SpyStore, bridgeWith, minimalStore, recorder } from '../../helpers/fakes';

beforeEach(() => {
  vi.spyOn(console, 'error').mockImplementation(() => {});
  vi.spyOn(console, 'warn').mockImplementation(() => {});
});
afterEach(() => {
  delete process.env.MANAGEMENT_ENDPOINT;
});

describe('GatewayBridge.builder', () => {
  it('picks a matched set of backends per provider', () => {
    const local = GatewayBridge.builder().provider('local').build();
    expect(local.provider).toBe('local');
    expect(local.store).toBeInstanceOf(InMemoryConnectionStore);
    expect(local.publisher).toBeInstanceOf(LocalPublisher);
    expect(local.bus).toBeInstanceOf(InlineMessageBus);

    const aws = GatewayBridge.builder().provider('aws').build();
    expect(aws.store).toBeInstanceOf(DynamoConnectionStore);
    expect(aws.publisher).toBeInstanceOf(ApiGatewayPublisher);
    expect(aws.bus).toBeInstanceOf(DynamoOutboxBus);
    expect((aws.flusher as any).options.ledger).toBeInstanceOf(DynamoDeliveryLedger);
  });

  it('lets an individual setter win over the preset, whatever the call order', () => {
    const store = minimalStore();
    const ledger = new InMemoryDeliveryLedger();
    const bus = { publish: async () => {} };
    const a = GatewayBridge.builder().provider('aws').store(store).ledger(ledger).bus(bus).build();
    const b = GatewayBridge.builder().store(store).provider('aws').build();
    expect(a.store).toBe(store);
    expect(b.store).toBe(store);
    expect(a.bus).toBe(bus);
    expect((a.flusher as any).options.ledger).toBe(ledger);
  });

  it('hands a publisher factory the resolved store, and a bus factory the bridge', () => {
    const store = new InMemoryConnectionStore();
    const publisher = new SpyPublisher();
    const publisherFactory = vi.fn(() => publisher);
    const busFactory = vi.fn(() => ({ publish: async () => {} }));
    const bridge = GatewayBridge.builder()
      .provider('local').store(store).publisher(publisherFactory).bus(busFactory).build();
    expect(publisherFactory).toHaveBeenCalledWith(store);
    expect(busFactory).toHaveBeenCalledWith(bridge);
    expect(bridge.publisher).toBe(publisher);
  });

  it('passes the tuning knobs through', () => {
    const bridge = GatewayBridge.builder().provider('local')
      .concurrency(3).reserveMs(500).connectTimeout(1234).maxConnectionData(99).build();
    expect((bridge.flusher as any).concurrency).toBe(3);
    expect((bridge.flusher as any).reserveMs).toBe(500);
    expect((bridge as any).connectTimeout).toBe(1234);
    expect((bridge as any).maxConnectionData).toBe(99);
  });

  it('refuses an unknown provider', () => {
    expect(() => GatewayBridge.builder().provider('gcp' as any).build()).toThrow(/unknown provider "gcp"/);
  });

  it('builds a subclass when asked', () => {
    class SubBridge extends GatewayBridge {
      tag() {
        return 'sub';
      }
    }
    const sub = GatewayBridge.builder().provider('local').build(SubBridge);
    expect(sub).toBeInstanceOf(SubBridge);
    expect(sub.tag()).toBe('sub');
    expect(sub.store).toBeInstanceOf(InMemoryConnectionStore);
  });

  it('shares nothing between two bridges', () => {
    expect(GatewayBridge.builder().provider('local').build().store).not.toBe(
      GatewayBridge.builder().provider('local').build().store,
    );
  });
});

describe('protocols', () => {
  it('registers a protocol\'s fan-out kinds and attaches it', () => {
    const attach = vi.fn();
    const protocol: ProtocolHandler = {
      handleFrame: async () => false,
      attach,
      fanout: { presence: async () => ({ targets: [] }) },
    };
    const { bridge } = bridgeWith({ protocols: [protocol] });
    expect(attach).toHaveBeenCalledWith(bridge);
    expect(bridge.flusher.kinds()).toEqual(['room', 'presence']);
  });

  it('derives the negotiable subprotocols, deduplicated, plus explicit extras', () => {
    const bridge = GatewayBridge.builder().provider('local')
      .use({ subprotocol: 'graphql-transport-ws', handleFrame: async () => false })
      .use({ subprotocol: 'graphql-transport-ws', handleFrame: async () => false })
      .subprotocols('phoenix')
      .build();
    expect(bridge.subprotocols).toEqual(['graphql-transport-ws', 'phoenix']);
    expect(GatewayBridge.builder().provider('local').build().subprotocols).toEqual([]);
  });
});

describe('bridge.serve', () => {
  it('routes by event shape', async () => {
    const { bridge } = bridgeWith();
    expect(await bridge.serve(connectEvent(nextId()))).toEqual({ statusCode: 200 });
    expect(await bridge.serve({ Records: [] })).toEqual({ batchItemFailures: [] });
    await expect(bridge.serve({ nope: true })).rejects.toThrow(/unrecognised event/);
  });
});

describe('dispatch without connect hooks', () => {
  it('accepts $connect, auto-joining the global room', async () => {
    const { bridge, store } = bridgeWith();
    const id = nextId();
    expect(await bridge.dispatch(connectEvent(id))).toEqual({ statusCode: 200 });
    expect(await store.get(id)).toEqual({ connectedAt: expect.any(Number) });
    expect(await store.membersOf(GLOBAL_ROOM)).toContain(id);
  });

  it('negotiates and echoes a subprotocol — and keeps that socket out of the global room', async () => {
    const { bridge, store } = bridgeWith({
      protocols: [{ subprotocol: 'graphql-transport-ws', handleFrame: async () => false }],
    });
    const id = nextId();
    const res = await bridge.dispatch(
      connectEvent(id, { headers: { 'Sec-WebSocket-Protocol': 'unknown, graphql-transport-ws' } }),
    );
    expect(res).toEqual({ statusCode: 200, headers: { 'Sec-WebSocket-Protocol': 'graphql-transport-ws' } });
    expect((await store.get(id))!.subprotocol).toBe('graphql-transport-ws');
    expect(await store.membersOf(GLOBAL_ROOM)).not.toContain(id);
  });

  it('echoes nothing for a subprotocol no protocol answers to', async () => {
    const { bridge } = bridgeWith();
    expect(await bridge.dispatch(connectEvent(nextId(), { headers: { 'sec-websocket-protocol': 'x' } }))).toEqual({
      statusCode: 200,
    });
  });

  it('sets the @connections endpoint from an aws event', async () => {
    const bridge = GatewayBridge.builder().provider('aws').store(new InMemoryConnectionStore()).build();
    await bridge.dispatch(connectEvent(nextId(), { requestContext: { domainName: 'abc.example', stage: 'prod' } }));
    expect(process.env.MANAGEMENT_ENDPOINT).toBe('https://abc.example/prod');
  });

  it('leaves the endpoint alone in local mode', async () => {
    const { bridge } = bridgeWith();
    await bridge.dispatch(connectEvent(nextId(), { requestContext: { domainName: 'abc.example' } }));
    expect(process.env.MANAGEMENT_ENDPOINT).toBeUndefined();
  });

  it('offers a frame to signature-bearing protocols first, the fallback last', async () => {
    const order: string[] = [];
    const named = (name: string, claims: boolean, fallback = false): ProtocolHandler => ({
      name, fallback,
      handleFrame: async () => {
        order.push(name);
        return claims;
      },
    });
    const { bridge } = bridgeWith({
      protocols: [named('fallback', true, true), named('first', false), named('second', true), named('third', true)],
    });
    expect(await bridge.dispatch(frameEvent(nextId(), { event: 'x' }))).toEqual({ statusCode: 200 });
    expect(order).toEqual(['first', 'second']);
  });

  it('turns a body-less frame into { event: routeKey }', async () => {
    const rec = recorder();
    const { bridge } = bridgeWith({ protocols: [rec.protocol] });
    await bridge.dispatch(wsEvent(nextId(), 'MESSAGE', { requestContext: { routeKey: 'ping' } }));
    expect(rec.seen[0].frame).toEqual({ event: 'ping', data: {} });
  });

  it('answers 500 when a frame cannot be handled', async () => {
    const { bridge } = bridgeWith();
    const res = await bridge.dispatch(wsEvent(nextId(), 'MESSAGE', { body: '{not json' }));
    expect(res).toEqual({ statusCode: 500, body: 'dispatch failed' });
    expect(console.error).toHaveBeenCalledWith('[dispatch error]', expect.any(SyntaxError));
  });

  it('serves a frame from any connectionId — there is nothing to check it against', async () => {
    const rec = recorder();
    const { bridge } = bridgeWith({ protocols: [rec.protocol] });
    await bridge.dispatch(frameEvent('never-connected', { event: 'x' }));
    expect(rec.seen).toHaveLength(1);
  });

  it('builds one client per connection and announces it once', async () => {
    const rec = recorder();
    const { bridge } = bridgeWith({ protocols: [rec.protocol] });
    const announced = vi.fn();
    bridge.server.on('connection', announced);
    await bridge.dispatch(frameEvent('a', { n: 1 }, { requestContext: { authorizer: { p: 1 } } }));
    await bridge.dispatch(frameEvent('a', { n: 2 }));
    expect(announced).toHaveBeenCalledOnce();
    expect(rec.seen[0].client).toBe(rec.seen[1].client);
    expect(rec.seen[0].client.handshake).toEqual({
      headers: {}, query: {}, subprotocols: [],
      connectedAt: 1_700_000_000_000, sourceIp: '203.0.113.7', authorizer: { p: 1 },
    });
  });

  it('ensureClient() works without an event', () => {
    const { bridge } = bridgeWith();
    const client = bridge.ensureClient('x');
    expect(client).toBeInstanceOf(GatewayClient);
    expect(bridge.ensureClient('x')).toBe(client);
    expect(client.handshake.connectedAt).toEqual(expect.any(Number));
  });
});

describe('cleanup', () => {
  it('runs every protocol\'s onDisconnect, then forgets the connection', async () => {
    const rec = recorder();
    const { bridge, store } = bridgeWith({ protocols: [rec.protocol] });
    const id = nextId();
    await bridge.dispatch(connectEvent(id));
    await bridge.dispatch(frameEvent(id, {}));
    await bridge.dispatch(disconnectEvent(id, 'going away'));
    expect(rec.disconnects).toEqual([{ id, client: rec.seen[0].client, reason: 'going away' }]);
    expect(await store.get(id)).toBeNull();
    expect(await store.membersOf(GLOBAL_ROOM)).not.toContain(id);
  });

  it('defaults the reason, and passes no client when this instance has none', async () => {
    const rec = recorder();
    const { bridge } = bridgeWith({ protocols: [rec.protocol] });
    await bridge.dispatch(disconnectEvent('stranger'));
    expect(rec.disconnects).toEqual([{ id: 'stranger', client: undefined, reason: 'disconnect' }]);
  });

  it('keeps going when one protocol\'s cleanup fails', async () => {
    const rec = recorder();
    const { bridge, store } = bridgeWith({
      protocols: [
        { name: 'flaky', handleFrame: async () => false, onDisconnect: async () => { throw new Error('nope'); } },
        { handleFrame: async () => false, onDisconnect: async () => { throw new Error('anonymous'); } },
        rec.protocol,
      ],
    });
    const id = nextId();
    await bridge.dispatch(connectEvent(id));
    await bridge.cleanup(id);
    expect(rec.disconnects).toHaveLength(1);
    expect(await store.get(id)).toBeNull();
    expect(console.error).toHaveBeenCalledWith('[disconnect: flaky]', expect.any(Error));
    expect(console.error).toHaveBeenCalledWith('[disconnect: protocol]', expect.any(Error));
  });

  it('tears down the live streams the connection opened', async () => {
    const { bridge } = bridgeWith();
    const client = bridge.ensureClient('a');
    const unsubscribe = vi.fn();
    client.subscriptions.push({ unsubscribe } as any);
    await bridge.cleanup('a');
    expect(unsubscribe).toHaveBeenCalledOnce();
    expect(bridge.ensureClient('a')).not.toBe(client);
  });
});

describe('onSendError', () => {
  it('reaps a gone connection', async () => {
    const rec = recorder();
    const { bridge } = bridgeWith({ protocols: [rec.protocol] });
    await bridge.onSendError(bridge.ensureClient('a'), new ConnectionGoneError('a'));
    expect(rec.disconnects[0]).toMatchObject({ id: 'a', reason: 'gone' });
  });

  it('logs anything else', async () => {
    const { bridge } = bridgeWith();
    const err = new Error('boom');
    await bridge.onSendError(bridge.ensureClient('a'), err);
    expect(console.error).toHaveBeenCalledWith('[send error]', err);
  });
});

describe('bridge.disconnect', () => {
  it('cleans up, then has the publisher close the socket', async () => {
    const order: string[] = [];
    const publisher = new SpyPublisher();
    publisher.disconnect = async id => void order.push(`close ${id}`);
    const { bridge } = bridgeWith({
      publisher,
      protocols: [{ handleFrame: async () => false, onDisconnect: async (id, _c, reason) => void order.push(`${reason} ${id}`) }],
    });
    await bridge.disconnect('a');
    expect(order).toEqual(['server disconnect a', 'close a']);
  });

  it('is what client.disconnect() does on a legacy client', async () => {
    const rec = recorder();
    const { bridge, publisher } = bridgeWith({ protocols: [rec.protocol] });
    await bridge.dispatch(frameEvent('legacy', {}));
    await rec.seen[0].client.disconnect();
    expect(rec.disconnects[0]).toMatchObject({ id: 'legacy', reason: 'server disconnect' });
    expect((publisher as SpyPublisher).disconnected).toEqual(['legacy']);
  });

  it('refuses when the publisher cannot close sockets', async () => {
    const bridge = GatewayBridge.builder().provider('local')
      .publisher({ toConnection: async () => {} }).build();
    await expect(bridge.disconnect('a')).rejects.toThrow(/does not implement disconnect/);
  });
});

describe('broadcasts', () => {
  it('records through the bus', async () => {
    const published: unknown[] = [];
    const bridge = GatewayBridge.builder().provider('local')
      .bus({ publish: async m => void published.push(m) }).build();
    await bridge.publish({ kind: 'presence', who: 'ada' });
    expect(published).toEqual([{ kind: 'presence', who: 'ada' }]);
  });

  it('delivers a room broadcast to its members, inline in local mode', async () => {
    const { bridge, store, publisher } = bridgeWith();
    await store.join('a', 'r');
    await store.join('b', 'r');
    await bridge.server.to('r').emit('news', { n: 1 });
    expect((publisher as SpyPublisher).sent).toEqual([
      { id: 'a', event: 'news', data: { n: 1 } },
      { id: 'b', event: 'news', data: { n: 1 } },
    ]);
  });

  it('falls back to membersOf() for a store that cannot page', async () => {
    const publisher = new SpyPublisher();
    const bridge = GatewayBridge.builder().provider('local')
      .store(minimalStore(['x', 'y'])).publisher(publisher).build();
    await bridge.server.emit('hello', 1);
    expect(publisher.sent.map(f => f.id)).toEqual(['x', 'y']);
  });
});

describe('continuations', () => {
  it('requeue the rest of a room through the bridge\'s own bus', async () => {
    const published: any[] = [];
    const store = new SpyStore();
    for (const id of ['a', 'b', 'c']) await store.join(id, 'big');
    vi.spyOn(store, 'pageMembersOf').mockImplementation(async (_room, cursor?: any) =>
      cursor ? { items: ['c'], cursor: undefined } : { items: ['a', 'b'], cursor: { after: 'b' } },
    );
    const publisher = new SpyPublisher();
    const bridge = GatewayBridge.builder().provider('local').store(store).publisher(publisher)
      .bus({ publish: async m => void published.push(m) }).reserveMs(1000).build();
    await bridge.flushRecord(
      { kind: 'room', room: 'big', event: 'e', data: 1, messageId: 'm', publishedAt: 1 } as any,
      { remainingMs: () => 10 },
    );
    expect(publisher.sent.map(f => f.id)).toEqual(['a', 'b']);
    expect(published).toEqual([expect.objectContaining({ messageId: 'm', cursor: { after: 'b' } })]);
  });
});

describe('bridge.flush', () => {
  const image = (pk: string, messageId: string) => ({
    eventName: 'INSERT',
    dynamodb: {
      SequenceNumber: messageId,
      NewImage: {
        pk: { S: pk }, sk: { S: messageId }, kind: { S: 'test' }, topic: { S: pk },
        messageId: { S: messageId }, publishedAt: { N: '1' }, payload: { M: {} },
      },
    },
  });

  it('keeps per-topic order, and stops a topic at its first failure', async () => {
    const order: string[] = [];
    const remaining: Array<number | undefined> = [];
    const failing = new Set(['t2-b']);
    const { bridge } = bridgeWith({
      protocols: [{
        handleFrame: async () => false,
        fanout: {
          test: async record => ({
            targets: [{
              connectionId: 'c', key: `K#${record.messageId}`,
              send: async () => {
                if (failing.has(record.messageId)) throw new Error('boom');
                order.push(record.messageId);
              },
            }],
          }),
        },
      }],
    });
    const flushRecord = bridge.flushRecord.bind(bridge);
    vi.spyOn(bridge, 'flushRecord').mockImplementation((record, ctx) => {
      remaining.push(ctx?.remainingMs?.());
      return flushRecord(record, ctx);
    });
    const out = await bridge.flush(
      { Records: [image('TOPIC#t1', 't1-a'), image('TOPIC#t1', 't1-b'), image('TOPIC#t2', 't2-b'), image('TOPIC#t2', 't2-c')] },
      { getRemainingTimeInMillis: () => 9000 },
    );
    expect(order).toEqual(['t1-a', 't1-b']);
    expect(out.batchItemFailures).toEqual([{ itemIdentifier: 't2-b' }]);
    expect(remaining).toEqual([9000, 9000, 9000]);
  });
});
