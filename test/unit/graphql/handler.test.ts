import { GraphQLID, GraphQLNonNull, GraphQLObjectType, GraphQLSchema, GraphQLString } from 'graphql';
import { withFilter } from 'graphql-subscriptions';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ConnectionGoneError, GatewayBridge, GatewayClient, InMemoryConnectionStore } from '../../../src';
import {
  DynamoSubscriptionRegistry,
  GraphQLWsHandler,
  InMemorySubscriptionRegistry,
  createGraphQLWsHandler,
  enableGraphQLSubscriptions,
} from '../../../src/graphql';
import { connectEvent, frameEvent, nextId } from '../../helpers/events';
import { SpyPublisher, bridgeWith, minimalStore } from '../../helpers/fakes';

const Said = new GraphQLObjectType({
  name: 'Said',
  fields: { text: { type: GraphQLString }, who: { type: GraphQLString } },
});

const schema = new GraphQLSchema({
  query: new GraphQLObjectType({
    name: 'Query',
    fields: {
      whoami: { type: GraphQLString, resolve: (_r, _a, ctx) => ctx.connection?.data?.user ?? null },
      tenant: { type: GraphQLString, resolve: (_r, _a, ctx) => ctx.tenant ?? null },
      connectionId: { type: GraphQLID, resolve: (_r, _a, ctx) => ctx.connectionId },
      pubsubIsReal: { type: GraphQLString, resolve: (_r, _a, ctx) => String(typeof ctx.pubsub?.publish) },
      boom: { type: GraphQLString, resolve: () => { throw new Error('kaboom'); } },
    },
  }),
  mutation: new GraphQLObjectType({
    name: 'Mutation',
    fields: {
      say: {
        type: GraphQLString,
        args: { text: { type: new GraphQLNonNull(GraphQLString) } },
        resolve: async (_r, { text }, ctx) => {
          await ctx.pubsub.publish('SAID', { text });
          return text;
        },
      },
    },
  }),
  subscription: new GraphQLObjectType({
    name: 'Subscription',
    fields: {
      said: {
        type: Said,
        subscribe: (_r, _a, ctx) => ctx.pubsub.asyncIterableIterator('SAID'),
        resolve: (payload: any, _a, ctx) => ({ text: payload.text, who: ctx.connection?.data?.user ?? null }),
      },
      saidContaining: {
        type: Said,
        args: { term: { type: new GraphQLNonNull(GraphQLString) } },
        subscribe: withFilter(
          (_r: any, _a: any, ctx: any) => ctx.pubsub.asyncIterableIterator('SAID'),
          (payload: any, args: any) => payload.text.includes(args.term),
        ) as any,
        resolve: (payload: any) => ({ text: payload.text }),
      },
      untracked: {
        type: GraphQLString,
        subscribe: async function* () {
          yield 'never durable';
        },
      },
      failing: {
        type: GraphQLString,
        subscribe: () => { throw new Error('cannot subscribe'); },
      },
    },
  }),
});

function setup(options: Partial<ConstructorParameters<typeof GraphQLWsHandler>[0]> = {}) {
  const publisher = new SpyPublisher();
  const registry = new InMemorySubscriptionRegistry();
  const handler = createGraphQLWsHandler({ schema, registry, ...options });
  const { bridge, store } = bridgeWith({ publisher, protocols: [handler] });
  const client = (id = nextId(), data: Record<string, unknown> = {}) =>
    new GatewayClient(id, store, publisher, { data });
  /** Frames sent to one connection, without the envelope. */
  const to = (id: string) => publisher.raw.filter(m => m.id === id).map(m => m.payload);
  return { publisher, registry, handler, bridge, store, client, to };
}

const subscribe = (id: string, query: string, variables?: Record<string, unknown>, operationName?: string) => ({
  type: 'subscribe', id, payload: { query, ...(variables ? { variables } : {}), ...(operationName ? { operationName } : {}) },
});

beforeEach(() => {
  vi.spyOn(console, 'error').mockImplementation(() => {});
});

describe('GraphQLWsHandler', () => {
  it('declares its subprotocol and its fan-out kind', () => {
    const { handler, bridge } = setup();
    expect(handler.subprotocol).toBe('graphql-transport-ws');
    expect(bridge.subprotocols).toContain('graphql-transport-ws');
    expect(bridge.flusher.kinds()).toContain('topic');
  });

  it('defaults its registry from the bridge\'s provider', () => {
    const local = new GraphQLWsHandler({ schema });
    GatewayBridge.builder().provider('local').use(local).build();
    expect((local as any)._registry).toBeInstanceOf(InMemorySubscriptionRegistry);
    const aws = new GraphQLWsHandler({ schema });
    GatewayBridge.builder().provider('aws').store(new InMemoryConnectionStore()).use(aws).build();
    expect((aws as any)._registry).toBeInstanceOf(DynamoSubscriptionRegistry);
  });

  it('refuses to serve before it is registered on a bridge', async () => {
    const handler = new GraphQLWsHandler({ schema: () => schema });
    const client = new GatewayClient('a', minimalStore(), new SpyPublisher());
    await expect(handler.handleFrame({ type: 'complete', id: '1' }, client)).rejects.toThrow(/before serving frames/);
    await expect(handler.handleFrame({ type: 'connection_init' }, client)).rejects.toThrow(/call bridge\.use\(handler\)/);
  });

  it('leaves frames that are not graphql-ws to the next protocol', async () => {
    const { handler, client } = setup();
    expect(await handler.handleFrame({ event: 'chat.send', data: {} }, client())).toBe(false);
  });

  describe('the protocol', () => {
    it('acknowledges connection_init, answers ping with pong, ignores pong', async () => {
      const { handler, client, to } = setup();
      const c = client();
      expect(await handler.handleFrame({ type: 'connection_init' }, c)).toBe(true);
      await handler.handleFrame({ type: 'ping' }, c);
      await handler.handleFrame({ type: 'ping', payload: { at: 1 } }, c);
      expect(await handler.handleFrame({ type: 'pong' }, c)).toBe(true);
      expect(to(c.connectionId)).toEqual([
        { type: 'connection_ack' },
        { type: 'pong' },
        { type: 'pong', payload: { at: 1 } },
      ]);
    });

    it('runs a query: next, then complete', async () => {
      const { handler, client, to } = setup();
      const c = client('q-conn');
      await handler.handleFrame(subscribe('1', '{ connectionId pubsubIsReal }'), c);
      expect(to('q-conn')).toEqual([
        { type: 'next', id: '1', payload: { data: { connectionId: 'q-conn', pubsubIsReal: 'function' } } },
        { type: 'complete', id: '1' },
      ]);
    });

    it('reports resolver errors inside the result', async () => {
      const { handler, client, to } = setup();
      const c = client();
      await handler.handleFrame(subscribe('1', '{ boom }'), c);
      expect(to(c.connectionId)[0]).toMatchObject({
        type: 'next', payload: { data: { boom: null }, errors: [{ message: 'kaboom' }] },
      });
    });

    it.each([
      ['a document that does not parse', '{', /Syntax Error/],
      ['a document that does not validate', '{ nope }', /Cannot query field .*nope/],
      ['an operation name that is not there', 'query A { tenant } query B { tenant }', /unable to identify the operation/],
    ])('answers %s with an error frame', async (_what, query, message) => {
      const { handler, client, to } = setup();
      const c = client();
      await handler.handleFrame(subscribe('1', query, undefined, _what.includes('name') ? 'C' : undefined), c);
      const [frame] = to(c.connectionId);
      expect(frame).toMatchObject({ type: 'error', id: '1' });
      expect(JSON.stringify(frame.payload)).toMatch(message);
    });
  });

  describe('subscriptions', () => {
    it('persists what is needed to replay it — identity included — and sends nothing yet', async () => {
      const { handler, client, registry, to } = setup();
      const c = client('s-conn', { user: 'bob' });
      await handler.handleFrame(subscribe('s1', 'subscription { said { text who } }'), c);
      expect(await registry.byTopic('SAID')).toEqual([{
        connectionId: 's-conn', subscriptionId: 's1', topics: ['SAID'],
        query: 'subscription { said { text who } }', variables: null, operationName: null,
        connectionData: { user: 'bob' },
      }]);
      expect(to('s-conn')).toEqual([]);
    });

    it('leaves the snapshot out for an anonymous connection', async () => {
      const { handler, client, registry } = setup();
      await handler.handleFrame(subscribe('s1', 'subscription { said { text } }'), client());
      expect((await registry.byTopic('SAID'))[0]).not.toHaveProperty('connectionData');
    });

    it('refuses a subscription backed by no durable topic', async () => {
      const { handler, client, to } = setup();
      const c = client();
      await handler.handleFrame(subscribe('s1', 'subscription { untracked }'), c);
      expect(JSON.stringify(to(c.connectionId)[0])).toMatch(/did not register a topic with ApiGwPubSub/);
    });

    it('reports a subscribe resolver that throws', async () => {
      const { handler, client, to } = setup();
      const c = client();
      await handler.handleFrame(subscribe('s1', 'subscription { failing }'), c);
      expect(JSON.stringify(to(c.connectionId)[0])).toMatch(/cannot subscribe/);
    });

    it('reports a source stage that throws, or yields no stream and no errors', async () => {
      const { handler, client, to } = setup();
      const c = client();
      vi.spyOn(handler.pubsub, 'captureTopics').mockRejectedValueOnce(new Error('source exploded'));
      await handler.handleFrame(subscribe('s1', 'subscription { said { text } }'), c);
      vi.spyOn(handler.pubsub, 'captureTopics').mockResolvedValueOnce({ result: {}, topics: [] } as any);
      await handler.handleFrame(subscribe('s2', 'subscription { said { text } }'), c);
      const [first, second] = to(c.connectionId);
      expect(first.payload).toEqual([{ message: 'source exploded' }]);
      expect(second.payload).toEqual([expect.objectContaining({ message: 'subscription failed to start' })]);
    });

    it('stops on complete, and is forgotten on disconnect', async () => {
      const { handler, client, registry } = setup();
      const c = client();
      await handler.handleFrame(subscribe('s1', 'subscription { said { text } }'), c);
      await handler.handleFrame(subscribe('s2', 'subscription { said { text } }'), c);
      await handler.handleFrame({ type: 'complete', id: 's1' }, c);
      expect((await registry.byTopic('SAID')).map(s => s.subscriptionId)).toEqual(['s2']);
      await handler.onDisconnect(c.connectionId);
      expect(await registry.byTopic('SAID')).toEqual([]);
    });
  });

  describe('delivery', () => {
    async function subscribed(query: string, variables?: Record<string, unknown>, data: Record<string, unknown> = {}) {
      const ctx = setup();
      const c = ctx.client(nextId(), data);
      await ctx.handler.handleFrame(subscribe('s1', query, variables), c);
      return { ...ctx, id: c.connectionId };
    }

    it('replays a publish through the stored subscription, with the stored identity', async () => {
      const { bridge, to, id, store } = await subscribed('subscription { said { text who } }', undefined, { user: 'bob' });
      const reads = vi.spyOn(store, 'get');
      await bridge.flushRecord({ kind: 'topic', topic: 'SAID', payload: { text: 'hi' }, messageId: 'm', publishedAt: 1 } as any);
      expect(to(id)).toEqual([{ type: 'next', id: 's1', payload: { data: { said: { text: 'hi', who: 'bob' } } } }]);
      expect(reads).not.toHaveBeenCalled();
    });

    it('runs a filter per subscriber, with that subscriber\'s variables', async () => {
      const { bridge, to, id } = await subscribed('subscription($t: String!) { saidContaining(term: $t) { text } }', { t: 'cat' });
      const publish = (text: string, messageId: string) =>
        bridge.flushRecord({ kind: 'topic', topic: 'SAID', payload: { text }, messageId, publishedAt: 1 } as any);
      await publish('a dog', 'm1');
      await publish('a cat', 'm2');
      expect(to(id).map(f => f.payload.data.saidContaining.text)).toEqual(['a cat']);
    });

    it('drops a stored document that no longer parses', async () => {
      const { bridge, registry } = setup();
      await registry.add({ connectionId: 'c', subscriptionId: 's', topics: ['SAID'], query: 'subscription {' });
      await bridge.flushRecord({ kind: 'topic', topic: 'SAID', payload: {}, messageId: 'm', publishedAt: 1 } as any);
      expect(await registry.byTopic('SAID')).toEqual([]);
    });

    it('answers a stored subscription the schema no longer has with an error frame', async () => {
      const { bridge, registry, to } = setup();
      await registry.add({ connectionId: 'c', subscriptionId: 's', topics: ['SAID'], query: 'subscription { removedField }' });
      await bridge.flushRecord({ kind: 'topic', topic: 'SAID', payload: {}, messageId: 'm', publishedAt: 1 } as any);
      expect(to('c')[0]).toMatchObject({ type: 'error', id: 's' });
    });

    it('answers a replay that yields no stream and no errors with an error frame', async () => {
      const { bridge, handler, to, id } = await subscribed('subscription { said { text } }');
      vi.spyOn(handler.pubsub, 'replayPayload').mockResolvedValueOnce({} as any);
      await bridge.flushRecord({ kind: 'topic', topic: 'SAID', payload: {}, messageId: 'm', publishedAt: 1 } as any);
      expect(to(id)[0].payload).toEqual([expect.objectContaining({ message: 'subscription failed to resume' })]);
    });

    it('lets a failed send reach the flusher, so the record is retried', async () => {
      const { bridge, publisher } = await subscribed('subscription { said { text } }');
      publisher.toConnectionRaw = async () => { throw new Error('throttled'); };
      await expect(
        bridge.flushRecord({ kind: 'topic', topic: 'SAID', payload: { text: 'x' }, messageId: 'm', publishedAt: 1 } as any),
      ).rejects.toThrow(/deliveries failed/);
    });

    it('publishes from a mutation over the socket', async () => {
      const { handler, client, bridge, to } = setup();
      const listener = client();
      const speaker = client();
      await handler.handleFrame(subscribe('s1', 'subscription { said { text } }'), listener);
      await handler.handleFrame(subscribe('m1', 'mutation { say(text: "hello") }'), speaker);
      expect(to(listener.connectionId)).toEqual([{ type: 'next', id: 's1', payload: { data: { said: { text: 'hello' } } } }]);
      expect(bridge.flusher.kinds()).toContain('topic');
    });
  });

  describe('sending', () => {
    it('reaps a gone connection while answering a frame, rather than failing it', async () => {
      const { handler, client, bridge, publisher } = setup();
      publisher.toConnectionRaw = async id => { throw new ConnectionGoneError(id); };
      const cleanup = vi.spyOn(bridge, 'cleanup');
      const c = client();
      expect(await handler.handleFrame({ type: 'connection_init' }, c)).toBe(true);
      expect(cleanup).toHaveBeenCalledWith(c.connectionId, 'gone');
    });

    it('lets any other send failure propagate', async () => {
      const { handler, client, publisher } = setup();
      publisher.toConnectionRaw = async () => { throw new Error('throttled'); };
      await expect(handler.handleFrame({ type: 'connection_init' }, client())).rejects.toThrow('throttled');
    });

    it('needs a publisher that can send raw frames', async () => {
      const handler = createGraphQLWsHandler({ schema });
      const bridge = GatewayBridge.builder().provider('local')
        .publisher({ toConnection: async () => {} }).use(handler).build();
      const client = new GatewayClient('a', bridge.store, bridge.publisher);
      await expect(handler.handleFrame({ type: 'connection_init' }, client)).rejects.toThrow(/no toConnectionRaw/);
    });
  });

  describe('the operation context', () => {
    it('carries the connection, merges the caller\'s context, and keeps pubsub its own', async () => {
      const context = vi.fn(async (_id: string, connection: { data: Record<string, unknown> }) => ({
        tenant: `t-${connection.data.user}`,
        pubsub: 'impostor',
      }));
      const { handler, client, to } = setup({ context });
      const c = client('ctx', { user: 'ada' });
      await handler.handleFrame(subscribe('1', '{ whoami tenant pubsubIsReal }'), c);
      expect(context).toHaveBeenCalledWith('ctx', { data: { user: 'ada' } });
      expect(to('ctx')[0].payload.data).toEqual({ whoami: 'ada', tenant: 't-ada', pubsubIsReal: 'function' });
    });

    it('lets the caller\'s context replace `connection`, as it could before', async () => {
      const { handler, client, to } = setup({ context: () => ({ connection: { data: { user: 'override' } } }) });
      const c = client('ctx2', { user: 'ada' });
      await handler.handleFrame(subscribe('1', '{ whoami }'), c);
      expect(to('ctx2')[0].payload.data.whoami).toBe('override');
    });
  });

  it('formats an error that is not a GraphQLError', async () => {
    const { handler, client, to } = setup();
    const c = client();
    vi.spyOn(handler.pubsub, 'captureTopics').mockRejectedValueOnce('a bare string');
    await handler.handleFrame(subscribe('s1', 'subscription { said { text } }'), c);
    expect(to(c.connectionId)[0].payload).toEqual([{ message: 'a bare string' }]);
  });
});

describe('enableGraphQLSubscriptions', () => {
  const appWith = (get: () => unknown) => ({ get: vi.fn(get) }) as any;

  it('registers the protocol with the app\'s schema and returns the PubSub', async () => {
    const bridge = GatewayBridge.builder().provider('local').build();
    const pubsub = enableGraphQLSubscriptions(appWith(() => ({ schema })), bridge);
    expect(pubsub.attached).toBe(true);
    expect(bridge.subprotocols).toContain('graphql-transport-ws');
    expect(bridge.flusher.kinds()).toContain('topic');
  });

  it('explains a missing GraphQLModule', () => {
    const bridge = GatewayBridge.builder().provider('local').build();
    expect(() => enableGraphQLSubscriptions(appWith(() => { throw new Error('no provider'); }), bridge)).toThrow(
      /GraphQLSchemaHost is not available/,
    );
  });

  it('explains a call made before the schema exists', () => {
    const bridge = GatewayBridge.builder().provider('local').build();
    const host = { get schema(): GraphQLSchema { throw new Error('not built'); } };
    expect(() => enableGraphQLSubscriptions(appWith(() => host), bridge)).toThrow(/Call this AFTER/);
  });
});

describe('over a bridge', () => {
  it('handles a graphql-transport-ws socket end to end', async () => {
    const { bridge, to, store } = setup();
    const id = nextId();
    const connected = await bridge.dispatch(connectEvent(id, { headers: { 'sec-websocket-protocol': 'graphql-transport-ws' } }));
    expect(connected.headers).toEqual({ 'Sec-WebSocket-Protocol': 'graphql-transport-ws' });
    await bridge.dispatch(frameEvent(id, { type: 'connection_init' }));
    await bridge.dispatch(frameEvent(id, subscribe('1', '{ tenant }')));
    expect(to(id).map(f => f.type)).toEqual(['connection_ack', 'next', 'complete']);
    expect(await store.get(id)).not.toBeNull();
  });
});
