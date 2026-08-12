/* =============================================================================
 *  GraphQLWsHandler — the graphql-ws server loop, as a bridge protocol.
 * =============================================================================
 *  Same shape as graphql-sse's createHandler({ schema }): hand it a schema, it
 *  executes operations. It owns no global state and knows nothing about how the
 *  socket got here — the bridge hands it frames, it hands back `true` when the
 *  frame was its own.
 *
 *      const { schema } = app.get(GraphQLSchemaHost);
 *      bridge.use(createGraphQLWsHandler({ schema }));
 *
 *  It answers graphql-ws's protocol as a pure function of (frame, durable state):
 *
 *    connection_init                 -> connection_ack                (no state)
 *    ping                            -> pong                          (no state)
 *    subscribe, query/mutation       -> execute now, next + complete  (no state)
 *    subscribe, subscription         -> learn the topics, persist the row
 *    <someone publishes to a topic>  -> replay every stored row, push `next`
 *    complete / $disconnect / 410    -> delete the rows
 *
 *  The only thing that survives between frames is the registry row — which is
 *  exactly what lets a subscription outlive a redeploy, a cold start, or being
 *  served by a different container than the one that created it.
 *
 *  WHY APOLLO ISN'T IN THIS PICTURE. ApolloServer#executeOperation does not run a
 *  subscription's `subscribe` at all — it executes the operation like a query, so
 *  `resolve` receives undefined. Apollo's own recommended subscription setup,
 *  graphql-ws + useServer, bypasses Apollo Server and calls graphql-js directly.
 *  That is exactly what this does.
 * ========================================================================== */

import {
  execute,
  parse,
  validate,
  createSourceEventStream,
  getOperationAST,
  GraphQLError,
  GraphQLSchema,
  ExecutionResult,
  GraphQLFormattedError,
} from 'graphql';
import type { INestApplication } from '@nestjs/common';

import {
  FanoutResolver,
  GatewayBridge,
  GatewayClient,
  OutboxRecord,
  ProtocolHandler,
  TopicBroadcast,
  isConnectionGone,
} from '../index';
import {
  GRAPHQL_TRANSPORT_WS_PROTOCOL,
  Message,
  MessageType,
  SubscribePayload,
  isClientMessage,
} from './messages';
import { ApiGwPubSub } from './pubsub';
import {
  DynamoSubscriptionRegistry,
  GqlSubscriptionRecord,
  GqlSubscriptionRegistry,
  InMemorySubscriptionRegistry,
} from './subscription-registry';
import { PUBSUB_CONTEXT_KEY } from './tokens';

export interface GraphQLWsOptions {
  /** The executable schema. A thunk is accepted so it can be read lazily —
   *  `() => host.schema` — for callers wiring up before GraphQLModule has built
   *  it. */
  schema: GraphQLSchema | (() => GraphQLSchema);
  /**
   * Durable "who is subscribed to what". Defaults to the bridge's provider
   * (DynamoDB in aws mode, in-memory in local).
   *
   * PASS ONE EXPLICITLY if you rebuild the bridge while sockets stay open — the
   * local emulator's simulated redeploy does exactly that, and an in-memory
   * registry rebuilt with the bridge would wipe every subscription. In aws mode
   * the default is DynamoDB, which trivially outlives any bridge.
   */
  registry?: GqlSubscriptionRegistry;
  /** The PubSub published on every GraphQL context. Pass your own when the HTTP
   *  path needs the same instance (see apiGwPubSubContext). */
  pubsub?: ApiGwPubSub;
  /** Extra values merged into the GraphQL `context` for every operation on this
   *  socket. Called for the initial `subscribe` AND for every replay, so keep it
   *  derivable from the connectionId alone (durable state, not in-process). */
  context?: (connectionId: string) => Record<string, unknown> | Promise<Record<string, unknown>>;
}

const isAsyncIterable = (value: unknown): value is AsyncIterable<unknown> =>
  typeof (value as any)?.[Symbol.asyncIterator] === 'function';

const formatErrors = (errors: readonly any[]): GraphQLFormattedError[] =>
  errors.map(err =>
    typeof err?.toJSON === 'function' ? err.toJSON() : { message: String(err?.message ?? err) },
  );

/** ExecutionResult -> plain JSON (GraphQLError instances aren't wire-safe). */
function formatResult(result: ExecutionResult): Record<string, unknown> {
  const out: Record<string, unknown> = { data: result.data ?? null };
  if (result.errors?.length) out.errors = formatErrors(result.errors);
  if ((result as any).extensions) out.extensions = (result as any).extensions;
  return out;
}

export class GraphQLWsHandler implements ProtocolHandler {
  readonly name = 'graphql-ws';
  readonly subprotocol = GRAPHQL_TRANSPORT_WS_PROTOCOL;
  /** The PubSub resolvers receive as `@Context('pubsub')`. */
  readonly pubsub: ApiGwPubSub;

  private readonly schema: () => GraphQLSchema;
  private readonly extraContext?: GraphQLWsOptions['context'];
  private _registry?: GqlSubscriptionRegistry;
  private bridge?: GatewayBridge;

  constructor(options: GraphQLWsOptions) {
    const { schema } = options;
    this.schema = typeof schema === 'function' ? schema : () => schema;
    this._registry = options.registry;
    this.pubsub = options.pubsub ?? new ApiGwPubSub();
    this.extraContext = options.context;
  }

  /** Contributes the `topic` kind. Everything else about delivery — paging,
   *  batching, retry, idempotency, ordering — belongs to the bridge's Flusher and
   *  is shared with room broadcasts; this only answers "who wants this topic". */
  readonly fanout: Record<string, FanoutResolver> = {
    topic: (record, cursor) => this.fanoutPage(record, cursor),
  };

  attach(bridge: GatewayBridge) {
    this.bridge = bridge;
    // Default the registry to match the bridge's provider — the one thing this
    // handler can't decide on its own.
    this._registry ??=
      bridge.provider === 'aws'
        ? new DynamoSubscriptionRegistry()
        : new InMemorySubscriptionRegistry();
    this.pubsub.attach(bridge.bus);
  }

  private get registry(): GqlSubscriptionRegistry {
    if (!this._registry) {
      throw new Error(
        'GraphQLWsHandler is not registered on a bridge yet — call bridge.use(handler) before serving frames.',
      );
    }
    return this._registry;
  }

  private get host(): GatewayBridge {
    if (!this.bridge) {
      throw new Error(
        'GraphQLWsHandler is not registered on a bridge yet — call bridge.use(handler).',
      );
    }
    return this.bridge;
  }

  /* ---- inbound ----------------------------------------------------------- */

  /** ProtocolHandler: true means "this frame was mine". */
  async handleFrame(frame: unknown, client: GatewayClient): Promise<boolean> {
    if (!isClientMessage(frame)) return false;
    const message = frame as Message;

    switch (message.type) {
      case MessageType.ConnectionInit:
        // No handshake state to keep: over API Gateway the socket is already
        // established by the time we see this, and there is no second process
        // that could disagree about whether init happened.
        await this.send(client.connectionId, { type: MessageType.ConnectionAck });
        return true;

      case MessageType.Ping:
        await this.send(client.connectionId, {
          type: MessageType.Pong,
          ...((message as any).payload ? { payload: (message as any).payload } : {}),
        });
        return true;

      case MessageType.Pong:
        return true; // reply to our own keepalive; nothing to do

      case MessageType.Complete:
        await this.registry.remove(client.connectionId, (message as any).id);
        return true;

      case MessageType.Subscribe:
        await this.onSubscribe(message as any, client);
        return true;

      default:
        return false;
    }
  }

  /** A dead socket must not leave subscription rows behind — every later publish
   *  would replay them and fan out to nobody. */
  async onDisconnect(connectionId: string) {
    await this.registry.removeAll(connectionId);
  }

  private async onSubscribe(
    message: { id: string; payload: SubscribePayload },
    client: GatewayClient,
  ) {
    const { id, payload } = message;
    const schema = this.schema();

    let document;
    try {
      document = parse(payload.query);
    } catch (err) {
      return this.sendError(client.connectionId, id, [err]);
    }

    const validationErrors = validate(schema, document);
    if (validationErrors.length) {
      return this.sendError(client.connectionId, id, validationErrors);
    }

    const operation = getOperationAST(document, payload.operationName ?? undefined);
    if (!operation) {
      return this.sendError(client.connectionId, id, [
        new GraphQLError('unable to identify the operation to run'),
      ]);
    }

    const args = {
      schema,
      document,
      variableValues: payload.variables ?? undefined,
      operationName: payload.operationName ?? undefined,
      contextValue: await this.context(client.connectionId),
    };

    // Queries and mutations are single-result operations: graphql-ws runs them
    // over the same socket, which is how a client can TRIGGER a subscription
    // without any HTTP endpoint at all.
    if (operation.operation !== 'subscription') {
      const result = await execute(args);
      await this.send(client.connectionId, {
        type: MessageType.Next,
        id,
        payload: formatResult(result),
      });
      await this.send(client.connectionId, { type: MessageType.Complete, id });
      return;
    }

    // A subscription. Run the source stage ONLY to discover which topics the
    // field registered, then discard it — see pubsub.ts (capture mode).
    let topics: string[];
    let stream: unknown;
    try {
      const captured = await this.pubsub.captureTopics(() =>
        createSourceEventStream(args as any),
      );
      stream = captured.result;
      topics = captured.topics;
    } catch (err) {
      return this.sendError(client.connectionId, id, [err]);
    }

    if (!isAsyncIterable(stream)) {
      // createSourceEventStream returns { errors } instead of throwing when the
      // execution context itself is invalid.
      return this.sendError(
        client.connectionId,
        id,
        (stream as any)?.errors ?? [new GraphQLError('subscription failed to start')],
      );
    }
    await (stream as any).return?.();

    if (!topics.length) {
      return this.sendError(client.connectionId, id, [
        new GraphQLError(
          'This subscription did not register a topic with ApiGwPubSub. Over API Gateway a ' +
            'subscription must be backed by a durable topic — return ' +
            'pubsub.asyncIterableIterator(<topic>) from the @Subscription() resolver.',
        ),
      ]);
    }

    await this.registry.add({
      connectionId: client.connectionId,
      subscriptionId: id,
      topics,
      query: payload.query,
      variables: payload.variables ?? null,
      operationName: payload.operationName ?? null,
    });
  }

  /* ---- outbound ---------------------------------------------------------- */

  /** One page of the subscribers awaiting a topic, each as a thunk the flusher
   *  runs after claiming the delivery in the ledger. */
  private async fanoutPage(record: OutboxRecord, cursor?: Record<string, unknown>) {
    const { topic, payload } = record as TopicBroadcast & OutboxRecord;
    const registry = this.registry;
    const page = registry.pageByTopic
      ? await registry.pageByTopic(topic, cursor)
      : { items: await registry.byTopic(topic), cursor: undefined };
    return {
      targets: page.items.map((subscription: GqlSubscriptionRecord) => ({
        connectionId: subscription.connectionId,
        // A single connection can hold several subscriptions on one topic, and
        // each is owed its own frame — so the ledger key is per subscription.
        key: `SUB#${subscription.connectionId}#${subscription.subscriptionId}`,
        send: () => this.deliver(subscription, payload),
      })),
      cursor: page.cursor,
    };
  }

  /** Re-run one stored subscription against a published payload. */
  private async deliver(record: GqlSubscriptionRecord, payload: unknown) {
    let document;
    try {
      document = parse(record.query);
    } catch {
      // The stored document no longer parses (schema/codebase moved on).
      await this.registry.remove(record.connectionId, record.subscriptionId);
      return;
    }

    const args = {
      schema: this.schema(),
      document,
      variableValues: record.variables ?? undefined,
      operationName: record.operationName ?? undefined,
      contextValue: await this.context(record.connectionId),
    };

    // Replay mode: the field's subscribe() sees a stream carrying exactly this
    // payload, so any withFilter() Nest wrapped around it runs for real.
    const stream = await this.pubsub.replayPayload(payload, () =>
      createSourceEventStream(args as any),
    );
    if (!isAsyncIterable(stream)) {
      return this.sendError(
        record.connectionId,
        record.subscriptionId,
        (stream as any)?.errors ?? [new GraphQLError('subscription failed to resume')],
      );
    }

    const iterator = (stream as AsyncIterable<unknown>)[Symbol.asyncIterator]();
    const first = await iterator.next();
    await iterator.return?.();
    // done === the @Subscription({ filter }) rejected this payload for this
    // subscriber. Correct outcome: send nothing.
    if (first.done) return;

    const result = await execute({ ...(args as any), rootValue: first.value });
    // sendRaw, not send: a failure here must reach the Flusher. It releases the
    // ledger claim and lets the stream redeliver the record — swallowing it is
    // exactly the lost-message bug the outbox exists to remove. A 410 surfaces as
    // ConnectionGoneError, which the flusher treats as "reap it", not "retry".
    await this.sendRaw(record.connectionId, {
      type: MessageType.Next,
      id: record.subscriptionId,
      payload: formatResult(result),
    });
  }

  /* ---- plumbing ---------------------------------------------------------- */

  private sendError(connectionId: string, id: string, errors: readonly any[]) {
    return this.send(connectionId, {
      type: MessageType.Error,
      id,
      payload: formatErrors(errors.length ? errors : [new GraphQLError('unknown error')]),
    });
  }

  /** Put a graphql-transport-ws frame on the wire. Propagates everything —
   *  callers decide what a failure means. */
  private async sendRaw(connectionId: string, message: Record<string, unknown>) {
    const publisher = this.host.publisher;
    if (!publisher.toConnectionRaw) {
      throw new Error(
        'The configured RealtimePublisher has no toConnectionRaw(); GraphQL over WebSocket needs it ' +
          'to send `{ type, id, payload }` frames unwrapped.',
      );
    }
    await publisher.toConnectionRaw(connectionId, message);
  }

  /** Tolerant send, for frames produced INLINE while answering a client frame
   *  (ack, pong, error). There is no outbox record behind these and nothing to
   *  retry them, so a dead socket is reaped and the failure ends here. */
  private async send(connectionId: string, message: Record<string, unknown>) {
    try {
      await this.sendRaw(connectionId, message);
    } catch (err) {
      if (!isConnectionGone(err)) throw err;
      await this.host.cleanup(connectionId); // 410 — drops rooms + our rows
    }
  }

  /** Built for the initial `subscribe` AND for every replay, so it must be
   *  derivable from the connectionId alone — there is no request to read.
   *
   *  `pubsub` is published here so resolvers can use the ordinary
   *  `@Context('pubsub')` and stay ignorant of this handler. It goes on last: a
   *  user-supplied key of the same name must not replace the one instance that
   *  actually works on this runtime. */
  private async context(connectionId: string): Promise<Record<string, unknown>> {
    const extra = (await this.extraContext?.(connectionId)) ?? {};
    return {
      connectionId,
      transport: 'apigw-ws',
      ...extra,
      [PUBSUB_CONTEXT_KEY]: this.pubsub,
    };
  }
}

/** graphql-sse-shaped factory: hand it a schema, get a handler to `bridge.use()`. */
export function createGraphQLWsHandler(options: GraphQLWsOptions): GraphQLWsHandler {
  return new GraphQLWsHandler(options);
}

export interface EnableGraphQLSubscriptionsOptions extends Omit<GraphQLWsOptions, 'schema'> {}

/**
 * The three lines above, for the common case: read the schema Nest already
 * built, register the protocol, hand back the PubSub.
 *
 *     const bridge = GatewayBridge.builder().provider('aws').build();
 *     const app = await NestFactory.create(AppModule);
 *     app.useWebSocketAdapter(new ApiGatewayWsAdapter(app, bridge));
 *     await app.init();
 *     enableGraphQLSubscriptions(app, bridge);
 *
 * MUST be called after `app.init()` (or `app.listen()`): GraphQLModule builds the
 * schema during its own init, and this reads it eagerly so a misordered call
 * fails at boot rather than on the first client frame.
 */
export function enableGraphQLSubscriptions(
  app: INestApplication,
  bridge: GatewayBridge,
  options: EnableGraphQLSubscriptionsOptions = {},
): ApiGwPubSub {
  // Required lazily so the core entry point never pulls in @nestjs/graphql.
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const { GraphQLSchemaHost } = require('@nestjs/graphql');

  let host: { schema: GraphQLSchema };
  try {
    // strict:false — we needn't sit in GraphQLModule's module graph.
    host = app.get(GraphQLSchemaHost, { strict: false });
  } catch {
    throw new Error(
      'enableGraphQLSubscriptions(): GraphQLSchemaHost is not available. Import GraphQLModule.forRoot(...) ' +
        'in your application module.',
    );
  }
  // Touch it now: the getter throws while the schema is still unbuilt, and a
  // clear message here beats an obscure failure on the first subscribe frame.
  try {
    void host.schema;
  } catch {
    throw new Error(
      'enableGraphQLSubscriptions(): the GraphQL schema has not been built yet. Call this AFTER ' +
        'app.init() (Lambda) or app.listen() (HTTP).',
    );
  }

  const handler = new GraphQLWsHandler({ schema: () => host.schema, ...options });
  bridge.use(handler);
  return handler.pubsub;
}
