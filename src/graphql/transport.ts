/* =============================================================================
 *  GraphQLWsTransport — the graphql-ws server loop, without a server.
 * =============================================================================
 *  Not a Nest provider and not a module: a plain object that attaches itself to
 *  a GatewayBridge the first time an ApiGwPubSub is constructed (see pubsub.ts).
 *  That's the whole wiring story — you add ApiGwPubSub to a `providers` array
 *  and subscriptions start working.
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
 *  The only thing that survives between frames is the registry row in DynamoDB —
 *  which is exactly what lets a subscription outlive a redeploy, a cold start, or
 *  being served by a different container than the one that created it.
 *
 *  See pubsub.ts for how capture/replay recovers the topics and re-applies
 *  @Subscription({ filter }) without ever parking an iterator.
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

import { GatewayBridge, GatewayClient, isConnectionGone } from '../index';
import { Message, MessageType, SubscribePayload, isClientMessage } from './protocol';
import { apiGwPubSub, captureTopics, replayPayload } from './pubsub';
import { GqlSubscriptionRecord, GqlSubscriptionRegistry } from './subscription-registry';
import { PUBSUB_CONTEXT_KEY } from './tokens';

/** Optional knobs, supplied by providing GQL_TRANSPORT_OPTIONS. */
export interface ApiGwGraphQLOptions {
  /** Extra values merged into the GraphQL `context` for every operation on this
   *  socket. Called for the initial `subscribe` AND for every replay, so keep it
   *  derivable from the connectionId alone (durable state, not in-process). */
  context?: (connectionId: string) => Record<string, unknown> | Promise<Record<string, unknown>>;
}

export interface TransportDeps {
  /** Read lazily: GraphQLModule builds the schema during its own onModuleInit. */
  schema: () => GraphQLSchema;
  registry: GqlSubscriptionRegistry;
  options?: ApiGwGraphQLOptions;
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

export class GraphQLWsTransport {
  /** One transport per bridge. Several modules may each provide ApiGwPubSub, and
   *  the local emulator builds a NEW bridge on every simulated redeploy — keying
   *  on the bridge gets both cases right, with exactly one frame handler each. */
  private static readonly attached = new WeakMap<GatewayBridge, GraphQLWsTransport>();

  static attach(bridge: GatewayBridge, deps: TransportDeps): GraphQLWsTransport {
    let transport = GraphQLWsTransport.attached.get(bridge);
    if (!transport) {
      transport = new GraphQLWsTransport(bridge, deps);
      GraphQLWsTransport.attached.set(bridge, transport);
      transport.install();
    }
    return transport;
  }

  private constructor(
    private readonly bridge: GatewayBridge,
    private readonly deps: TransportDeps,
  ) {}

  private install() {
    // Claim graphql-ws frames before the {event,data} routing.
    this.bridge.useFrameHandler((frame, client) => this.handle(frame, client));
    // A dead socket must not leave subscription rows behind — every later
    // publish would replay them and fan out to nobody.
    this.bridge.onDisconnect(connectionId => this.deps.registry.removeAll(connectionId));
  }

  /* ---- inbound ----------------------------------------------------------- */

  /** FrameHandler: true means "this frame was mine". */
  private async handle(frame: unknown, client: GatewayClient): Promise<boolean> {
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
        await this.deps.registry.remove(client.connectionId, (message as any).id);
        return true;

      case MessageType.Subscribe:
        await this.onSubscribe(message as any, client);
        return true;

      default:
        return false;
    }
  }

  private async onSubscribe(
    message: { id: string; payload: SubscribePayload },
    client: GatewayClient,
  ) {
    const { id, payload } = message;
    const schema = this.deps.schema();

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
      const captured = await captureTopics(() => createSourceEventStream(args as any));
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

    await this.deps.registry.add({
      connectionId: client.connectionId,
      subscriptionId: id,
      topics,
      query: payload.query,
      variables: payload.variables ?? null,
      operationName: payload.operationName ?? null,
    });
  }

  /* ---- outbound ---------------------------------------------------------- */

  /** ApiGwPubSub.publish -> every stored subscription on `topic`, anywhere. */
  async fanOut(topic: string, payload: unknown) {
    const records = await this.deps.registry.byTopic(topic);
    if (!records.length) return;
    // NOTE: same fan-out cost caveat as server.emit() — one @connections call per
    // subscriber. Past a threshold, hand the topic to SQS and parallelize there.
    const settled = await Promise.allSettled(
      records.map(record => this.deliver(record, payload)),
    );
    for (const outcome of settled) {
      if (outcome.status === 'rejected') {
        // eslint-disable-next-line no-console
        console.error('[graphql fan-out]', outcome.reason);
      }
    }
  }

  /** Re-run one stored subscription against a published payload. */
  private async deliver(record: GqlSubscriptionRecord, payload: unknown) {
    let document;
    try {
      document = parse(record.query);
    } catch {
      // The stored document no longer parses (schema/codebase moved on).
      await this.deps.registry.remove(record.connectionId, record.subscriptionId);
      return;
    }

    const args = {
      schema: this.deps.schema(),
      document,
      variableValues: record.variables ?? undefined,
      operationName: record.operationName ?? undefined,
      contextValue: await this.context(record.connectionId),
    };

    // Replay mode: the field's subscribe() sees a stream carrying exactly this
    // payload, so any withFilter() Nest wrapped around it runs for real.
    const stream = await replayPayload(payload, () => createSourceEventStream(args as any));
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
    await this.send(record.connectionId, {
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

  private async send(connectionId: string, message: Record<string, unknown>) {
    const raw = this.bridge.publisher.toConnectionRaw;
    if (!raw) {
      throw new Error(
        'The configured RealtimePublisher has no toConnectionRaw(); GraphQL over WebSocket needs it ' +
          'to send `{ type, id, payload }` frames unwrapped.',
      );
    }
    try {
      await raw.call(this.bridge.publisher, connectionId, message);
    } catch (err) {
      if (!isConnectionGone(err)) throw err;
      await this.bridge.cleanup(connectionId); // 410 — drops rooms + our rows
    }
  }

  /** Built for the initial `subscribe` AND for every replay, so it must be
   *  derivable from the connectionId alone — there is no request to read.
   *
   *  `pubsub` is published here so resolvers can use the ordinary
   *  `@Context('pubsub')` and stay ignorant of this transport (see context.ts).
   *  It goes on last: a user-supplied key of the same name must not replace the
   *  one instance that actually works on this runtime. */
  private async context(connectionId: string): Promise<Record<string, unknown>> {
    const extra = (await this.deps.options?.context?.(connectionId)) ?? {};
    return {
      connectionId,
      transport: 'apigw-ws',
      ...extra,
      [PUBSUB_CONTEXT_KEY]: apiGwPubSub(),
    };
  }
}
