/* =============================================================================
 *  ApiGwPubSub — a PubSub that never holds a subscriber in memory.
 * =============================================================================
 *  A plain object: no decorators, no DI, no Nest lifecycle. It is created and
 *  wired by enableGraphQLSubscriptions() at the composition root, next to
 *  createGatewayBridge() — see enable.ts. Resolvers receive it on the GraphQL
 *  context (@Context('pubsub')) and never name it.
 *
 *  THE PROBLEM IT SOLVES. `graphql-subscriptions`' PubSub hands the subscription
 *  field an AsyncIterator and parks it in memory until a publish() pushes into
 *  it. On Lambda that iterator dies the moment the invocation returns, and the
 *  next frame may land on a different container — in-process pub/sub simply
 *  cannot cross instances (see the README's cross-instance rule). A subscriber
 *  that "works" locally is silently dropped in production.
 *
 *  THE FIX. Never park anything. This PubSub is used in two one-shot modes, both
 *  driven from the transport through an AsyncLocalStorage so concurrent
 *  dispatches can't see each other's mode:
 *
 *   1. CAPTURE (on `subscribe`) — subscribe(topics) records the topic names and
 *      returns an iterable that never yields. The transport runs graphql's
 *      createSourceEventStream() purely to LEARN the topics, then persists
 *      {connectionId, subscriptionId, topics, query, variables} in the
 *      GqlSubscriptionRegistry (DynamoDB) and throws the stream away.
 *
 *   2. REPLAY (on `publish`) — subscribe(topics) returns an iterable yielding
 *      exactly ONE value: the published payload. The transport re-runs
 *      createSourceEventStream() per stored subscription, so the very same
 *      graphql machinery re-applies — crucially including `withFilter`, which
 *      Nest wraps around the field's subscribe when @Subscription({ filter }) is
 *      used. A filtered-out payload simply reports `done` and no frame is sent.
 *      Then execute() with the payload as rootValue runs the field's `resolve`
 *      and the client's selection set.
 *
 *  So the durable state is the registry row, not a live iterator — a redeploy, a
 *  scale-out, or a cold start changes nothing.
 * ========================================================================== */

import { AsyncLocalStorage } from "async_hooks";
import { GraphQLWsTransport } from "./transport";
import { PubSub } from "graphql-subscriptions";
import { PubSubAsyncIterableIterator } from "graphql-subscriptions/dist/pubsub-async-iterable-iterator";
type Mode =
  | { kind: "capture"; topics: string[] }
  | { kind: "replay"; payload: unknown };

const modeStore = new AsyncLocalStorage<Mode>();

/** Run `fn` with the pubsub in capture mode; returns the topics it registered. */
export async function captureTopics<T>(
  fn: () => Promise<T>,
): Promise<{ result: T; topics: string[] }> {
  const mode: Mode = { kind: "capture", topics: [] };
  const result = await modeStore.run(mode, fn);
  return { result, topics: mode.topics };
}

/** Run `fn` with the pubsub replaying a single published payload. */
export function replayPayload<T>(
  payload: unknown,
  fn: () => Promise<T>,
): Promise<T> {
  return modeStore.run({ kind: "replay", payload }, fn);
}

/** Yields nothing, ever. Enough to satisfy createSourceEventStream's
 *  "must return an AsyncIterable" assertion while we only want the topics. */
async function* inert(): AsyncIterator<never> {
  /* intentionally empty */
}

/** Yields the published payload once, then completes. */
async function* once<T>(value: T): AsyncIterator<T> {
  yield value;
}

/**
 * A `graphql-subscriptions` PubSub whose subscriptions live in DynamoDB instead
 * of in the process. Extending the real class — rather than merely matching its
 * shape — is what lets a resolver be written against the STANDARD contract and
 * stay ignorant of this runtime:
 *
 *     import { PubSub } from 'graphql-subscriptions';
 *
 *     @Subscription(() => Post, { resolve: (p) => p.post })
 *     postAdded(@Context('pubsub') pubsub: PubSub) {
 *       return pubsub.asyncIterableIterator('POST_ADDED');
 *     }
 *
 *     await pubsub.publish('POST_ADDED', { post });
 *
 * Nothing in that resolver names this class, so the same file runs against the
 * in-process PubSub on a long-lived server. `asyncIterator` (the v2 spelling) is
 * kept as an alias.
 *
 * Only `subscribe(trigger, onMessage)` — the callback form — cannot be honoured;
 * see the override.
 */
export class ApiGwPubSub<
  Events extends {
    [event: string]: unknown;
  } = Record<string, never>,
> extends PubSub<Events> {
  private transport?: GraphQLWsTransport;

  /** @internal — enableGraphQLSubscriptions() binds (and re-binds) the transport.
   *  Re-binding matters: the local emulator builds a NEW bridge on every
   *  simulated redeploy, and this instance must follow it. */
  bindTransport(transport: GraphQLWsTransport) {
    this.transport = transport;
  }

  /**
   * The ONE part of the PubSub contract that cannot be honoured here.
   *
   * `subscribe(trigger, onMessage)` registers an in-process callback and returns
   * a numeric id — and that callback would live and die inside a single Lambda
   * invocation, which is the entire failure mode this class exists to prevent.
   * Throwing beats returning an id for a subscription that will never fire.
   *
   * Called with a single argument it is instead the graphql-yoga /
   * current-docs spelling of "give me the stream", so it forwards to
   * asyncIterableIterator rather than punishing a reasonable guess.
   */
  override subscribe<T extends keyof Events>(
    triggers: T & string,
    onMessage: (...args: any[]) => void,
  ): Promise<number> {
    if (typeof onMessage === "function") {
      throw new Error(
        "ApiGwPubSub does not support PubSubEngine's subscribe(trigger, onMessage): an in-process " +
          "callback cannot survive the invocation, let alone reach a subscriber on another instance. " +
          "Return pubsub.subscribe(topic) from a @Subscription() resolver instead.",
      );
    }
    return this.asyncIterableIterator<T>(
      triggers,
    ) as unknown as Promise<number>;
  }

  /**
   * Called by a @Subscription() resolver. Returns a one-shot iterable whose
   * meaning depends on the ambient mode — see the module header.
   */
  override asyncIterableIterator<T>(
    triggers: string | readonly string[],
  ): PubSubAsyncIterableIterator<T> {
    const topics = typeof triggers === "string" ? [triggers] : [...triggers];
    const mode = modeStore.getStore();
    if (!mode) {
      // Called outside a dispatch — e.g. from the HTTP GraphQL endpoint's
      // subscription path. There is no socket to attach to, so failing loudly
      // beats returning a stream that never fires.
      throw new Error(
        "ApiGwPubSub.subscribe() was called outside a WebSocket subscription dispatch. " +
          "Subscriptions must arrive over the API Gateway socket (graphql-transport-ws).",
      );
    }
    if (mode.kind === "capture") {
      mode.topics.push(...topics);
      return inert() as PubSubAsyncIterableIterator<T>;
    }
    return once(mode.payload) as PubSubAsyncIterableIterator<T>;
  }

  /** graphql-subscriptions v2 spelling. */
  asyncIterator<T = unknown>(
    triggers: string | readonly string[],
  ): AsyncIterableIterator<T> {
    return this.asyncIterableIterator<T>(triggers);
  }

  /**
   * Fan a payload out to every connection subscribed to `topic`, on every
   * instance. Await it in your mutation: on Lambda the container freezes the
   * moment the handler returns.
   */
  async publish(topic: string, payload: unknown): Promise<void> {
    if (!this.transport) {
      throw new Error(
        "ApiGwPubSub is not wired to a transport. Call enableGraphQLSubscriptions(app, bridge) " +
          "after app.init(), where you build the bridge.",
      );
    }
    await this.transport.fanOut(topic, payload);
  }

  /* PubSubEngine surface. There is no in-process subscriber list to cancel —
   * subscriptions live in the registry, keyed by connection, and a client ends
   * one by sending `complete` (or disconnecting). */
  unsubscribe(): void {
    /* no in-process subscription to cancel */
  }
}

/** The one instance, published into the GraphQL context. */
let singleton: ApiGwPubSub | undefined;

/** @internal — created on the first enableGraphQLSubscriptions() call. */
export function ensureApiGwPubSub(): ApiGwPubSub {
  return (singleton ??= new ApiGwPubSub());
}

/**
 * The PubSub handed to resolvers on the GraphQL context. Exported for services
 * that publish outside a resolver; resolvers should read `@Context('pubsub')`
 * and stay ignorant of this module.
 */
export function apiGwPubSub(): ApiGwPubSub {
  if (!singleton) {
    throw new Error(
      "GraphQL subscriptions are not enabled. Call enableGraphQLSubscriptions(app, bridge) after " +
        "app.init(), next to createGatewayBridge().",
    );
  }
  return singleton;
}
