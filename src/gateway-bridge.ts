/* =============================================================================
 *  GatewayBridge — the synthetic "server", its sockets, and its builder.
 * =============================================================================
 *  The whole point: a NestJS @WebSocketGateway written against the STANDARD
 *  Socket.IO-shaped API (@WebSocketServer server.to(room).emit(), @ConnectedSocket
 *  client.join(room)) runs UNCHANGED over API Gateway, because these synthetic
 *  objects implement that same surface and quietly persist connections/rooms via
 *  the ConnectionStore and deliver via the RealtimePublisher.
 *
 *  THE BRIDGE OWNS ITS WORLD. There are no module-level singletons: the store,
 *  publisher, bus, ledger and flusher are all fields on the instance the builder
 *  produced. That matters beyond tidiness — a hidden singleton meant two bridges
 *  in one process silently shared state, and it made "which store is this?" an
 *  unanswerable question at a call site. Now it is `bridge.store`.
 *
 *  EVERY PROTOCOL IS A PLUG-IN. The `{ event, data }` NestJS protocol and
 *  graphql-transport-ws are the same kind of thing — parse a frame, route it, fan
 *  out — so they register the same way, through bridge.use(). dispatch() has no
 *  special case for either.
 * ========================================================================== */

import { EventEmitter } from "events";
import { Subscription } from "rxjs";
import {
  Broadcast,
  ConnectionStore,
  DeliveryLedger,
  FanoutResolver,
  MessageBus,
  OutboxRecord,
  PageCursor,
  RealtimePublisher,
  isConnectionGone,
} from "./ports";
import {
  ApiGwWsEvent,
  ApiGwResponse,
  ClientFrame,
  EVENT_TYPE,
  ROUTE,
} from "./contract";
import { PROVIDER, Provider } from "./config";
import { runInDispatchScope } from "./dispatch-scope";
import {
  BatchResponse,
  DynamoDeliveryLedger,
  DynamoOutboxBus,
  FlushContext,
  Flusher,
  InMemoryDeliveryLedger,
  InlineMessageBus,
  StreamContext,
  StreamEvent,
  decodeStreamEvent,
} from "./outbox";
import { DynamoConnectionStore, ApiGatewayPublisher } from "./providers/aws";
import { InMemoryConnectionStore, LocalPublisher } from "./providers/local";
import { BaseWsInstance, MessageMappingProperties } from "@nestjs/websockets";

/** One @SubscribeMessage route. (Nest also tags handlers that take an @Ack()
 *  param with isAckHandledManually so the adapter won't double-send a response.) */
export type BoundHandler = MessageMappingProperties & {
  isAckHandledManually?: boolean;
};

/* ---- the synthetic socket ------------------------------------------------ */

/** Synthetic per-connection socket. Mirrors the Socket.IO client API used inside
 *  gateways (connectionId, join/leave, emit) — rooms are persisted by the store,
 *  delivery goes through the publisher. */
export class GatewayClient {
  /** Awaitable frame processor installed by the adapter's bindMessageHandlers.
   *  Resolves only AFTER the handler ran and every response was sent. */
  handleFrame?: (frame: ClientFrame) => Promise<void>;

  /** Routes by event name, MERGED across every @WebSocketGateway bound to this
   *  connection, so all gateways stay reachable (not just the last one bound). */
  readonly handlers = new Map<string, BoundHandler>();

  /** Live rxjs subscriptions opened by streaming handlers (Observable returns). */
  readonly subscriptions: Subscription[] = [];

  constructor(
    readonly connectionId: string,
    private readonly store: ConnectionStore,
    private readonly publisher: RealtimePublisher,
  ) {}

  /** Send a frame back to THIS connection (used by the adapter for acks). */
  send(frame: ClientFrame) {
    return this.publisher.toConnection(this.connectionId, frame.event, frame.data);
  }
  /** Send a payload to THIS connection VERBATIM — no `{ event, data }` envelope.
   *  For protocols that own their wire format (see src/graphql). */
  sendRaw(payload: unknown) {
    if (!this.publisher.toConnectionRaw) {
      return Promise.reject(
        new Error(
          "this RealtimePublisher does not implement toConnectionRaw(); raw-frame protocols (e.g. GraphQL over WebSocket) require it",
        ),
      );
    }
    return this.publisher.toConnectionRaw(this.connectionId, payload);
  }
  /** Socket.IO-style: emit an event to THIS connection. */
  emit(event: string, data: unknown) {
    return this.publisher.toConnection(this.connectionId, event, data);
  }
  /** Socket.IO-style: join / leave a room (persisted by the store). */
  join(room: string) {
    return this.store.join(this.connectionId, room);
  }
  leave(room: string) {
    return this.store.leave(this.connectionId, room);
  }
}

/** The room EVERY `{ event, data }` connection is auto-joined to on $connect. It
 *  is the durable backing for a "global" channel: server.emit(event, data) fans
 *  out to it, so it reaches every client across every instance. A sentinel name
 *  so it can't collide with an application room. */
export const GLOBAL_ROOM = "@@global";

/** EventEmitter's own/internal events. These must keep going to the in-process
 *  listeners (the Nest 'connection' hub, error handling) instead of the wire. */
const RESERVED_EVENTS = new Set([
  "connection",
  "newListener",
  "removeListener",
  "error",
]);

/** Synthetic server handed to @WebSocketServer(). Mirrors Socket.IO's server API
 *  (to(room).emit for a room, emit(...) for a GLOBAL broadcast) and doubles as
 *  the connection hub (an EventEmitter Nest binds 'connection' on).
 *
 *  Both broadcast forms go through the OUTBOX rather than posting to sockets
 *  here. What a gateway awaits is therefore the durable write, not the fan-out:
 *  the emit is recorded, and delivery is retried by the flush consumer until it
 *  succeeds. See src/outbox.ts. */
export class GatewayServer extends EventEmitter implements BaseWsInstance {
  constructor(private readonly bus: () => MessageBus) {
    super();
  }

  close() {
    super.removeAllListeners();
  }

  /** Socket.IO-style room broadcast. Returns an awaitable so handlers can ensure
   *  the message is DURABLY RECORDED before a Lambda freezes. */
  to(room: string) {
    return {
      emit: (event: string, data: unknown): Promise<void> =>
        this.bus().publish({ kind: "room", room, event, data }),
    };
  }

  /** Socket.IO-style GLOBAL broadcast — the `io.emit(event, data)` analog.
   *
   *  Reserved EventEmitter events (notably the internal 'connection' hub Nest
   *  binds on us) are delegated to the base emitter rather than broadcast. */
  emit(event: string | symbol, ...args: any[]): any {
    if (typeof event === "symbol" || RESERVED_EVENTS.has(event)) {
      return super.emit(event as any, ...args);
    }
    return this.bus().publish({
      kind: "room",
      room: GLOBAL_ROOM,
      event,
      data: args[0],
    });
  }
}

/* ---- protocols ----------------------------------------------------------- */

/**
 * A wire protocol plugged into the bridge — the single registration point that
 * used to be three (a frame handler, a disconnect hook, and a fan-out resolver,
 * each added somewhere else).
 *
 * Everything is optional except handleFrame, so a protocol declares only what it
 * actually has.
 */
export interface ProtocolHandler {
  /** For diagnostics and error messages. */
  readonly name?: string;
  /** WebSocket subprotocol this handler answers to. The bridge collects these
   *  and negotiates them at $connect — so a subprotocol is offerable exactly
   *  when a handler exists for it, rather than being hardcoded. */
  readonly subprotocol?: string;
  /** Consulted LAST, after every non-fallback handler declined. The NestJS
   *  `{ event, data }` protocol is the fallback: it has no frame signature of
   *  its own, so it must not pre-empt a protocol that does. */
  readonly fallback?: boolean;
  /** Outbox kinds this protocol delivers. Merged into the bridge's flusher. */
  readonly fanout?: Record<string, FanoutResolver>;
  /** Called once, when registered. The place to capture the bridge (and through
   *  it the store, publisher and bus). */
  attach?(bridge: GatewayBridge): void;
  /** Return true if the frame was yours; false to pass it on. */
  handleFrame(
    frame: unknown,
    client: GatewayClient,
    event: ApiGwWsEvent,
  ): boolean | Promise<boolean>;
  /** $disconnect / 410 Gone — runs BEFORE the store row is dropped, so durable
   *  state belonging to this connection can still be read to clean it up. */
  onDisconnect?(connectionId: string): void | Promise<void>;
}

/* ---- the bridge ---------------------------------------------------------- */

export interface BridgeConfig {
  provider: Provider;
  store: ConnectionStore;
  publisher: RealtimePublisher;
  ledger: DeliveryLedger;
  /** Built lazily because InlineMessageBus needs the bridge that owns it. */
  bus: (bridge: GatewayBridge) => MessageBus;
  subprotocols?: string[];
  concurrency?: number;
  reserveMs?: number;
}

export class GatewayBridge {
  /** Handed to Nest via the adapter's create(); becomes the gateway's
   *  @WebSocketServer() and the 'connection' hub. */
  readonly server: GatewayServer;
  readonly provider: Provider;
  readonly store: ConnectionStore;
  readonly publisher: RealtimePublisher;
  readonly bus: MessageBus;
  readonly flusher: Flusher;

  private readonly clients = new Map<string, GatewayClient>();
  private readonly protocols: ProtocolHandler[] = [];
  private readonly extraSubprotocols: string[];

  /** Use {@link GatewayBridge.builder}. Public so a caller who genuinely wants to
   *  assemble the parts by hand can, but the builder is the supported path. */
  constructor(config: BridgeConfig) {
    this.provider = config.provider;
    this.store = config.store;
    this.publisher = config.publisher;
    this.extraSubprotocols = config.subprotocols ?? [];
    this.bus = config.bus(this);
    this.server = new GatewayServer(() => this.bus);
    this.flusher = new Flusher({
      ledger: config.ledger,
      bus: () => this.bus,
      onConnectionGone: id => this.cleanup(id),
      concurrency: config.concurrency,
      reserveMs: config.reserveMs,
    });
    // `room` is the bridge's OWN fan-out, not a plug-in's: server.to().emit() is
    // built in, so it must work with no protocol registered at all.
    this.flusher.register("room", this.roomFanout);
  }

  static builder(): GatewayBridgeBuilder {
    return new GatewayBridgeBuilder();
  }

  /** Register a protocol: its frames, its disconnect cleanup, its fan-out kinds
   *  and its subprotocol, in one call. */
  use(handler: ProtocolHandler): this {
    this.protocols.push(handler);
    for (const [kind, resolver] of Object.entries(handler.fanout ?? {})) {
      this.flusher.register(kind, resolver);
    }
    handler.attach?.(this);
    return this;
  }

  /** Every subprotocol the bridge can negotiate: whatever the registered
   *  protocols declare, plus any the builder was told about explicitly. */
  get subprotocols(): string[] {
    return [
      ...new Set([
        ...this.protocols.map(p => p.subprotocol).filter((s): s is string => !!s),
        ...this.extraSubprotocols,
      ]),
    ];
  }

  /* ---- inbound: one entry point for every trigger ------------------------- */

  /**
   * Route an event by its shape — the recommended Lambda entry point:
   *
   *     export const handler = (event, context) => bridge.serve(event, context);
   *
   * WHY ONE FUNCTION AND NOT TWO. The obvious layout is a Function per trigger,
   * and it costs you the thing that matters most on Lambda: warm containers. Both
   * halves need the SAME booted Nest app (fan-out re-executes a subscription per
   * subscriber, so it needs the schema and the DI container), and a separate flush
   * Function can only be kept warm by flush traffic — so it pays the ~0.5s Nest
   * boot on a large share of its invocations, right after another container
   * finished the very mutation that produced the record.
   *
   * Pointing every trigger at one Function puts them in one warm pool: the
   * container that just served the mutation is a candidate to serve the stream
   * record moments later, with the app already up.
   *
   * The trade is a shared concurrency pool — a fan-out storm competes with
   * $connect. Reserved concurrency is the lever if that ever bites.
   */
  async serve(event: any, context?: StreamContext): Promise<ApiGwResponse | BatchResponse> {
    if (event?.requestContext) return this.dispatch(event as ApiGwWsEvent);
    if (Array.isArray(event?.Records)) return this.flush(event as StreamEvent, context);
    throw new Error(
      'bridge.serve(): unrecognised event — expected an API Gateway WebSocket event ' +
        '(requestContext) or a DynamoDB Stream event (Records). Call dispatch()/flush() directly ' +
        'if you route them yourself.',
    );
  }

  /** The API Gateway WebSocket entry point. Connection lifecycle (add/remove) is
   *  handled HERE, transparently. */
  async dispatch(event: ApiGwWsEvent): Promise<ApiGwResponse> {
    const { connectionId, routeKey, eventType, domainName, stage } =
      event.requestContext;

    if (this.provider === "aws" && domainName) {
      // @connections endpoint for the publisher (per request).
      process.env.MANAGEMENT_ENDPOINT = `https://${domainName}/${stage}`;
    }

    const isConnect =
      eventType === EVENT_TYPE.CONNECT || routeKey === ROUTE.CONNECT;
    const isDisconnect =
      eventType === EVENT_TYPE.DISCONNECT || routeKey === ROUTE.DISCONNECT;

    try {
      if (isConnect) {
        // Echo the negotiated subprotocol, or the browser aborts the handshake.
        const accepted = this.negotiate(event.headers);
        await this.store.add(connectionId, {
          connectedAt: Date.now(),
          ...(accepted ? { subprotocol: accepted } : {}),
        });
        if (!accepted) {
          // Auto-subscribe to the global channel, persisted in the store — so a
          // later server.emit(...) reaches this connection from ANY instance.
          //
          // ONLY for sockets speaking this library's {event,data} protocol. A
          // client that negotiated a foreign subprotocol (graphql-transport-ws)
          // treats an unrecognised frame as a FATAL protocol violation and closes
          // the connection — so auto-joining it here would mean one server.emit()
          // silently disconnects every GraphQL subscriber.
          await this.store.join(connectionId, GLOBAL_ROOM);
        }
        return accepted
          ? { statusCode: 200, headers: { "Sec-WebSocket-Protocol": accepted } }
          : { statusCode: 200 };
      }
      if (isDisconnect) {
        await this.cleanup(connectionId);
        return { statusCode: 200 };
      }

      // Any other route = a message frame.
      return await runInDispatchScope(async () => {
        const client = this.ensureClient(connectionId);
        const parsed: unknown = event.body
          ? JSON.parse(event.body)
          : { event: routeKey, data: {} };

        // Protocols with a frame signature of their own get first refusal; the
        // fallback (NestJS `{ event, data }`) only sees what nobody claimed.
        for (const protocol of this.orderedProtocols()) {
          if (await protocol.handleFrame(parsed, client, event)) break;
        }
        return { statusCode: 200 };
      });
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error("[dispatch error]", err);
      return { statusCode: 500, body: "dispatch failed" };
    }
  }

  /* ---- inbound: the outbox stream ---------------------------------------- */

  /**
   * The DynamoDB Stream consumer. Export it from your Lambda as a second entry
   * point (`export const flush = (e, c) => bridge.flush(e, c)`).
   *
   * ORDERING. Records are grouped by partition key and each group is walked
   * SEQUENTIALLY, while different topics run concurrently. A stream already
   * guarantees order within a partition key; processing a group in parallel would
   * throw that away, so two publishes to one topic could land reversed.
   *
   * PARTIAL FAILURE. Returning batchItemFailures makes Lambda retry from the
   * first failed record instead of re-running the whole batch. On a failure we
   * stop that group — delivering message N+1 after N failed would reorder it.
   */
  async flush(event: StreamEvent, context?: StreamContext): Promise<BatchResponse> {
    const remainingMs = context?.getRemainingTimeInMillis?.bind(context);
    const batchItemFailures: Array<{ itemIdentifier: string }> = [];
    await Promise.all(
      [...decodeStreamEvent(event).values()].map(async entries => {
        for (const { seq, record } of entries) {
          try {
            await this.flushRecord(record, { remainingMs });
          } catch (err) {
            // eslint-disable-next-line no-console
            console.error("[flush]", record.kind, record.messageId, err);
            batchItemFailures.push({ itemIdentifier: seq });
            return; // preserve order: don't run later messages for this topic
          }
        }
      }),
    );
    return { batchItemFailures };
  }

  /** Deliver a single outbox record now. Also what InlineMessageBus calls in
   *  local mode, which is why the emulator exercises the real delivery path. */
  flushRecord(record: OutboxRecord, ctx: FlushContext = {}): Promise<void> {
    return this.flusher.flush(record, ctx);
  }

  /** Record a broadcast. Equivalent to `server.to(room).emit(...)` for room
   *  kinds, and the way a custom protocol publishes its own kind. */
  publish(message: Broadcast): Promise<void> {
    return this.bus.publish(message);
  }

  /* ---- lifecycle --------------------------------------------------------- */

  /** Lazily materialize a conduit. Works on ANY instance because the durable
   *  truth is in the store — the local map is disposable. */
  ensureClient(connectionId: string): GatewayClient {
    let client = this.clients.get(connectionId);
    if (!client) {
      client = new GatewayClient(connectionId, this.store, this.publisher);
      this.clients.set(connectionId, client);
      this.server.emit("connection", client); // -> Nest calls bindMessageHandlers
    }
    return client;
  }

  async onSendError(client: GatewayClient, err: unknown) {
    if (isConnectionGone(err)) {
      await this.cleanup(client.connectionId); // 410 — the socket is a ghost
    } else {
      // eslint-disable-next-line no-console
      console.error("[send error]", err); // -> DLQ in production
    }
  }

  /** Forget a connection everywhere: protocol state first (those hooks still need
   *  its store rows), then the store row itself, then in-process leftovers. */
  async cleanup(connectionId: string) {
    for (const protocol of this.protocols) {
      try {
        await protocol.onDisconnect?.(connectionId);
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error(`[disconnect: ${protocol.name ?? "protocol"}]`, err);
      }
    }
    await this.store.remove(connectionId); // also drops it from all rooms
    const gone = this.clients.get(connectionId);
    gone?.subscriptions.forEach(s => s.unsubscribe()); // tear down live streams
    this.clients.delete(connectionId);
  }

  /* ---- internals --------------------------------------------------------- */

  private orderedProtocols(): ProtocolHandler[] {
    return [
      ...this.protocols.filter(p => !p.fallback),
      ...this.protocols.filter(p => p.fallback),
    ];
  }

  /** `{ event, data }` room broadcasts: members of the room, each sent the frame. */
  private readonly roomFanout: FanoutResolver = async (record, cursor) => {
    const message = record as { room: string; event: string; data: unknown } & OutboxRecord;
    const page = this.store.pageMembersOf
      ? await this.store.pageMembersOf(message.room, cursor as PageCursor)
      : { items: await this.store.membersOf(message.room), cursor: undefined };
    return {
      targets: page.items.map((connectionId: string) => ({
        connectionId,
        key: `CONN#${connectionId}`,
        send: () =>
          this.publisher.toConnection(connectionId, message.event, message.data),
      })),
      cursor: page.cursor,
    };
  };

  /** Pick the first subprotocol the client offered that we accept. API Gateway
   *  forwards the client's Sec-WebSocket-Protocol header to $connect and sends
   *  whatever we echo back; a browser that offered subprotocols FAILS the
   *  handshake if the server answers with one it never offered. */
  private negotiate(headers: Record<string, string | undefined> = {}): string | undefined {
    const raw = Object.entries(headers).find(
      ([k]) => k.toLowerCase() === "sec-websocket-protocol",
    )?.[1];
    if (!raw) return undefined;
    const accepted = this.subprotocols;
    return raw
      .split(",")
      .map(s => s.trim())
      .filter(Boolean)
      .find(p => accepted.includes(p));
  }
}

/* ---- the builder --------------------------------------------------------- */

const PRESETS: Record<
  Provider,
  {
    store: () => ConnectionStore;
    publisher: (store: ConnectionStore) => RealtimePublisher;
    ledger: () => DeliveryLedger;
    bus: (bridge: GatewayBridge) => MessageBus;
  }
> = {
  local: {
    store: () => new InMemoryConnectionStore(),
    publisher: store => new LocalPublisher(store),
    ledger: () => new InMemoryDeliveryLedger(),
    // No table, no stream: deliver inline, awaited.
    bus: bridge => new InlineMessageBus(record => bridge.flushRecord(record)),
  },
  aws: {
    store: () => new DynamoConnectionStore(),
    publisher: store => new ApiGatewayPublisher(store),
    ledger: () => new DynamoDeliveryLedger(),
    bus: () => new DynamoOutboxBus(),
  },
};

/**
 * Assembles a GatewayBridge.
 *
 *     const bridge = GatewayBridge.builder()
 *       .provider('aws')                  // preset: store + publisher + bus + ledger
 *       .store(new MyConnectionStore())   // ...or override any of them individually
 *       .use(new SomeProtocol())
 *       .build();
 *
 * `provider()` picks a matched SET of four backends; the individual setters win
 * over it regardless of call order, so `.provider('aws').store(mine)` and
 * `.store(mine).provider('aws')` mean the same thing. Anything not set falls back
 * to the preset, resolved at build() time.
 */
export class GatewayBridgeBuilder {
  private _provider: Provider = PROVIDER;
  private _store?: ConnectionStore;
  private _publisher?: RealtimePublisher | ((store: ConnectionStore) => RealtimePublisher);
  private _bus?: MessageBus | ((bridge: GatewayBridge) => MessageBus);
  private _ledger?: DeliveryLedger;
  private _subprotocols: string[] = [];
  private _protocols: ProtocolHandler[] = [];
  private _concurrency?: number;
  private _reserveMs?: number;

  /** Choose a matched set of backends. Defaults to `RT_PROVIDER`, or 'local'. */
  provider(provider: Provider): this {
    this._provider = provider;
    return this;
  }

  store(store: ConnectionStore): this {
    this._store = store;
    return this;
  }

  /** A publisher, or a factory receiving the resolved store (the AWS publisher
   *  needs it to expand a room). */
  publisher(
    publisher: RealtimePublisher | ((store: ConnectionStore) => RealtimePublisher),
  ): this {
    this._publisher = publisher;
    return this;
  }

  /** Where broadcasts are recorded. A factory receives the bridge, so an inline
   *  bus can deliver straight back into it. */
  bus(bus: MessageBus | ((bridge: GatewayBridge) => MessageBus)): this {
    this._bus = bus;
    return this;
  }

  ledger(ledger: DeliveryLedger): this {
    this._ledger = ledger;
    return this;
  }

  /** Register a protocol. Equivalent to calling bridge.use() after build(). */
  use(handler: ProtocolHandler): this {
    this._protocols.push(handler);
    return this;
  }

  /** Extra subprotocols to accept beyond those the registered protocols declare. */
  subprotocols(...subprotocols: string[]): this {
    this._subprotocols.push(...subprotocols);
    return this;
  }

  /** Concurrent sends per fan-out page (default 25). */
  concurrency(n: number): this {
    this._concurrency = n;
    return this;
  }

  /** Requeue the remainder of a fan-out below this much remaining time
   *  (default 15s). */
  reserveMs(ms: number): this {
    this._reserveMs = ms;
    return this;
  }

  build(): GatewayBridge {
    const preset = PRESETS[this._provider];
    if (!preset) {
      throw new Error(
        `unknown provider "${this._provider}" — expected 'aws' or 'local'. Pass custom backends with .store()/.publisher()/.bus()/.ledger() instead.`,
      );
    }
    const store = this._store ?? preset.store();
    const publisher =
      typeof this._publisher === "function"
        ? this._publisher(store)
        : (this._publisher ?? preset.publisher(store));
    const bus = this._bus;

    const bridge = new GatewayBridge({
      provider: this._provider,
      store,
      publisher,
      ledger: this._ledger ?? preset.ledger(),
      bus:
        typeof bus === "function"
          ? bus
          : bus
            ? () => bus
            : b => preset.bus(b),
      subprotocols: this._subprotocols,
      concurrency: this._concurrency,
      reserveMs: this._reserveMs,
    });
    for (const protocol of this._protocols) bridge.use(protocol);
    return bridge;
  }
}
