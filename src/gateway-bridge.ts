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
 *
 *  $CONNECT IS A DECISION, THEN A WRITE. With a connect hook registered (the
 *  builder's onConnect, a protocol's onConnect, or the Nest adapter routing to
 *  handleConnection), the hooks run first and nothing reaches the store unless
 *  they all accept. Identity is written to `client.data`, persisted with the
 *  connection, and rehydrated from the store on whichever instance serves the
 *  next frame — so a guard reads the same thing everywhere.
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
  SessionMeta,
  isConnectionGone,
  isConnectionRejected,
} from "./ports";
import {
  ApiGwWsEvent,
  ApiGwResponse,
  ClientFrame,
  EVENT_TYPE,
  ROUTE,
} from "./contract";
import { PROVIDER, Provider } from "./config";
import { enqueueBroadcast, runInDispatchScope } from "./dispatch-scope";
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

/* ---- the handshake ------------------------------------------------------- */

/**
 * What the client presented at $connect — Socket.IO's `socket.handshake`.
 *
 * COMPLETE ONLY DURING THE CONNECT PHASE. Once the connection is accepted it is
 * reduced — on the connecting instance too — to what is persisted with it, so a
 * client looks the same on every instance. A guard reading
 * `handshake.headers.authorization` therefore fails at once in development,
 * instead of passing on the warm container and failing on scale-out. Raw
 * headers are deliberately never persisted: they are the bearer tokens.
 */
export interface Handshake {
  /** Header names lower-cased (as Node and Socket.IO do). Connect phase only. */
  headers: Record<string, string>;
  /** queryStringParameters, undefined values dropped. Connect phase only. */
  query: Record<string, string>;
  /** Every subprotocol the client offered, in order. A bearer smuggled as an
   *  entry is readable here and never echoed (negotiation only picks registered
   *  ones). Connect phase only. */
  subprotocols: string[];
  /** The subprotocol the bridge accepted and echoed, if any. Persisted. */
  subprotocol?: string;
  /** requestContext.identity.sourceIp. Persisted. */
  sourceIp?: string;
  /** requestContext.identity.userAgent. Connect phase only. */
  userAgent?: string;
  /** requestContext.connectedAt, else when $connect was handled. Persisted. */
  connectedAt: number;
  /** requestContext.authorizer, when an API Gateway Lambda authorizer ran. API
   *  Gateway repeats it on every route, so it survives the reduction. */
  authorizer?: Record<string, unknown>;
}

/** Read the handshake off a $connect event. Pure: nothing is written. */
export function handshakeOf(event: ApiGwWsEvent): Handshake {
  const ctx = event.requestContext;
  const headers: Record<string, string> = {};
  for (const [name, values] of Object.entries(event.multiValueHeaders ?? {})) {
    if (values?.length) headers[name.toLowerCase()] = values.join(", ");
  }
  for (const [name, value] of Object.entries(event.headers ?? {})) {
    if (value !== undefined) headers[name.toLowerCase()] = value;
  }
  const query: Record<string, string> = {};
  for (const [name, values] of Object.entries(event.multiValueQueryStringParameters ?? {})) {
    if (values?.length) query[name] = values[values.length - 1];
  }
  for (const [name, value] of Object.entries(event.queryStringParameters ?? {})) {
    if (value !== undefined) query[name] = value;
  }
  return {
    headers,
    query,
    subprotocols: (headers["sec-websocket-protocol"] ?? "")
      .split(",")
      .map(s => s.trim())
      .filter(Boolean),
    sourceIp: ctx.identity?.sourceIp,
    userAgent: ctx.identity?.userAgent ?? headers["user-agent"],
    connectedAt: ctx.connectedAt ?? Date.now(),
    authorizer: ctx.authorizer,
  };
}

/** The handshake every instance sees once $connect is over: only what is
 *  persisted with the connection (plus the authorizer context, which API
 *  Gateway itself repeats on every route). */
function persistedHandshake(from: {
  connectedAt: number;
  subprotocol?: string;
  sourceIp?: string;
  authorizer?: Record<string, unknown>;
}): Handshake {
  return {
    headers: {},
    query: {},
    subprotocols: [],
    connectedAt: from.connectedAt,
    ...(from.subprotocol ? { subprotocol: from.subprotocol } : {}),
    ...(from.sourceIp ? { sourceIp: from.sourceIp } : {}),
    ...(from.authorizer ? { authorizer: from.authorizer } : {}),
  };
}

/* ---- the synthetic socket ------------------------------------------------ */

/** 'connecting' while $connect hooks run, 'refused' once one of them called
 *  disconnect(), 'open' once accepted (and for any client no connect phase
 *  created). */
export type ClientPhase = "connecting" | "refused" | "open";

export interface GatewayClientOptions<TData extends object = Record<string, any>> {
  handshake?: Handshake;
  data?: TData;
  /** Rebuilt from the store rather than created by $connect on this instance. */
  rehydrated?: boolean;
  /** 'connecting' records joins/leaves and refuses sends until accepted. */
  phase?: "connecting" | "open";
  /** What disconnect() does on an open connection. The bridge passes its own
   *  disconnect(), so handleDisconnect and the store cleanup run too; without
   *  it, only the publisher's disconnect() is called. */
  kick?: () => Promise<void>;
}

/** Bookkeeping for a client whose $connect is still being decided. Kept beside
 *  the client, keyed by it, so none of it is public surface: only the client
 *  and the bridge in this module read it. */
interface ConnectAttempt {
  /** join()/leave() calls, applied in order once the connection is accepted —
   *  a refused connection must leave no room rows behind. */
  memberships: Array<{ room: string; join: boolean }>;
  /** disconnect() was called: refuse with 403. */
  refused: boolean;
}
const attempts = new WeakMap<GatewayClient<any>, ConnectAttempt>();

/** Clients whose 'connection' event the bridge is emitting right now. The
 *  adapter's handleConnection shim reads it to tell Nest's fire-and-forget call
 *  apart from any other. */
const announcing = new WeakSet<object>();

/** @internal — true while the bridge is announcing this client to Nest. */
export const isAnnouncing = (client: object): boolean => announcing.has(client);

/** Make a send awaited by the current dispatch even when its caller drops the
 *  promise. Nest's exception filter answers with an un-awaited client.emit(),
 *  and on Lambda an un-awaited send can be frozen mid-flight. The caller still
 *  gets the very same promise. */
function track<T>(promise: Promise<T>): Promise<T> {
  enqueueBroadcast(promise);
  return promise;
}

/** Synthetic per-connection socket. Mirrors the Socket.IO client API used inside
 *  gateways (connectionId, handshake, data, join/leave, emit, disconnect) —
 *  rooms are persisted by the store, delivery goes through the publisher. */
export class GatewayClient<TData extends object = Record<string, any>> {
  /** Awaitable frame processor installed by the adapter's bindMessageHandlers.
   *  Resolves only AFTER the handler ran and every response was sent. */
  handleFrame?: (frame: ClientFrame) => Promise<void>;

  /** Routes by event name, MERGED across every @WebSocketGateway bound to this
   *  connection, so all gateways stay reachable (not just the last one bound). */
  readonly handlers = new Map<string, BoundHandler>();

  /** Live rxjs subscriptions opened by streaming handlers (Observable returns). */
  readonly subscriptions: Subscription[] = [];

  /** Socket.IO's socket.handshake. See {@link Handshake} for what survives
   *  $connect. */
  handshake: Handshake;

  /** Socket.IO's socket.data — where identity lives. Writable while $connect is
   *  being decided; persisted with the connection when it is accepted;
   *  deep-frozen after that on every instance (a write throws), because a change
   *  made here would exist on one container only. `{}` when nothing was set.
   *  Keep ids and claims in it, not secrets: it is stored in clear. */
  data: TData;

  /** true when this object was rebuilt from the store rather than created by
   *  $connect on this instance. */
  readonly rehydrated: boolean;

  private readonly kick?: () => Promise<void>;

  constructor(
    readonly connectionId: string,
    private readonly store: ConnectionStore,
    private readonly publisher: RealtimePublisher,
    options: GatewayClientOptions<TData> = {},
  ) {
    this.handshake = options.handshake ?? persistedHandshake({ connectedAt: Date.now() });
    this.data = options.data ?? ({} as TData);
    this.rehydrated = options.rehydrated ?? false;
    this.kick = options.kick;
    if (options.phase === "connecting") {
      attempts.set(this, { memberships: [], refused: false });
    }
  }

  get phase(): ClientPhase {
    const attempt = attempts.get(this);
    if (!attempt) return "open";
    return attempt.refused ? "refused" : "connecting";
  }

  /** Send a frame back to THIS connection (used by the adapter for acks). */
  send(frame: ClientFrame) {
    this.assertAccepted("send");
    return track(this.publisher.toConnection(this.connectionId, frame.event, frame.data));
  }
  /** Send a payload to THIS connection VERBATIM — no `{ event, data }` envelope.
   *  For protocols that own their wire format (see src/graphql). */
  sendRaw(payload: unknown) {
    this.assertAccepted("sendRaw");
    if (!this.publisher.toConnectionRaw) {
      return Promise.reject(
        new Error(
          "this RealtimePublisher does not implement toConnectionRaw(); raw-frame protocols (e.g. GraphQL over WebSocket) require it",
        ),
      );
    }
    return track(this.publisher.toConnectionRaw(this.connectionId, payload));
  }
  /** Socket.IO-style: emit an event to THIS connection. */
  emit(event: string, data: unknown) {
    this.assertAccepted("emit");
    return track(this.publisher.toConnection(this.connectionId, event, data));
  }
  /** Socket.IO-style: join / leave a room (persisted by the store). During
   *  $connect the call is recorded and applied once the connection is accepted. */
  join(room: string) {
    const attempt = attempts.get(this);
    if (attempt) {
      attempt.memberships.push({ room, join: true });
      return Promise.resolve();
    }
    return this.store.join(this.connectionId, room);
  }
  leave(room: string) {
    const attempt = attempts.get(this);
    if (attempt) {
      attempt.memberships.push({ room, join: false });
      return Promise.resolve();
    }
    return this.store.leave(this.connectionId, room);
  }
  /** Socket.IO's socket.disconnect(true). While $connect is being decided it
   *  refuses the connection (403); afterwards it closes the socket server-side. */
  disconnect(): Promise<void> {
    const attempt = attempts.get(this);
    if (attempt) {
      attempt.refused = true;
      return Promise.resolve();
    }
    if (this.kick) return this.kick();
    if (!this.publisher.disconnect) {
      return Promise.reject(
        new Error("this RealtimePublisher does not implement disconnect(); it cannot close a socket"),
      );
    }
    return this.publisher.disconnect(this.connectionId);
  }

  private assertAccepted(method: string) {
    if (attempts.has(this)) {
      throw new Error(
        `client.${method}() during $connect: API Gateway does not deliver to a connection whose ` +
          `$connect has not completed. Send from a later frame, or broadcast with ` +
          `server.to(room).emit().`,
      );
    }
  }
}

function deepFreeze<T>(value: T): T {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const inner of Object.values(value)) deepFreeze(inner);
  }
  return value;
}

/** Make client.data read-only — deep-frozen, and the property itself not
 *  reassignable — so a write after $connect throws instead of quietly creating
 *  state that exists on one instance only. */
function sealData(client: GatewayClient<any>, data: Record<string, unknown>) {
  Object.defineProperty(client, "data", {
    value: deepFreeze(data),
    enumerable: true,
    writable: false,
    configurable: false,
  });
}

/** client.data as every instance will see it: a JSON round trip (so a Date is an
 *  ISO string on the connecting instance too, not only after rehydration),
 *  bounded in size. Over the bound is a programming error, not a refusal. */
function normaliseData(data: unknown, maxBytes: number): Record<string, unknown> {
  const json = JSON.stringify(data ?? {}) ?? "{}";
  const bytes = Buffer.byteLength(json, "utf8");
  if (bytes > maxBytes) {
    throw new Error(
      `client.data is ${bytes} bytes of JSON, over maxConnectionData (${maxBytes}). Keep ids and ` +
        `claims in it, not documents or tokens.`,
    );
  }
  const value = JSON.parse(json);
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("client.data must be a plain object");
  }
  return value;
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
  /** $connect, before anything is persisted and after the builder's onConnect
   *  hooks. Throw (or call client.disconnect()) to refuse the socket; write
   *  client.data to establish identity. Registering a protocol that has one
   *  requires ConnectionStore.get(). */
  onConnect?(client: GatewayClient, event: ApiGwWsEvent): void | Promise<void>;
  /** $disconnect / 410 Gone / bridge.disconnect() — runs BEFORE the store row is
   *  dropped, so durable state belonging to this connection can still be read
   *  to clean it up. `client` is absent when this instance has none and the
   *  store no longer knows the connection. */
  onDisconnect?(
    connectionId: string,
    client?: GatewayClient,
    reason?: string,
  ): void | Promise<void>;
}

/** A framework-agnostic connect hook: runs at $connect before anything is
 *  persisted. Throw (a ConnectionRejectedError, a Nest HttpException, anything)
 *  or call client.disconnect() to refuse; write client.data to establish
 *  identity. */
export type ConnectHook = (client: GatewayClient, event: ApiGwWsEvent) => void | Promise<void>;

/** How a connect hook's throw becomes a $connect status — by SHAPE, never by
 *  instanceof, so the core needs no Nest import and a duplicated bundle copy of
 *  an exception class still maps. Anything unrecognised fails closed (500). */
function connectStatusOf(err: any): number {
  const isClientError = (s: number) => Number.isInteger(s) && s >= 400 && s <= 499;
  if (isConnectionRejected(err)) {
    const status = Number(err.statusCode);
    return Number.isInteger(status) && status >= 400 && status <= 599 ? status : 500;
  }
  if (typeof err?.getStatus === "function") {
    // A Nest HttpException: UnauthorizedException, ForbiddenException, ...
    const status = Number(err.getStatus());
    return isClientError(status) ? status : 500;
  }
  if (typeof err?.getError === "function") {
    // A Nest WsException: new WsException({ status: 403, message }) picks its
    // status; any other WsException means "not authenticated".
    const inner = err.getError();
    const status = Number(inner?.status ?? inner?.statusCode);
    return isClientError(status) ? status : 401;
  }
  return 500;
}

/** Answered when a connect hook outlives `connectTimeout`. */
const CONNECT_TIMEOUT = Symbol("connect timeout");

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
  /** Bound on the whole $connect hook chain, in ms (default 10s); past it the
   *  connection is refused with 503. */
  connectTimeout?: number;
  /** Ceiling on `client.data`'s UTF-8 JSON size, in bytes (default 16 KiB). */
  maxConnectionData?: number;
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
  /** Rehydrations in flight, so two concurrent frames of one connection (HTTP
   *  mode) cost one store read and one 'connection' emission. */
  private readonly rehydrating = new Map<string, Promise<GatewayClient | undefined>>();
  /** Clients Nest has been told about ('connection' emitted) on this instance. */
  private readonly announced = new WeakSet<GatewayClient>();
  private readonly protocols: ProtocolHandler[] = [];
  private readonly connectHooks: ConnectHook[] = [];
  private readonly extraSubprotocols: string[];
  private readonly connectTimeout: number;
  private readonly maxConnectionData: number;

  /** Use {@link GatewayBridge.builder}. Public so a caller who genuinely wants to
   *  assemble the parts by hand can, but the builder is the supported path. */
  constructor(config: BridgeConfig) {
    this.provider = config.provider;
    this.store = config.store;
    this.publisher = config.publisher;
    this.extraSubprotocols = config.subprotocols ?? [];
    this.connectTimeout = config.connectTimeout ?? 10_000;
    this.maxConnectionData = config.maxConnectionData ?? 16 * 1024;
    this.bus = config.bus(this);
    this.server = new GatewayServer(() => this.bus);
    this.flusher = new Flusher({
      ledger: config.ledger,
      bus: () => this.bus,
      onConnectionGone: id => this.cleanup(id, "gone"),
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

  /** Register a protocol: its frames, its connect/disconnect hooks, its fan-out
   *  kinds and its subprotocol, in one call. */
  use(handler: ProtocolHandler): this {
    if (handler.onConnect) {
      this.assertRehydratable(`bridge.use(${handler.name ?? "protocol"})`);
    }
    this.protocols.push(handler);
    for (const [kind, resolver] of Object.entries(handler.fanout ?? {})) {
      this.flusher.register(kind, resolver);
    }
    handler.attach?.(this);
    return this;
  }

  /** Register a framework-agnostic connect hook. Hooks run at $connect, in
   *  registration order and before every protocol's onConnect. */
  onConnect(hook: ConnectHook): this {
    this.assertRehydratable("onConnect()");
    this.connectHooks.push(hook);
    return this;
  }

  /** true when $connect runs a connect phase — any connect hook is registered,
   *  which includes the Nest adapter's default `lifecycle: 'connect'`. Without
   *  one, $connect accepts unconditionally and frames are served without a
   *  store read. */
  get hasConnectHooks(): boolean {
    return this.connectHooks.length > 0 || this.protocols.some(p => !!p.onConnect);
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
        if (this.hasConnectHooks) return await this.connect(event);
        // No connect hook: accept unconditionally.
        // Echo the negotiated subprotocol, or the browser aborts the handshake.
        const accepted = this.negotiate(handshakeOf(event).subprotocols);
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
        await this.cleanup(
          connectionId,
          event.requestContext.disconnectReason || "disconnect",
        );
        return { statusCode: 200 };
      }

      // Any other route = a message frame.
      return await runInDispatchScope(async () => {
        const client = await this.materialize(connectionId, event);
        if (!client) return this.refuseUnknown(connectionId);
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

  /** Lazily materialize a conduit, without reading the store. Works on ANY
   *  instance because the durable truth is in the store — the local map is
   *  disposable. Kept for compatibility: with connect hooks registered the
   *  MESSAGE path uses {@link materialize}, which rehydrates `client.data`. */
  ensureClient(connectionId: string, event?: ApiGwWsEvent): GatewayClient {
    let client = this.clients.get(connectionId);
    if (!client) {
      client = new GatewayClient(connectionId, this.store, this.publisher, {
        handshake: persistedHandshake({
          connectedAt: event?.requestContext.connectedAt ?? Date.now(),
          sourceIp: event?.requestContext.identity?.sourceIp,
          authorizer: event?.requestContext.authorizer,
        }),
        kick: () => this.disconnect(connectionId),
      });
      this.clients.set(connectionId, client);
    }
    return this.announce(client); // -> Nest calls bindMessageHandlers
  }

  /**
   * The client a frame belongs to: the cached one, else — when connect hooks
   * are registered — one rebuilt from the store, so `client.data` reads the same
   * on every instance. undefined when the store does not know the connection
   * (never accepted, or expired).
   *
   * Costs one strongly-consistent read per (instance, connection), not per
   * frame: `data` cannot change after $connect, so the cached copy can't go
   * stale.
   */
  async materialize(
    connectionId: string,
    event?: ApiGwWsEvent,
  ): Promise<GatewayClient | undefined> {
    if (!this.hasConnectHooks) return this.ensureClient(connectionId, event);
    const cached = this.clients.get(connectionId);
    if (cached) return this.announce(cached);
    let pending = this.rehydrating.get(connectionId);
    if (!pending) {
      pending = this.rehydrate(connectionId, event).finally(() =>
        this.rehydrating.delete(connectionId),
      );
      this.rehydrating.set(connectionId, pending);
    }
    return pending;
  }

  async onSendError(client: GatewayClient, err: unknown) {
    if (isConnectionGone(err)) {
      await this.cleanup(client.connectionId, "gone"); // 410 — the socket is a ghost
    } else {
      // eslint-disable-next-line no-console
      console.error("[send error]", err); // -> DLQ in production
    }
  }

  /** Forget a connection everywhere: protocol state first (those hooks still need
   *  its store rows, and see the client with its data), then the store row
   *  itself, then in-process leftovers. `reason` is the $disconnect reason,
   *  'gone' after a 410, or 'server disconnect' after {@link disconnect}. */
  async cleanup(connectionId: string, reason = "disconnect") {
    const cached = this.clients.get(connectionId);
    let client = cached;
    if (!client && this.hasConnectHooks) {
      try {
        client = await this.restore(connectionId);
      } catch (err) {
        // A disconnect cannot be refused: clean up without the client.
        // eslint-disable-next-line no-console
        console.error("[disconnect: rehydrate]", err);
      }
    }
    for (const protocol of this.protocols) {
      try {
        await protocol.onDisconnect?.(connectionId, client, reason);
      } catch (err) {
        // eslint-disable-next-line no-console
        console.error(`[disconnect: ${protocol.name ?? "protocol"}]`, err);
      }
    }
    await this.store.remove(connectionId); // also drops it from all rooms
    cached?.subscriptions.forEach(s => s.unsubscribe()); // tear down live streams
    this.clients.delete(connectionId);
  }

  /** Close a connection server-side — a kick. Runs the same cleanup as
   *  $disconnect first (protocols' onDisconnect see `client.data`, with reason
   *  'server disconnect'), then has the publisher drop the socket. A
   *  $disconnect that follows finds nothing left to clean. */
  async disconnect(connectionId: string): Promise<void> {
    if (!this.publisher.disconnect) {
      throw new Error(
        "bridge.disconnect(): this RealtimePublisher does not implement disconnect(), so it cannot close a socket",
      );
    }
    await this.cleanup(connectionId, "server disconnect");
    await this.publisher.disconnect(connectionId);
  }

  /* ---- the connect phase ------------------------------------------------- */

  /** $connect with connect hooks registered: decide, then persist — never the
   *  other way round. */
  private async connect(event: ApiGwWsEvent): Promise<ApiGwResponse> {
    const { connectionId } = event.requestContext;
    const handshake = handshakeOf(event);
    // Negotiated BEFORE the hooks: it is a pure function of the headers and the
    // registered protocols, and protocol-specific auth needs to know which
    // protocol the socket will speak. Nothing is written until accept().
    const accepted = this.negotiate(handshake.subprotocols);
    if (accepted) handshake.subprotocol = accepted;
    const client = new GatewayClient(connectionId, this.store, this.publisher, {
      handshake,
      phase: "connecting",
      kick: () => this.disconnect(connectionId),
    });

    const refusal = await runInDispatchScope(() => this.decide(client, event));
    if (refusal) return refusal;

    try {
      await this.accept(client);
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error("[connect: accept]", err);
      // A connection that cannot be recorded whole is not accepted.
      await this.store.remove(connectionId).catch(() => {});
      return { statusCode: 500, body: "connect failed" };
    }
    return accepted
      ? { statusCode: 200, headers: { "Sec-WebSocket-Protocol": accepted } }
      : { statusCode: 200 };
  }

  /** Run every connect hook under `connectTimeout`: the builder's in
   *  registration order, then each protocol's in routing order. The first
   *  throw (or client.disconnect()) stops the chain. Resolves to the refusal
   *  to answer with, or undefined to accept. */
  private async decide(
    client: GatewayClient,
    event: ApiGwWsEvent,
  ): Promise<ApiGwResponse | undefined> {
    let step = "onConnect";
    const chain = (async () => {
      for (const hook of this.connectHooks) {
        step = hook.name || "onConnect";
        await hook(client, event);
        if (client.phase === "refused") return;
      }
      for (const protocol of this.orderedProtocols()) {
        if (!protocol.onConnect) continue;
        step = protocol.name ?? "protocol";
        await protocol.onConnect(client, event);
        if (client.phase === "refused") return;
      }
    })();

    let timer: ReturnType<typeof setTimeout> | undefined;
    const expired = new Promise<typeof CONNECT_TIMEOUT>(resolve => {
      timer = setTimeout(() => resolve(CONNECT_TIMEOUT), this.connectTimeout);
    });
    try {
      if ((await Promise.race([chain, expired])) === CONNECT_TIMEOUT) {
        // eslint-disable-next-line no-console
        console.error(`[connect: ${step}] timed out after ${this.connectTimeout}ms`);
        return { statusCode: 503, body: "connect failed" };
      }
    } catch (err) {
      const statusCode = connectStatusOf(err);
      if (statusCode >= 500) {
        // eslint-disable-next-line no-console
        console.error(`[connect: ${step}]`, err);
        return { statusCode, body: "connect failed" };
      }
      // 4xx is expected traffic: answered, not logged.
      const message = String((err as any)?.message || "connection rejected");
      return { statusCode, body: message.slice(0, 200) };
    } finally {
      clearTimeout(timer);
    }
    return client.phase === "refused"
      ? { statusCode: 403, body: "connection refused" }
      : undefined;
  }

  /** Persist an accepted connection — META with its data, the global room,
   *  then the memberships its hooks recorded, in order — and make the client
   *  look the way every other instance will see it. */
  private async accept(client: GatewayClient): Promise<void> {
    const { connectionId, handshake } = client;
    const data = normaliseData(client.data, this.maxConnectionData);
    const meta: SessionMeta = {
      connectedAt: handshake.connectedAt,
      ...(handshake.subprotocol ? { subprotocol: handshake.subprotocol } : {}),
      ...(handshake.sourceIp ? { sourceIp: handshake.sourceIp } : {}),
      ...(Object.keys(data).length ? { data } : {}),
    };
    await this.store.add(connectionId, meta);
    // Same rule as without hooks: only `{ event, data }` sockets join it.
    if (!handshake.subprotocol) await this.store.join(connectionId, GLOBAL_ROOM);
    for (const { room, join } of attempts.get(client)?.memberships ?? []) {
      await (join
        ? this.store.join(connectionId, room)
        : this.store.leave(connectionId, room));
    }
    attempts.delete(client);
    client.handshake = persistedHandshake({ ...meta, authorizer: handshake.authorizer });
    sealData(client, data);
    // Cached, but Nest hears about it on the first frame, as before.
    this.clients.set(connectionId, client);
  }

  private async rehydrate(
    connectionId: string,
    event?: ApiGwWsEvent,
  ): Promise<GatewayClient | undefined> {
    const client = await this.restore(connectionId, event);
    if (!client) return undefined;
    // An accept on this instance may have cached one while we read.
    const existing = this.clients.get(connectionId);
    if (existing) return this.announce(existing);
    this.clients.set(connectionId, client);
    return this.announce(client);
  }

  /** Rebuild a client from its META row; undefined when there is none. */
  private async restore(
    connectionId: string,
    event?: ApiGwWsEvent,
  ): Promise<GatewayClient | undefined> {
    if (!this.store.get) throw new Error(REHYDRATE_NEEDS_GET);
    const meta = await this.store.get(connectionId);
    if (!meta) return undefined;
    const client = new GatewayClient(connectionId, this.store, this.publisher, {
      handshake: persistedHandshake({ ...meta, authorizer: event?.requestContext.authorizer }),
      rehydrated: true,
      kick: () => this.disconnect(connectionId),
    });
    sealData(client, meta.data ?? {});
    return client;
  }

  /** Tell Nest about a client, once per instance: the 'connection' hub event is
   *  what binds the gateways' @SubscribeMessage handlers to it. */
  private announce(client: GatewayClient): GatewayClient {
    if (!this.announced.has(client)) {
      this.announced.add(client);
      announcing.add(client);
      try {
        this.server.emit("connection", client);
      } finally {
        announcing.delete(client);
      }
    }
    return client;
  }

  /** A frame from a connection no connect phase accepted (or whose row
   *  expired): routed nowhere, answered 403, and closed. */
  private async refuseUnknown(connectionId: string): Promise<ApiGwResponse> {
    // eslint-disable-next-line no-console
    console.warn(`[dispatch] frame from unknown connection ${connectionId} — refused`);
    await this.publisher
      .disconnect?.(connectionId)
      .catch(err => console.error("[disconnect]", err));
    return { statusCode: 403, body: "unknown connection" };
  }

  private assertRehydratable(what: string) {
    if (typeof this.store.get !== "function") {
      throw new Error(`${what}: ${REHYDRATE_NEEDS_GET}`);
    }
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
  private negotiate(offered: string[]): string | undefined {
    const accepted = this.subprotocols;
    return offered.find(p => accepted.includes(p));
  }
}

/** ConnectionStore.get() is required by the type since 3.0; this catches a store
 *  written for 2.x that reaches the bridge anyway (plain JavaScript, a cast) at
 *  boot rather than on the first frame of a cold instance. */
const REHYDRATE_NEEDS_GET =
  "connect hooks need ConnectionStore.get() to rehydrate client.data on other instances, " +
  "and the configured store has none (get() is required since apigw-ws-nest 3.0)";

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
 *       .onConnect(authenticate)          // refuse or identify at $connect
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
  private _connectHooks: ConnectHook[] = [];
  private _concurrency?: number;
  private _reserveMs?: number;
  private _connectTimeout?: number;
  private _maxConnectionData?: number;

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

  /** Register a connect hook. Repeatable; hooks run in registration order,
   *  before every protocol's onConnect. Equivalent to bridge.onConnect(). */
  onConnect(hook: ConnectHook): this {
    this._connectHooks.push(hook);
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

  /** Bound on the whole $connect hook chain (default 10s). A hook stuck on a
   *  dead identity provider is answered 503 well before API Gateway's 29s. */
  connectTimeout(ms: number): this {
    this._connectTimeout = ms;
    return this;
  }

  /** Ceiling on `client.data`'s UTF-8 JSON size (default 16 KiB). A ceiling,
   *  not a target: it is written with every connection. */
  maxConnectionData(bytes: number): this {
    this._maxConnectionData = bytes;
    return this;
  }

  /** Build the bridge — or a subclass of it, for whoever still needs one. */
  build(): GatewayBridge;
  build<B extends GatewayBridge>(ctor: new (config: BridgeConfig) => B): B;
  build(ctor: new (config: BridgeConfig) => GatewayBridge = GatewayBridge): GatewayBridge {
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

    const bridge = new ctor({
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
      connectTimeout: this._connectTimeout,
      maxConnectionData: this._maxConnectionData,
    });
    for (const protocol of this._protocols) bridge.use(protocol);
    for (const hook of this._connectHooks) bridge.onConnect(hook);
    return bridge;
  }
}
