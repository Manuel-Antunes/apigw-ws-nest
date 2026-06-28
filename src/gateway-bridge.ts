/* =============================================================================
 *  GatewayBridge — the synthetic "server" + per-connection "sockets".
 * =============================================================================
 *  The whole point: a NestJS @WebSocketGateway written against the STANDARD
 *  Socket.IO-shaped API (@WebSocketServer server.to(room).emit(), @ConnectedSocket
 *  client.join(room)) runs UNCHANGED over API Gateway, because these synthetic
 *  objects implement that same surface and quietly persist connections/rooms via
 *  the ConnectionStore and deliver via the RealtimePublisher. Swap the adapter
 *  (e.g. to Socket.IO's IoAdapter) and the gateway code is identical.
 * ========================================================================== */

import { EventEmitter } from "events";
import { Subscription } from "rxjs";
import {
  ConnectionStore,
  RealtimePublisher,
  ConnectionGoneError,
} from "./ports";
import {
  ApiGwWsEvent,
  ApiGwResponse,
  ClientFrame,
  EVENT_TYPE,
  ROUTE,
} from "./contract";
import { PROVIDER } from "./config";
import { flushBroadcasts } from "./broadcast-queue";
import { BaseWsInstance, MessageMappingProperties } from "@nestjs/websockets";

/** One @SubscribeMessage route. (Nest also tags handlers that take an @Ack()
 *  param with isAckHandledManually so the adapter won't double-send a response.) */
export type BoundHandler = MessageMappingProperties & {
  isAckHandledManually?: boolean;
};

/** Synthetic per-connection socket. Mirrors the Socket.IO client API used inside
 *  gateways (connectionId, join/leave, emit) — rooms are persisted by the store,
 *  delivery goes through the publisher. */
export class GatewayClient {
  /** Awaitable frame processor installed by the adapter's bindMessageHandlers.
   *  Resolves only AFTER the handler ran and every response was sent — which is
   *  what lets dispatch() await completion before a Lambda freezes. */
  handleFrame?: (frame: ClientFrame) => Promise<void>;

  /** Routes by event name, MERGED across every @WebSocketGateway bound to this
   *  connection. Each gateway's bindMessageHandlers adds its routes here, so all
   *  gateways stay reachable (not just the last one bound). */
  readonly handlers = new Map<string, BoundHandler>();

  /** Live rxjs subscriptions opened by streaming handlers (Observable returns).
   *  Kept so they can be torn down on disconnect. */
  readonly subscriptions: Subscription[] = [];

  constructor(
    readonly connectionId: string,
    private readonly store: ConnectionStore,
    private readonly publisher: RealtimePublisher,
  ) {}

  /** Send a frame back to THIS connection (used by the adapter for acks). */
  send(frame: ClientFrame) {
    return this.publisher.toConnection(
      this.connectionId,
      frame.event,
      frame.data,
    );
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

/** The room EVERY connection is auto-joined to on $connect (see
 *  GatewayBridge.dispatch). It's the durable backing for a "global" channel:
 *  server.emit(event, data) fans out to it, so it reaches every client across
 *  every instance — no explicit subscribe needed. A sentinel name so it can't
 *  collide with an application room. */
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
 *  the connection hub (an EventEmitter Nest binds 'connection' on). */
export class GatewayServer extends EventEmitter implements BaseWsInstance {
  constructor(private readonly publisher: RealtimePublisher) {
    super();
  }

  close() {
    super.removeAllListeners();
  }

  /** Socket.IO-style room broadcast. Returns an awaitable so handlers can ensure
   *  delivery completes before a Lambda freezes. */
  to(room: string) {
    return {
      emit: (event: string, data: unknown): Promise<void> =>
        this.publisher.toRoom(room, event, data),
    };
  }

  /** Socket.IO-style GLOBAL broadcast — the `io.emit(event, data)` analog. Every
   *  connection is auto-joined to GLOBAL_ROOM on $connect, so this reaches all of
   *  them, across instances, via the store + publisher. Returns an awaitable so a
   *  handler can `await` it before the Lambda freezes.
   *
   *  Reserved EventEmitter events (notably the internal 'connection' hub Nest
   *  binds on us) are delegated to the base emitter rather than broadcast. */
  emit(event: string | symbol, ...args: any[]): any {
    if (typeof event === "symbol" || RESERVED_EVENTS.has(event)) {
      return super.emit(event as any, ...args);
    }
    return this.publisher.toRoom(GLOBAL_ROOM, event, args[0]);
  }
}

export class GatewayBridge {
  /** Handed to Nest via the adapter's create(); becomes the gateway's
   *  @WebSocketServer() and the 'connection' hub. */
  readonly server: GatewayServer;
  private readonly clients = new Map<string, GatewayClient>();

  constructor(
    private readonly store: ConnectionStore,
    private readonly publisher: RealtimePublisher,
  ) {
    this.server = new GatewayServer(publisher);
  }

  /** The single entry point for everything API Gateway sends us. Connection
   *  lifecycle (add/remove) is handled HERE, transparently — gateways never
   *  touch the store for that. */
  async dispatch(event: ApiGwWsEvent): Promise<ApiGwResponse> {
    const { connectionId, routeKey, eventType, domainName, stage } =
      event.requestContext;

    if (PROVIDER === "aws" && domainName) {
      // @connections endpoint for the publisher (per request).
      process.env.MANAGEMENT_ENDPOINT = `https://${domainName}/${stage}`;
    }

    const isConnect =
      eventType === EVENT_TYPE.CONNECT || routeKey === ROUTE.CONNECT;
    const isDisconnect =
      eventType === EVENT_TYPE.DISCONNECT || routeKey === ROUTE.DISCONNECT;

    try {
      if (isConnect) {
        await this.store.add(connectionId, { connectedAt: Date.now() });
        // Auto-subscribe to the global channel, persisted in the store — so a
        // later server.emit(...) reaches this connection from ANY instance, with
        // no explicit subscribe. ($disconnect's store.remove drops it again.)
        await this.store.join(connectionId, GLOBAL_ROOM);
        return { statusCode: 200 };
      }
      if (isDisconnect) {
        await this.store.remove(connectionId); // also drops the connection from all rooms
        const gone = this.clients.get(connectionId);
        gone?.subscriptions.forEach(s => s.unsubscribe()); // tear down live streams
        this.clients.delete(connectionId);
        return { statusCode: 200 };
      }

      // Any other route = a message frame.
      const client = this.ensureClient(connectionId);
      const frame: ClientFrame = event.body
        ? JSON.parse(event.body)
        : { event: routeKey, data: {} };
      // AWAIT full processing (handler + outbound sends) so the Lambda doesn't
      // freeze mid-flight. ensureClient emits 'connection' synchronously, so the
      // adapter has already installed handleFrame by now.
      if (client.handleFrame) await client.handleFrame(frame);
      // Drain any room broadcasts that handler .next()'d before the Lambda freezes.
      await flushBroadcasts();
      return { statusCode: 200 };
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error("[dispatch error]", err);
      return { statusCode: 500, body: "dispatch failed" };
    }
  }

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
    if (err instanceof ConnectionGoneError) {
      await this.store.remove(client.connectionId);
      this.clients.delete(client.connectionId);
    } else {
      // eslint-disable-next-line no-console
      console.error("[send error]", err); // -> DLQ in production
    }
  }
}
