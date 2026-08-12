/* =============================================================================
 *  Ports — every interface you can swap, in one place.
 * =============================================================================
 *  These are YOUR concepts, not the transport's, so they survive a switch from
 *  API Gateway to Socket.IO/ws, or from DynamoDB to anything else. Nothing here
 *  imports anything: it is the vocabulary the rest of the library is written in.
 *
 *  Four backends are swappable, and the builder takes each individually:
 *
 *    ConnectionStore    who is connected, and which rooms they are in
 *    RealtimePublisher  how a frame reaches a socket
 *    MessageBus         where a broadcast is recorded before delivery
 *    DeliveryLedger     which (message, subscriber) pairs are already served
 * ========================================================================== */

export interface SessionMeta {
  userId?: string;
  connectedAt: number;
  /** The WebSocket subprotocol negotiated at $connect, if any (e.g.
   *  `graphql-transport-ws`). Absent means the socket speaks this library's own
   *  `{ event, data }` frames. */
  subprotocol?: string;
}

/** Opaque "resume from here" token for a paged read. Storage-specific (a
 *  DynamoDB LastEvaluatedKey today); callers only ever pass it back. */
export type PageCursor = Record<string, unknown>;

export interface Page<T> {
  items: T[];
  /** Absent = this was the last page. */
  cursor?: PageCursor;
}

/** Connection registry + interest map ("rooms"). The bridge drives this on the
 *  gateway's behalf — gateways call client.join()/server.to(), never this. */
export interface ConnectionStore {
  add(connectionId: string, meta: SessionMeta): Promise<void>;
  remove(connectionId: string): Promise<void>;
  join(connectionId: string, room: string): Promise<void>;
  leave(connectionId: string, room: string): Promise<void>;
  membersOf(room: string): Promise<string[]>;
  /** Paged form, used by the fan-out path so one enormous room can't be pulled
   *  into a single invocation's memory. Optional: a store that omits it falls
   *  back to membersOf(). */
  pageMembersOf?(room: string, cursor?: PageCursor): Promise<Page<string>>;
}

/** Outbound port. aws -> @connections Management API; local -> in-memory.
 *
 *  `toConnection` speaks this library's `{ event, data }` frame shape.
 *  `toConnectionRaw` is the escape hatch for protocols that own their own wire
 *  format — GraphQL-over-WebSocket sends `{ type, id, payload }` messages that
 *  must NOT be wrapped. It's optional so existing custom publishers keep
 *  compiling; the GraphQL protocol requires it. */
export interface RealtimePublisher {
  toConnection(connectionId: string, event: string, data: unknown): Promise<void>;
  /** Send an arbitrary JSON payload verbatim — no `{ event, data }` envelope. */
  toConnectionRaw?(connectionId: string, payload: unknown): Promise<void>;
  /** IMMEDIATE, best-effort room fan-out. NOT the path `server.to(room).emit()`
   *  takes — that records the broadcast so delivery is retried. A primitive for
   *  callers that have already decided they want neither. */
  toRoom?(room: string, event: string, data: unknown): Promise<void>;
}

/* ---- broadcasts ---------------------------------------------------------- */

/** `server.to(room).emit(event, data)` — this library's `{ event, data }` frames. */
export interface RoomBroadcast {
  kind: 'room';
  room: string;
  event: string;
  data: unknown;
}

/** `pubsub.publish(topic, payload)` — a GraphQL subscription topic. */
export interface TopicBroadcast {
  kind: 'topic';
  topic: string;
  payload: unknown;
}

/** Anything publishable. `kind` is the discriminator the flusher dispatches on,
 *  which is what keeps the outbox agnostic: a protocol adds a kind plus a
 *  FanoutResolver, and inherits retry, ordering and idempotency for free. */
export type Broadcast =
  | RoomBroadcast
  | TopicBroadcast
  | { kind: string; [key: string]: unknown };

export type OutboxRecord = Broadcast & {
  /** Stable across retries AND across continuations — it is the dedupe identity,
   *  so it must NOT be regenerated when a partially-flushed message is requeued. */
  messageId: string;
  publishedAt: number;
  /** The @connections endpoint captured at publish time. A stream event carries
   *  no requestContext, so without this the flush would have no idea where to
   *  post; carrying it makes the record self-describing. */
  endpoint?: string;
  /** Set only on a continuation: resume paging subscribers from here. */
  cursor?: PageCursor;
};

/** Where a broadcast is recorded. aws writes one row and lets a DynamoDB Stream
 *  deliver it; local delivers inline, awaited. */
export interface MessageBus {
  publish(message: Broadcast | OutboxRecord): Promise<void>;
}

/**
 * Records that (message, subscriber) was served, so a delivery retry doesn't
 * send it twice.
 *
 * claim() is the whole contract: it must be an ATOMIC test-and-set, because two
 * concurrent retries would otherwise both read "not sent" and both send.
 */
export interface DeliveryLedger {
  /** true = you own this delivery, send it. false = someone already did. */
  claim(messageId: string, key: string): Promise<boolean>;
  /** Hand the claim back so a retry re-sends (the send failed). */
  release(messageId: string, key: string): Promise<void>;
}

/** One subscriber to serve. `send` is a thunk so the flusher can claim first. */
export interface DeliveryTarget {
  connectionId: string;
  /** Identity WITHIN this message, for the ledger. One connection can hold
   *  several subscriptions on a topic, and each is owed its own frame. */
  key: string;
  send(): Promise<void>;
}

export interface FanoutPage {
  targets: DeliveryTarget[];
  /** Absent = no more subscribers. */
  cursor?: PageCursor;
}

/** Resolves a record into subscribers, one page at a time. Contributed by a
 *  ProtocolHandler through its `fanout` map. */
export type FanoutResolver = (
  record: OutboxRecord,
  cursor?: PageCursor,
) => Promise<FanoutPage>;

/* ---- errors -------------------------------------------------------------- */

/** Marker error so the publisher can signal a dead connection (HTTP 410).
 *  `name` is set explicitly so callers can identify it without `instanceof`
 *  (which fails across duplicated bundle copies of this module). */
export class ConnectionGoneError extends Error {
  readonly name = 'ConnectionGoneError';
  constructor(readonly connectionId: string) {
    super(`connection gone: ${connectionId}`);
  }
}

/** True for a ConnectionGoneError from ANY copy of this module. */
export const isConnectionGone = (err: unknown): boolean =>
  !!err && (err as any).name === 'ConnectionGoneError';
