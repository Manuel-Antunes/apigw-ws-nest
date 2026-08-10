/* =============================================================================
 *  Ports (the portability seam) — interfaces, not transports
 * =============================================================================
 *  These are YOUR concepts, not the transport's, so they survive a switch from
 *  API Gateway to Socket.IO/ws, or from DynamoDB to anything else.
 * ========================================================================== */

export interface SessionMeta {
  userId?: string;
  connectedAt: number;
  /** The WebSocket subprotocol negotiated at $connect, if any (e.g.
   *  `graphql-transport-ws`). Absent means the socket speaks this library's own
   *  `{ event, data }` frames. */
  subprotocol?: string;
}

/** Connection registry + interest map ("rooms"). The transport adapter drives
 *  this on the gateway's behalf — gateways call client.join()/server.to(), never
 *  this directly. */
export interface ConnectionStore {
  add(connectionId: string, meta: SessionMeta): Promise<void>;
  remove(connectionId: string): Promise<void>;
  join(connectionId: string, room: string): Promise<void>;
  leave(connectionId: string, room: string): Promise<void>;
  membersOf(room: string): Promise<string[]>;
}

/** Outbound port. aws -> @connections Management API; local -> in-memory.
 *
 *  `toConnection`/`toRoom` speak this library's `{ event, data }` frame shape.
 *  `toConnectionRaw` is the escape hatch for transports that own their own wire
 *  format — the GraphQL-over-WebSocket layer sends `{ type, id, payload }`
 *  messages that must NOT be wrapped. It's optional so existing custom
 *  publishers keep compiling; the GraphQL transport requires it. */
export interface RealtimePublisher {
  toConnection(connectionId: string, event: string, data: unknown): Promise<void>;
  toRoom(room: string, event: string, data: unknown): Promise<void>;
  /** Send an arbitrary JSON payload verbatim — no `{ event, data }` envelope. */
  toConnectionRaw?(connectionId: string, payload: unknown): Promise<void>;
}

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
