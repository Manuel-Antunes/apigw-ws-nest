/* =============================================================================
 *  Ports (the portability seam) — interfaces, not transports
 * =============================================================================
 *  These are YOUR concepts, not the transport's, so they survive a switch from
 *  API Gateway to Socket.IO/ws, or from DynamoDB to anything else.
 * ========================================================================== */

export interface SessionMeta {
  userId?: string;
  connectedAt: number;
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

/** Outbound port. aws -> @connections Management API; local -> in-memory. */
export interface RealtimePublisher {
  toConnection(connectionId: string, event: string, data: unknown): Promise<void>;
  toRoom(room: string, event: string, data: unknown): Promise<void>;
}

/** Marker error so the publisher can signal a dead connection (HTTP 410). */
export class ConnectionGoneError extends Error {
  constructor(readonly connectionId: string) {
    super(`connection gone: ${connectionId}`);
  }
}
