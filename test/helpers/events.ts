/* Synthetic API Gateway WebSocket events, shaped like the real ones. */

import type { ApiGwRequestContext, ApiGwWsEvent } from '../../src';

export type EventKind = 'CONNECT' | 'MESSAGE' | 'DISCONNECT';

const ROUTE_OF: Record<EventKind, string> = {
  CONNECT: '$connect',
  MESSAGE: '$default',
  DISCONNECT: '$disconnect',
};

export type EventExtra = Partial<Omit<ApiGwWsEvent, 'requestContext'>> & {
  requestContext?: Partial<ApiGwRequestContext>;
};

let seq = 0;
/** A connection id unique within the test run. */
export const nextId = (prefix = 'conn') => `${prefix}-${++seq}`;

export const CONNECTED_AT = 1_700_000_000_000;
export const SOURCE_IP = '203.0.113.7';

export function wsEvent(
  connectionId: string,
  eventType: EventKind,
  { requestContext, ...extra }: EventExtra = {},
): ApiGwWsEvent {
  return {
    requestContext: {
      connectionId,
      eventType,
      routeKey: ROUTE_OF[eventType],
      stage: 'test',
      connectedAt: CONNECTED_AT,
      identity: { sourceIp: SOURCE_IP, userAgent: 'test-agent' },
      ...requestContext,
    },
    ...extra,
  };
}

export const connectEvent = (id: string, extra?: EventExtra) => wsEvent(id, 'CONNECT', extra);

export const frameEvent = (id: string, body: unknown, extra?: EventExtra) =>
  wsEvent(id, 'MESSAGE', { body: JSON.stringify(body), ...extra });

export const disconnectEvent = (id: string, reason?: string) =>
  wsEvent(id, 'DISCONNECT', reason ? { requestContext: { disconnectReason: reason } } : {});
