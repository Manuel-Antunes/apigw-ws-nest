/* =============================================================================
 *  AWS API Gateway WebSocket — wire contract
 * =============================================================================
 *  Full-fidelity models of the payloads API Gateway delivers to a Lambda (or an
 *  HTTP integration) and the response it expects back. These are OUR types, but
 *  they are a structural superset of @types/aws-lambda's
 *  `APIGatewayProxyWebsocketEventV2` / `APIGatewayProxyStructuredResultV2`
 *  (AWS's event type omits `headers`/`queryStringParameters`, which $connect
 *  actually delivers — we keep them), so a consumer can swap one for the other.
 * ========================================================================== */

/** The three lifecycle phases API Gateway tags every WebSocket event with. */
export const EVENT_TYPE = {
  CONNECT: 'CONNECT',
  MESSAGE: 'MESSAGE',
  DISCONNECT: 'DISCONNECT',
} as const;
export type ApiGwEventType = (typeof EVENT_TYPE)[keyof typeof EVENT_TYPE];

/** Reserved route keys. Anything else is a custom route (or `$default`). */
export const ROUTE = {
  CONNECT: '$connect',
  DISCONNECT: '$disconnect',
  DEFAULT: '$default',
} as const;

/** The caller identity block API Gateway attaches (subset we care about). */
export interface ApiGwIdentity {
  sourceIp?: string;
  userAgent?: string;
}

/** requestContext — the routing + connection metadata for one WS event. */
export interface ApiGwRequestContext {
  /** `$connect` | `$disconnect` | `$default` | a custom route name. */
  routeKey: string;
  /** Lifecycle phase; the most reliable thing to switch on. */
  eventType: ApiGwEventType;
  connectionId: string;
  /** `<api-id>.execute-api.<region>.amazonaws.com` — used to build the
   *  @connections management endpoint together with `stage`. */
  domainName?: string;
  stage?: string;
  apiId?: string;
  requestId?: string;
  /** epoch millis the socket connected at. */
  connectedAt?: number;
  /** present on MESSAGE events. */
  messageId?: string;
  identity?: ApiGwIdentity;
}

/** The event object a WebSocket route integration receives. */
export interface ApiGwWsEvent {
  requestContext: ApiGwRequestContext;
  /** Raw client frame (a JSON string `{ event, data }`) on MESSAGE events. */
  body?: string;
  /** Present on $connect. */
  headers?: Record<string, string | undefined>;
  queryStringParameters?: Record<string, string | undefined>;
  isBase64Encoded?: boolean;
}

/** What a route integration returns. For `$connect`, a non-2xx rejects the
 *  socket; for `$default`, the contract delivery is via the Management API, so
 *  the body here is informational. */
export interface ApiGwResponse {
  statusCode: number;
  body?: string;
}

/** Parsed inbound frame coming from a client. Structurally identical to NestJS's
 *  WsResponse — gateways type their acks/streams with WsResponse directly. */
export interface ClientFrame {
  event: string;
  data: any;
}
