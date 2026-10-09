/* =============================================================================
 *  Protocol layer — the `graphql-ws` package's own message contract.
 * =============================================================================
 *  We do NOT re-declare graphql-transport-ws here; we import the constants and
 *  the validating parser straight from `graphql-ws`, so a message we accept is
 *  a message its client would have produced, by construction.
 *
 *  What we can't reuse is `graphql-ws`'s SERVER (makeServer). That API is built
 *  around one long-lived socket: opened() returns a closed() callback and keeps
 *  the connection context and every subscription's AsyncIterator in memory until
 *  the socket dies. Over API Gateway there is no such process — each frame is a
 *  separate, frozen Lambda invocation, and the next one may run on a different
 *  container. Calling opened() per frame would ack the handshake and then lose
 *  every subscription the moment the invocation returned.
 *
 *  So the transport re-implements the server's message loop as a pure function
 *  of (frame, durable state) — see transport.ts — while the wire format itself
 *  stays graphql-ws's.
 *
 *  Note on close codes: the spec answers protocol violations by closing with a
 *  code (4400 BadRequest, 4409 SubscriberAlreadyExists, ...). The @connections
 *  API can only delete a connection, with no code or reason, so we answer with
 *  an `error` frame instead — every graphql-ws client surfaces it to the caller,
 *  and the socket stays usable.
 * ========================================================================== */

import {
  GRAPHQL_TRANSPORT_WS_PROTOCOL,
  MessageType,
  validateMessage,
  type Message,
  type SubscribePayload,
} from 'graphql-ws';

export { GRAPHQL_TRANSPORT_WS_PROTOCOL, MessageType };
export type { Message, SubscribePayload };

/** The message types a CLIENT may send. `complete` and `pong` travel both ways. */
const CLIENT_SENT: ReadonlySet<string> = new Set<string>([
  MessageType.ConnectionInit,
  MessageType.Ping,
  MessageType.Pong,
  MessageType.Subscribe,
  MessageType.Complete,
]);

/**
 * Does this already-parsed API Gateway frame belong to graphql-ws?
 *
 * The check is deliberately narrow, because the same socket also carries this
 * library's own `{ event, data }` frames: anything that isn't a well-formed,
 * client-sent graphql-ws message falls through to @SubscribeMessage routing.
 */
export function isClientMessage(frame: unknown): frame is Message {
  // validateMessage (in graphql-ws 5 and 6 alike; 6 dropped isMessage) throws
  // on anything that is not a well-formed message.
  try {
    validateMessage(frame);
  } catch {
    return false;
  }
  return CLIENT_SENT.has((frame as Message).type);
}
