/* =============================================================================
 *  Runtime wiring — DI-free instantiation of the transport pieces.
 * =============================================================================
 *  The store + publisher are process-level singletons (memoized), so everything
 *  driven through the GatewayBridge/GatewayServer/GatewayClient shares ONE store
 *  instance. That sharing is what makes the local (in-memory) fan-out work: a
 *  connection that joins the 'posts' room is visible to server.to('posts').emit's
 *  membersOf(). In aws mode each Lambda is its own process and state lives in
 *  DynamoDB, so sharing is moot but harmless.
 * ========================================================================== */

import { PROVIDER } from './config';
import { GatewayBridge } from './gateway-bridge';
import { ConnectionStore, RealtimePublisher } from './ports';
import { InMemoryConnectionStore, LocalPublisher } from './providers/local';
import { DynamoConnectionStore, ApiGatewayPublisher } from './providers/aws';

let _store: ConnectionStore | undefined;
let _publisher: RealtimePublisher | undefined;

/** Provider-appropriate ConnectionStore (memoized singleton). */
export function connectionStore(): ConnectionStore {
  return (_store ??=
    PROVIDER === 'aws' ? new DynamoConnectionStore() : new InMemoryConnectionStore());
}

/** Provider-appropriate RealtimePublisher (memoized singleton). */
export function publisher(): RealtimePublisher {
  return (_publisher ??=
    PROVIDER === 'aws'
      ? new ApiGatewayPublisher(connectionStore())
      : new LocalPublisher(connectionStore()));
}

/** Build the bridge handed to the WS adapter / Lambda handler — outside DI. */
export function createGatewayBridge(): GatewayBridge {
  return new GatewayBridge(connectionStore(), publisher());
}
