/* =============================================================================
 *  apigw-ws-nest — public API
 * =============================================================================
 *  NestJS custom WebSocketAdapter over AWS API Gateway WebSocket. Import this
 *  barrel from your app / Lambda handler / HTTP bootstrap.
 *
 *  The whole transport (connections, rooms, delivery) lives in the adapter +
 *  bridge. A @WebSocketGateway written against the standard NestJS/Socket.IO
 *  surface (@WebSocketServer, client.join, server.to().emit) needs nothing from
 *  here injected — swap the adapter and the gateway is unchanged.
 *
 *  Everything is reached through one object:
 *
 *      const bridge = GatewayBridge.builder().provider('aws').build();
 *
 *      bridge.dispatch(event)          // the WebSocket routes
 *      bridge.flush(event, context)    // the outbox stream
 *      bridge.use(protocol)            // add a wire protocol
 *      bridge.onConnect(hook)          // refuse or identify at $connect
 *      bridge.disconnect(id)           // kick a connection
 *      bridge.store / .publisher / .bus
 *
 *  There are no module-level singletons: two bridges in one process share
 *  nothing, and every backend is answerable at the call site.
 * ========================================================================== */

import 'reflect-metadata';

// the bridge and its builder
export { GatewayBridge, GatewayBridgeBuilder, GatewayClient, GatewayServer, GLOBAL_ROOM, handshakeOf } from './gateway-bridge';
export type {
  ProtocolHandler,
  BridgeConfig,
  BoundHandler,
  Handshake,
  ConnectHook,
  ClientPhase,
  GatewayClientOptions,
} from './gateway-bridge';

// app wiring
export { createNestApp } from './app-factory';
export type { CreateNestAppOptions } from './app-factory';
export { ApiGatewayWsAdapter, NestGatewayProtocol } from './ws-adapter';
export type { ApiGatewayWsAdapterOptions, GatewayLifecycle } from './ws-adapter';

// durable fan-out (outbox + DynamoDB Streams)
export {
  Flusher,
  DynamoOutboxBus,
  InlineMessageBus,
  DynamoDeliveryLedger,
  InMemoryDeliveryLedger,
  sealRecord,
  outboxPk,
  decodeStreamEvent,
  OUTBOX_TTL_SECONDS,
} from './outbox';
export type {
  FlushContext,
  FlusherOptions,
  StreamEvent,
  StreamContext,
  BatchResponse,
} from './outbox';

// ports — every swappable interface
export type {
  ConnectionStore,
  RealtimePublisher,
  MessageBus,
  DeliveryLedger,
  DeliveryTarget,
  FanoutPage,
  FanoutResolver,
  SessionMeta,
  Page,
  PageCursor,
  Broadcast,
  RoomBroadcast,
  TopicBroadcast,
  OutboxRecord,
} from './ports';
export {
  ConnectionGoneError,
  isConnectionGone,
  ConnectionRejectedError,
  isConnectionRejected,
} from './ports';

// providers (the built-in backends; the builder picks these via .provider())
export { DynamoConnectionStore, ApiGatewayPublisher } from './providers/aws';
export { InMemoryConnectionStore, LocalPublisher, LocalSocketRegistry } from './providers/local';
export { docClient, queryAll, queryPage } from './providers/dynamo';

// contract
export type {
  ApiGwWsEvent,
  ApiGwResponse,
  ApiGwRequestContext,
  ApiGwEventType,
  ClientFrame,
} from './contract';
export { EVENT_TYPE, ROUTE } from './contract';

// config
export { PROVIDER, HTTP_PORT, DISPATCH_PATH, DISPATCH_SECRET, DISPATCH_SECRET_HEADER } from './config';
export type { Provider } from './config';

// dispatch scope (advanced: making an out-of-band send awaitable)
export { enqueueBroadcast, runInDispatchScope, DispatchScope } from './dispatch-scope';
