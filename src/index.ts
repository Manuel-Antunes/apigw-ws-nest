/* =============================================================================
 *  apigw-ws — public API
 * =============================================================================
 *  NestJS custom WebSocketAdapter over AWS API Gateway WebSocket. Import this
 *  barrel from your app / Lambda handler / HTTP bootstrap.
 *
 *  The whole transport (connections, rooms, delivery) lives in the adapter +
 *  bridge. A @WebSocketGateway written against the standard NestJS/Socket.IO
 *  surface (@WebSocketServer, client.join, server.to().emit) needs nothing from
 *  here injected — swap the adapter and the gateway is unchanged.
 * ========================================================================== */

import 'reflect-metadata';

// app wiring
export { createNestApp } from './app-factory';
export type { CreateNestAppOptions } from './app-factory';
export { createGatewayBridge, connectionStore, publisher } from './runtime';

// transport internals (advanced / custom wiring)
export { ApiGatewayWsAdapter } from './ws-adapter';
export type { ApiGatewayWsAdapterOptions } from './ws-adapter';
export { GatewayBridge, GatewayClient, GatewayServer, GLOBAL_ROOM } from './gateway-bridge';

// ports + contract types
export type { ConnectionStore, RealtimePublisher, SessionMeta } from './ports';
export { ConnectionGoneError } from './ports';
export type {
  ApiGwWsEvent,
  ApiGwResponse,
  ApiGwRequestContext,
  ApiGwEventType,
  ClientFrame,
} from './contract';
export { EVENT_TYPE, ROUTE } from './contract';

// config
export { PROVIDER, HTTP_PORT, DISPATCH_PATH } from './config';

// local-mode helper (used by the local emulator)
export { LocalSocketRegistry } from './providers/local';
