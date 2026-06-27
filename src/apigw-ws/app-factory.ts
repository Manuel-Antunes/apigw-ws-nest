/* =============================================================================
 *  App factory — build a Nest app wired for API Gateway WebSocket.
 * =============================================================================
 *  Generic in the root module so the library never imports the demo. The caller
 *  owns the GatewayBridge (built outside DI via createGatewayBridge) and passes
 *  it in; we hand it to the ApiGatewayWsAdapter, which both registers the inbound
 *  dispatch route on the HTTP adapter AND drives the WS message pipeline.
 *
 *  Building the adapter BEFORE the caller runs init()/listen() keeps the raw
 *  dispatch route ahead of Nest's 404 catch-all. Nest still binds the
 *  @SubscribeMessage handlers to bridge.hub during init(), so the SAME bridge
 *  instance the handler dispatches into is the one the handlers are wired to.
 * ========================================================================== */

import { INestApplication } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { GatewayBridge } from "./gateway-bridge";
import { ApiGatewayWsAdapter, ApiGatewayWsAdapterOptions } from "./ws-adapter";

export interface CreateNestAppOptions {
  /** passed through to NestFactory.create (NestApplicationOptions) */
  nest?: any;
  adapter?: ApiGatewayWsAdapterOptions;
}

/** Build a fully-wired (but NOT yet initialized) Nest app around a bridge. The
 *  caller decides whether to .init() (Lambda) or .listen() (HTTP). */
export async function createNestApp(
  rootModule: any,
  bridge: GatewayBridge,
  opts: CreateNestAppOptions = {},
): Promise<INestApplication> {
  const app = await NestFactory.create(
    rootModule,
    opts.nest ?? { logger: ["error", "warn"] },
  );
  app.useWebSocketAdapter(new ApiGatewayWsAdapter(app, bridge, opts.adapter));
  return app;
}
