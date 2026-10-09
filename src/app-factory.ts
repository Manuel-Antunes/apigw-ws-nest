/* =============================================================================
 *  App factory — the ordinary NestJS bootstrap, with one extra line.
 * =============================================================================
 *  A convenience wrapper, nothing more. It expands to exactly what you would
 *  write by hand:
 *
 *      const app = await NestFactory.create(AppModule);
 *      app.useWebSocketAdapter(new ApiGatewayWsAdapter(app, bridge));
 *      // then YOU call app.init() (Lambda) or app.listen() (HTTP)
 *
 *  ...which is the same shape as swapping in Socket.IO's IoAdapter. Use whichever
 *  reads better; there is no hidden wiring either way.
 *
 *  ORDER MATTERS: the adapter is constructed BEFORE init()/listen(), which keeps
 *  its raw dispatch route (when `dispatchPath` asks for one) ahead of Nest's 404
 *  catch-all, and registers the `{ event, data }` protocol on the bridge before
 *  any frame can arrive. Nest binds the @SubscribeMessage handlers during init(), so the SAME
 *  bridge instance the handler dispatches into is the one they are wired to.
 * ========================================================================== */

import { INestApplication } from "@nestjs/common";
import { AbstractHttpAdapter, NestFactory } from "@nestjs/core";
import { GatewayBridge } from "./gateway-bridge";
import { ApiGatewayWsAdapter, ApiGatewayWsAdapterOptions } from "./ws-adapter";

export interface CreateNestAppOptions {
  /** passed through to NestFactory.create (NestApplicationOptions) */
  nest?: any;
  adapter?: ApiGatewayWsAdapterOptions;
  /** The HTTP platform, e.g. `new FastifyAdapter()`. Default: Nest's own default
   *  (Express, from @nestjs/platform-express). */
  httpAdapter?: AbstractHttpAdapter;
}

/** Build a fully-wired (but NOT yet initialized) Nest app around a bridge. The
 *  caller decides whether to .init() (Lambda) or .listen() (HTTP). */
export async function createNestApp(
  rootModule: any,
  bridge: GatewayBridge,
  opts: CreateNestAppOptions = {},
): Promise<INestApplication> {
  const nestOptions = opts.nest ?? { logger: ["error", "warn"] };
  const app = opts.httpAdapter
    ? await NestFactory.create(rootModule, opts.httpAdapter, nestOptions)
    : await NestFactory.create(rootModule, nestOptions);
  app.useWebSocketAdapter(new ApiGatewayWsAdapter(app, bridge, opts.adapter));
  return app;
}
