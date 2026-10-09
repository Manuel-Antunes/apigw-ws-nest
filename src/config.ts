/* =============================================================================
 *  Runtime configuration (read from env at import time)
 * =============================================================================
 *  IMPORTANT: this module reads process.env when first imported. Anything that
 *  needs to FORCE a provider (e.g. the local emulator forcing 'local') must set
 *  process.env.RT_PROVIDER BEFORE importing any module that pulls this in.
 * ========================================================================== */

export type Provider = 'local' | 'aws';

export const PROVIDER = (process.env.RT_PROVIDER ?? 'local') as Provider;
export const HTTP_PORT = Number(process.env.PORT ?? 3000);

/** The conventional path for the raw dispatch route API Gateway's HTTP
 *  integration POSTs WebSocket events to (ECS/HTTP mode). Nothing registers it
 *  on its own: pass it as the adapter's `dispatchPath`. See ws-adapter.ts. */
export const DISPATCH_PATH = process.env.APIGW_DISPATCH_PATH ?? '/@dispatch';

/** The value the dispatch route requires in its secret header. Unset = the
 *  route is unauthenticated, which is only safe where nothing but API Gateway
 *  can reach it. */
export const DISPATCH_SECRET = process.env.APIGW_DISPATCH_SECRET || undefined;

/** The header the dispatch secret travels in, unless the adapter names another. */
export const DISPATCH_SECRET_HEADER = 'x-apigw-dispatch-secret';
