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

/** Where the raw API Gateway HTTP integration POSTs WebSocket events (ECS/HTTP
 *  mode). Bypasses the Nest controller lifecycle — see http-bridge.ts. */
export const DISPATCH_PATH = process.env.APIGW_DISPATCH_PATH ?? '/@dispatch';
