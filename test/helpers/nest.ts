/* Booting a real Nest app over a bridge, on Express or Fastify. */

import { AddressInfo } from 'node:net';
import { INestApplication } from '@nestjs/common';
import { AbstractHttpAdapter } from '@nestjs/core';
import { FastifyAdapter } from '@nestjs/platform-fastify';
import {
  ApiGatewayWsAdapterOptions,
  GatewayBridge,
  GatewayBridgeBuilder,
  createNestApp,
} from '../../src';

export type Platform = 'express' | 'fastify';

export const PLATFORMS: Platform[] = ['express', 'fastify'];

export const httpAdapterFor = (platform: Platform): AbstractHttpAdapter | undefined =>
  platform === 'fastify' ? new FastifyAdapter() : undefined;

export interface Booted {
  app: INestApplication;
  bridge: GatewayBridge;
  url: string;
  post(path: string, body: unknown, headers?: Record<string, string>): Promise<Response>;
}

/** createNestApp + listen(0). Errors surface instead of exiting the process. */
export async function boot(
  rootModule: unknown,
  {
    platform = 'express',
    adapter = {},
    configure = b => b,
  }: {
    platform?: Platform;
    adapter?: ApiGatewayWsAdapterOptions;
    configure?: (builder: GatewayBridgeBuilder) => GatewayBridgeBuilder;
  } = {},
): Promise<Booted> {
  const bridge = configure(GatewayBridge.builder().provider('local')).build();
  const app = await createNestApp(rootModule, bridge, {
    adapter,
    httpAdapter: httpAdapterFor(platform),
    nest: { logger: false, abortOnError: false },
  });
  await app.listen(0, '127.0.0.1');
  const { port } = app.getHttpServer().address() as AddressInfo;
  const url = `http://127.0.0.1:${port}`;
  return {
    app,
    bridge,
    url,
    post: (path, body, headers = {}) =>
      fetch(`${url}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...headers },
        body: JSON.stringify(body),
      }),
  };
}
