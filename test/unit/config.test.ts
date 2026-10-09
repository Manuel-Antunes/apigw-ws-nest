import { afterEach, describe, expect, it, vi } from 'vitest';

/** config.ts reads the environment at import time, so every case re-imports it. */
async function loadConfig(env: Record<string, string | undefined>) {
  vi.resetModules();
  for (const [key, value] of Object.entries(env)) vi.stubEnv(key, value as string);
  return import('../../src/config');
}

afterEach(() => {
  vi.unstubAllEnvs();
});

describe('config', () => {
  it('has defaults', async () => {
    const config = await loadConfig({
      RT_PROVIDER: undefined,
      PORT: undefined,
      APIGW_DISPATCH_PATH: undefined,
      APIGW_DISPATCH_SECRET: undefined,
    });
    expect(config.PROVIDER).toBe('local');
    expect(config.HTTP_PORT).toBe(3000);
    expect(config.DISPATCH_PATH).toBe('/@dispatch');
    expect(config.DISPATCH_SECRET).toBeUndefined();
    expect(config.DISPATCH_SECRET_HEADER).toBe('x-apigw-dispatch-secret');
  });

  it('reads the environment', async () => {
    const config = await loadConfig({
      RT_PROVIDER: 'aws',
      PORT: '8080',
      APIGW_DISPATCH_PATH: '/hook',
      APIGW_DISPATCH_SECRET: 's3cret',
    });
    expect(config.PROVIDER).toBe('aws');
    expect(config.HTTP_PORT).toBe(8080);
    expect(config.DISPATCH_PATH).toBe('/hook');
    expect(config.DISPATCH_SECRET).toBe('s3cret');
  });

  it('treats an empty dispatch secret as none', async () => {
    const config = await loadConfig({ APIGW_DISPATCH_SECRET: '' });
    expect(config.DISPATCH_SECRET).toBeUndefined();
  });
});
