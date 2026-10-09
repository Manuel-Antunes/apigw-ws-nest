/* =============================================================================
 *  Vitest — three projects, one coverage report.
 * =============================================================================
 *    unit         test/unit         one module at a time, fakes for the rest
 *    integration  test/integration  real Nest apps (Express and Fastify) over
 *                                   the library, no emulator
 *    e2e          test/e2e          real WebSocket clients against the local
 *                                   emulator (or a deployed API: E2E_WS_URL)
 *
 *  SWC instead of Vite's esbuild transform: Nest — @nestjs/graphql's @Args() in
 *  particular — reads `design:paramtypes`, which only a compiler that implements
 *  emitDecoratorMetadata produces. unplugin-swc takes the decorator settings
 *  from tsconfig.json.
 * ========================================================================== */

import { createRequire } from 'node:module';
import swc from 'unplugin-swc';
import { defineConfig } from 'vitest/config';

const require = createRequire(import.meta.url);

export default defineConfig({
  plugins: [swc.vite({ module: { type: 'es6' } })],
  resolve: {
    // graphql 16 has no "exports": Node resolves it to index.js (CommonJS), Vite
    // to its "module" field (index.mjs). @nestjs/graphql is loaded by Node and
    // the library through Vite, so without this they build and execute against
    // two copies — "Cannot use GraphQLSchema from another module or realm".
    // Production has one resolver (Node's), so pin Vite to it.
    alias: [{ find: /^graphql$/, replacement: require.resolve('graphql') }],
  },
  test: {
    // The example repositories (and PROVIDER) read it at import time.
    env: { RT_PROVIDER: 'local' },
    restoreMocks: true,
    coverage: {
      provider: 'v8',
      include: ['src/**/*.ts'],
      // The example app is exercised by the e2e project, but it is not the
      // product: coverage measures the library.
      exclude: ['src/example/**'],
      reporter: ['text', 'html', 'lcov'],
      thresholds: { statements: 98, branches: 95, functions: 98, lines: 98 },
    },
    projects: [
      {
        extends: true,
        test: { name: 'unit', include: ['test/unit/**/*.test.ts'] },
      },
      {
        extends: true,
        test: {
          name: 'integration',
          include: ['test/integration/**/*.test.ts'],
          testTimeout: 20_000,
          hookTimeout: 30_000,
        },
      },
      {
        extends: true,
        test: {
          name: 'e2e',
          include: ['test/e2e/**/*.test.ts'],
          testTimeout: 60_000,
          hookTimeout: 60_000,
          // One emulator per file is plenty; running files one at a time keeps
          // a slow machine from timing the WebSocket round trips out.
          fileParallelism: false,
        },
      },
    ],
  },
});
