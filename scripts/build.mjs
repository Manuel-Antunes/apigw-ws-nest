/* =============================================================================
 *  Library build — dual ESM + CJS via esbuild.
 * =============================================================================
 *  Emits the JS bundles only; type declarations are produced separately by
 *  `tsc -p tsconfig.build.json` (esbuild does not emit .d.ts).
 *
 *    dist/index.mjs    — ESM  (import 'apigw-ws-nest')
 *    dist/index.cjs    — CJS  (require)
 *    dist/graphql.mjs  — ESM  (import 'apigw-ws-nest/graphql')
 *    dist/graphql.cjs  — CJS
 *    dist/**.d.ts      — types (from tsc)
 *
 *  Only the two library entries are bundled; the example/ app is never reachable
 *  from them, so it stays out of the package. Dependencies are kept EXTERNAL
 *  (packages: 'external') — the consumer provides @nestjs/*, rxjs, the AWS SDK,
 *  etc. — so this ships just our code.
 *
 *  The graphql entry is a SEPARATE bundle so that `graphql` + `@nestjs/graphql`
 *  are only loaded by apps that actually use subscriptions. Its `../index`
 *  imports are rewritten to the sibling core bundle (see coreExternal) rather
 *  than inlined — two copies of the core would mean two GatewayBridge classes
 *  and two broadcast queues in one process.
 * ========================================================================== */

import { build } from "esbuild";
import { rmSync } from "node:fs";

rmSync("dist", { recursive: true, force: true });

/** Keep `import ... from '../index'` (the core barrel, as imported from inside
 *  src/graphql/) external, pointing at the already-built sibling bundle. */
const coreExternal = (target) => ({
  name: "core-external",
  setup(b) {
    b.onResolve({ filter: /^\.\.\/index$/ }, () => ({ path: target, external: true }));
  },
});

/** Shared options. target 'esnext' = no down-leveling; platform 'node' because
 *  this is a Node/Lambda library (uses node:events, node:module, the AWS SDK). */
const shared = {
  bundle: true,
  platform: "node",
  target: "esnext",
  packages: "external",
  sourcemap: true,
  logLevel: "info",
};

// The AWS provider and the Dynamo subscription registry lazily require() the AWS
// SDK (so local never loads it). In an ESM bundle `require` doesn't exist and
// esbuild would replace those calls with a shim that throws — so define a real
// require from import.meta.url.
const esmBanner = {
  js: "import { createRequire as __cr } from 'node:module'; const require = __cr(import.meta.url);",
};

await build({ ...shared, entryPoints: ["src/index.ts"], format: "cjs", outfile: "dist/index.cjs" });
await build({
  ...shared,
  entryPoints: ["src/index.ts"],
  format: "esm",
  outfile: "dist/index.mjs",
  banner: esmBanner,
});

await build({
  ...shared,
  entryPoints: ["src/graphql/index.ts"],
  format: "cjs",
  outfile: "dist/graphql.cjs",
  plugins: [coreExternal("./index.cjs")],
});
await build({
  ...shared,
  entryPoints: ["src/graphql/index.ts"],
  format: "esm",
  outfile: "dist/graphql.mjs",
  banner: esmBanner,
  plugins: [coreExternal("./index.mjs")],
});

// eslint-disable-next-line no-console
console.log("✓ built dist/{index,graphql}.{cjs,mjs}");
