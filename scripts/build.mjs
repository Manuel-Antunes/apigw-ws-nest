/* =============================================================================
 *  Library build — dual ESM + CJS via esbuild.
 * =============================================================================
 *  Emits the JS bundles only; type declarations are produced separately by
 *  `tsc -p tsconfig.build.json` (esbuild does not emit .d.ts).
 *
 *    dist/index.mjs  — ESM  (import)
 *    dist/index.cjs  — CJS  (require)
 *    dist/index.d.ts — types (from tsc)
 *
 *  Only the library entry (src/index.ts) is bundled; the example/ app is never
 *  reachable from it, so it stays out of the package. Dependencies are kept
 *  EXTERNAL (packages: 'external') — the consumer provides @nestjs/*, rxjs, the
 *  AWS SDK, etc. — so this ships just our code.
 * ========================================================================== */

import { build } from "esbuild";
import { rmSync } from "node:fs";

rmSync("dist", { recursive: true, force: true });

/** Shared options. target 'esnext' = no down-leveling; platform 'node' because
 *  this is a Node/Lambda library (uses node:events, node:module, the AWS SDK). */
const shared = {
  entryPoints: ["src/index.ts"],
  bundle: true,
  platform: "node",
  target: "esnext",
  packages: "external",
  sourcemap: true,
  logLevel: "info",
};

await build({
  ...shared,
  format: "cjs",
  outfile: "dist/index.cjs",
});

await build({
  ...shared,
  format: "esm",
  outfile: "dist/index.mjs",
  // The AWS provider lazily require()s the AWS SDK (so local never loads it). In
  // an ESM bundle `require` doesn't exist and esbuild would replace those calls
  // with a shim that throws — so define a real require from import.meta.url.
  banner: {
    js: "import { createRequire as __cr } from 'node:module'; const require = __cr(import.meta.url);",
  },
});

// eslint-disable-next-line no-console
console.log("✓ built dist/index.cjs + dist/index.mjs");
