/* =============================================================================
 *  `pnpm dev` — the local API Gateway emulator on a fixed port.
 * =============================================================================
 *  The emulator itself is in emulator.ts (the end-to-end tests start it too).
 *
 *  RT_PROVIDER is forced to 'local' BEFORE any library import, so the example
 *  repositories pick their in-memory backends and the publisher routes through
 *  the in-memory socket registry instead of the AWS Management API.
 * ========================================================================== */

process.env.RT_PROVIDER = "local";
import "dotenv/config";
import { startEmulator } from "./emulator";

startEmulator({ port: Number(process.env.PORT ?? 6005) }).then(emulator => {
  /* eslint-disable no-console */
  console.log(`local API Gateway emulator`);
  console.log(`  test client : ${emulator.httpUrl}`);
  console.log(`  multi-chat  : ${emulator.httpUrl}/chat`);
  console.log(`  graphql     : ${emulator.httpUrl}/graphql`);
  console.log(`  websocket   : ${emulator.wsUrl}`);
  console.log(`  reload      : POST ${emulator.httpUrl}/__reload  (= new Lambda instance)`);
  console.log(`  inspect     : GET  ${emulator.httpUrl}/__connections/<id>`);
  /* eslint-enable no-console */
});
