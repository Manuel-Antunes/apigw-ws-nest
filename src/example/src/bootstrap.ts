/* =============================================================================
 *  HTTP bootstrap (ECS/Fargate mode)
 * =============================================================================
 *  Builds the app via the library factory and opens an HTTP port. API Gateway's
 *  WebSocket → HTTP integration POSTs events to the raw dispatch route that
 *  createNestApp registered on the http adapter. Run directly: `ts-node`.
 *
 *  The route is off unless dispatchPath asks for it, and it answers anyone who
 *  can reach it — so set APIGW_DISPATCH_SECRET and have the integration send it
 *  (x-apigw-dispatch-secret). The adapter warns at boot when it is missing.
 * ========================================================================== */

import {
  createNestApp,
  GatewayBridge,
  HTTP_PORT,
  PROVIDER,
  DISPATCH_PATH,
} from "../..";
import { enableGraphQLSubscriptions } from "../../graphql";
import { AppModule } from "./app.module";
import { pubsub } from "./pubsub";

export async function bootstrap() {
  const bridge = GatewayBridge.builder().provider(PROVIDER).build();
  // HTTP mode is the one runtime that needs the dispatch route. The secret
  // comes from APIGW_DISPATCH_SECRET.
  const app = await createNestApp(AppModule, bridge, { adapter: { dispatchPath: DISPATCH_PATH } });
  await app.listen(HTTP_PORT);
  // After listen(): the schema exists by then.
  enableGraphQLSubscriptions(app, bridge, { pubsub });
  // eslint-disable-next-line no-console
  console.log(
    `up on :${HTTP_PORT}  provider=${PROVIDER}  dispatch=${DISPATCH_PATH}`,
  );
}

if (require.main === module) {
  bootstrap();
}
