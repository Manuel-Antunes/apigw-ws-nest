/* =============================================================================
 *  HTTP bootstrap (ECS/Fargate mode)
 * =============================================================================
 *  Builds the app via the library factory and opens an HTTP port. API Gateway's
 *  WebSocket → HTTP integration POSTs events to the raw dispatch route that
 *  createNestApp registered on the http adapter. Run directly: `ts-node`.
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
  const app = await createNestApp(AppModule, bridge);
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
