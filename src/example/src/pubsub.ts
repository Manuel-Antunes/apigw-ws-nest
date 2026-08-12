/* =============================================================================
 *  The app's PubSub — one object, two references, no singleton.
 * =============================================================================
 *  GraphQLModule.forRoot is evaluated when AppModule is DEFINED, which is before
 *  the bridge exists. So the PubSub is created here, in a module both sides can
 *  import, and handed to each of them:
 *
 *    app.module.ts   context: apiGwPubSubContext(pubsub)     (the HTTP path)
 *    handler.ts      enableGraphQLSubscriptions(app, bridge, { pubsub })
 *
 *  It is inert until a bridge attaches its MessageBus — publishing before that
 *  throws with a message saying so, rather than silently going nowhere.
 * ========================================================================== */

import { ApiGwPubSub } from '../../graphql';

export const pubsub = new ApiGwPubSub();
