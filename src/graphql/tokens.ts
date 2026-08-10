/* =============================================================================
 *  The one shared constant of the GraphQL layer.
 * =============================================================================
 *  There are no DI tokens here any more: the transport is wired by
 *  enableGraphQLSubscriptions(app, bridge), so its options are plain function
 *  arguments rather than providers to register.
 * ========================================================================== */

/** The key the PubSub is published under in the GraphQL context, so resolvers can
 *  read it with the standard `@Context('pubsub')` instead of injecting a
 *  provider-specific class. Lives in its own leaf module to keep the transport,
 *  the pubsub and the context helper free of an import cycle. */
export const PUBSUB_CONTEXT_KEY = 'pubsub';
