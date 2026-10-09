/* =============================================================================
 *  ChatResolver — the same identity, over GraphQL.
 * =============================================================================
 *  An operation arriving over the API Gateway socket carries the connection's
 *  identity in its context as `connection: { id, data }` — the `client.data`
 *  that ChatGateway.handleConnection wrote at $connect. Over the HTTP GraphQL
 *  endpoint there is no socket, so it is absent and this answers null.
 * ========================================================================== */

import { Context, Query, Resolver } from "@nestjs/graphql";
import type { ChatIdentity } from "./chat.gateway";

@Resolver()
export class ChatResolver {
  @Query(() => String, {
    nullable: true,
    description: "The name this connection authenticated as at $connect, if any.",
  })
  whoami(@Context("connection") connection?: { data?: Partial<ChatIdentity> }): string | null {
    return connection?.data?.user?.name ?? null;
  }
}
