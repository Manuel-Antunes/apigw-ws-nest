/* =============================================================================
 *  WsUserGuard — an ordinary Nest guard, reading identity off the socket.
 * =============================================================================
 *  `client.data.user` was written by ChatGateway.handleConnection at $connect and
 *  persisted with the connection, so this works on ANY instance: the bridge
 *  rehydrates client.data from the store before Nest sees the frame. Refusing
 *  throws Nest's own WsException('Forbidden resource'), which the base exception
 *  filter answers with an `exception` frame.
 * ========================================================================== */

import { CanActivate, ExecutionContext, Injectable } from "@nestjs/common";
import { GatewayClient } from "../../..";
import type { ChatIdentity } from "./chat.gateway";

@Injectable()
export class WsUserGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    return !!context.switchToWs().getClient<GatewayClient<Partial<ChatIdentity>>>().data.user;
  }
}
