/* =============================================================================
 *  GraphQL object type for the demo's Post.
 * =============================================================================
 *  Code-first (@ObjectType), so the schema is generated from these decorators —
 *  `autoSchemaFile: true` builds it IN MEMORY, which matters on Lambda where the
 *  filesystem is read-only outside /tmp.
 *
 *  Every @Field carries an EXPLICIT type thunk. The usual code-first shorthand
 *  (`@Field() title: string`) reads the type from `design:type`, which only
 *  exists if the compiler emitted decorator metadata — and esbuild, which SST
 *  uses to bundle Lambdas, does not implement emitDecoratorMetadata. Without the
 *  thunk the schema build fails at cold start with "undefined type".
 *
 *  Deliberately the same shape as the Post the WebSocket gateway broadcasts, and
 *  backed by the SAME PostService: a post created through the GraphQL mutation
 *  shows up in the plain-WebSocket client's post.list, and vice versa.
 * ========================================================================== */

import { Field, Float, ID, ObjectType } from '@nestjs/graphql';

@ObjectType('Post')
export class PostModel {
  @Field(() => ID)
  id: string;

  @Field(() => String)
  title: string;

  @Field(() => String)
  body: string;

  @Field(() => Float)
  createdAt: number;
}
