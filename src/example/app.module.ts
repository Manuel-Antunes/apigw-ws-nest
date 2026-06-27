/* =============================================================================
 *  AppModule — the demo's composition root.
 * =============================================================================
 *  Tiny: pull in the library module (ports + bridge + stream consumer) and the
 *  posts feature. No controllers (the API Gateway integration is registered on
 *  the HTTP adapter by createNestApp, not via the Nest lifecycle).
 * ========================================================================== */

import { Module } from '@nestjs/common';
import { PostModule } from './posts/post.module';

@Module({
  imports: [PostModule],
})
export class AppModule {}
