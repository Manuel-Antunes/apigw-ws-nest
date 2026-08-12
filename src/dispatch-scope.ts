/* =============================================================================
 *  DispatchScope — make fire-and-forget sends awaitable, per dispatch.
 * =============================================================================
 *  Some sends are triggered indirectly and returned as void: an @Ack() callback,
 *  or an Observable whose .next() fires during ANOTHER client's dispatch (a
 *  subscriber's feed emits because someone else's post.create pushed to the
 *  subject). On Lambda the container freezes the moment the handler returns, so
 *  those sends must be awaited first.
 *
 *  WHY AN AsyncLocalStorage AND NOT A MODULE-LEVEL ARRAY. It used to be a module
 *  array, which is correct only while exactly one dispatch is ever in flight per
 *  process. That holds on Lambda and NOT in HTTP/ECS mode, where the dispatch
 *  route is an ordinary concurrent Express handler: two requests shared the
 *  queue, so request A awaited request B's sends, and — because draining reset
 *  the array — B could return before its own sends were awaited at all. Scoping
 *  the queue to the dispatch that created it removes the interference entirely.
 *
 *  settle() loops rather than draining once: awaiting a send can enqueue more
 *  (an Observable emitting in response to a delivered frame), and those are owed
 *  the same guarantee.
 * ========================================================================== */

import { AsyncLocalStorage } from 'async_hooks';

export class DispatchScope {
  private pending: Promise<unknown>[] = [];

  enqueue(promise: Promise<unknown>): void {
    this.pending.push(promise);
  }

  async settle(): Promise<void> {
    while (this.pending.length) {
      const inflight = this.pending;
      this.pending = [];
      await Promise.allSettled(inflight);
    }
  }
}

const scopes = new AsyncLocalStorage<DispatchScope>();

/** Run one dispatch with its own broadcast queue, drained before it resolves. */
export function runInDispatchScope<T>(fn: (scope: DispatchScope) => Promise<T>): Promise<T> {
  const scope = new DispatchScope();
  return scopes.run(scope, async () => {
    try {
      return await fn(scope);
    } finally {
      await scope.settle();
    }
  });
}

/** Register an in-flight send so the CURRENT dispatch will await it. Outside a
 *  dispatch there is nothing to hold the container open, so the promise is just
 *  kept from becoming an unhandled rejection. */
export function enqueueBroadcast(promise: Promise<unknown>): void {
  const scope = scopes.getStore();
  if (scope) scope.enqueue(promise);
  else void promise.catch(() => {});
}
