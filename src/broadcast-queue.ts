/* =============================================================================
 *  Broadcast queue — make fire-and-forget emissions awaitable.
 * =============================================================================
 *  Some sends are triggered indirectly and returned as void: an @Ack() callback,
 *  or a streaming Observable whose .next() fires during ANOTHER client's dispatch
 *  (e.g. a subscriber's feed emits because someone else's post.create pushed to
 *  the subject). On Lambda the container freezes the moment the handler returns,
 *  so those async sends must be awaited first. The adapter enqueues each send
 *  here; the bridge drains the queue (flushBroadcasts) at the end of every dispatch.
 *
 *  This is a leaf module (no imports) so it can be shared by the adapter and the
 *  bridge without an import cycle.
 * ========================================================================== */

let pending: Promise<unknown>[] = [];

/** Register an in-flight broadcast so the current dispatch will await it. */
export function enqueueBroadcast(p: Promise<unknown>): void {
  pending.push(p);
}

/** Await every broadcast enqueued so far, then reset. Called once per dispatch. */
export async function flushBroadcasts(): Promise<void> {
  if (pending.length === 0) return;
  const inflight = pending;
  pending = [];
  await Promise.allSettled(inflight);
}
