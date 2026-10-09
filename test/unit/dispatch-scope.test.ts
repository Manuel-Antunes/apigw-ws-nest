import { describe, expect, it } from 'vitest';
import { DispatchScope, enqueueBroadcast, runInDispatchScope } from '../../src';
import { sleep } from '../helpers/fakes';

describe('runInDispatchScope', () => {
  it('resolves with the function\'s result after every enqueued send settled', async () => {
    let sent = false;
    const result = await runInDispatchScope(async () => {
      enqueueBroadcast(sleep(20).then(() => (sent = true)));
      return 'done';
    });
    expect(result).toBe('done');
    expect(sent).toBe(true);
  });

  it('keeps draining while settling enqueues more', async () => {
    const order: string[] = [];
    await runInDispatchScope(async () => {
      enqueueBroadcast(
        sleep(5).then(() => {
          order.push('first');
          enqueueBroadcast(sleep(5).then(() => order.push('second')));
        }),
      );
    });
    expect(order).toEqual(['first', 'second']);
  });

  it('does not fail because a send failed', async () => {
    await expect(
      runInDispatchScope(async () => {
        enqueueBroadcast(Promise.reject(new Error('boom')));
        return 1;
      }),
    ).resolves.toBe(1);
  });

  it('still settles when the function throws, then rethrows', async () => {
    let sent = false;
    await expect(
      runInDispatchScope(async () => {
        enqueueBroadcast(sleep(10).then(() => (sent = true)));
        throw new Error('handler failed');
      }),
    ).rejects.toThrow('handler failed');
    expect(sent).toBe(true);
  });

  it('isolates concurrent dispatches: neither waits for the other\'s sends', async () => {
    const finished: string[] = [];
    const slow = runInDispatchScope(async () => {
      enqueueBroadcast(sleep(60).then(() => finished.push('slow-send')));
    }).then(() => finished.push('slow-dispatch'));
    const fast = runInDispatchScope(async () => {
      enqueueBroadcast(sleep(5).then(() => finished.push('fast-send')));
    }).then(() => finished.push('fast-dispatch'));
    await Promise.all([slow, fast]);
    expect(finished.indexOf('fast-dispatch')).toBeLessThan(finished.indexOf('slow-send'));
    expect(finished.indexOf('slow-dispatch')).toBeGreaterThan(finished.indexOf('slow-send'));
  });

  it('hands the function its scope', async () => {
    await runInDispatchScope(async scope => {
      expect(scope).toBeInstanceOf(DispatchScope);
    });
  });
});

describe('enqueueBroadcast outside a dispatch', () => {
  it('keeps a failing send from becoming an unhandled rejection', async () => {
    enqueueBroadcast(Promise.reject(new Error('nobody is listening')));
    await sleep(10); // vitest fails the run on an unhandled rejection
  });
});

describe('DispatchScope', () => {
  it('settles what was enqueued on it', async () => {
    const scope = new DispatchScope();
    let done = false;
    scope.enqueue(sleep(5).then(() => (done = true)));
    await scope.settle();
    expect(done).toBe(true);
  });
});
