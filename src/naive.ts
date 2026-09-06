import { randomUUID } from "node:crypto";
import type { Redis } from "ioredis";

/**
 * Deliberately wrong implementations, kept so the tests can fail against them.
 *
 * Neither of these is a strawman. They are the two shapes that actually get
 * written: the first is what "rate limiting with Redis" returns from a search,
 * the second is what someone writes after being told fixed windows are bad.
 * Do not import these outside tests.
 */

export interface NaiveOptions {
  windowMs: number;
  limit: number;
  keyPrefix?: string;
}

/**
 * Fixed window: INCR a bucket keyed by floor(now / windowMs), then EXPIRE it.
 *
 * Two independent problems.
 *
 * 1. Window boundary. The counter resets at a wall-clock instant rather than
 *    relative to traffic, so a client can spend its whole allowance at the end
 *    of one bucket and its whole allowance at the start of the next, and put
 *    2 x limit through in a span shorter than one window. Demonstrated in
 *    tests/rate-limiter.test.ts.
 *
 * 2. INCR and EXPIRE are two round trips. A crash, a timeout, or a failover
 *    between them leaves a counter with no TTL. INCR itself is atomic - the
 *    count is never wrong - but the key is now immortal and leaks.
 */
export async function naiveFixedWindowAllow(
  redis: Redis,
  id: string,
  options: NaiveOptions,
  nowMs: number = Date.now(),
): Promise<boolean> {
  const prefix = options.keyPrefix ?? "naive-fixed";
  const bucket = Math.floor(nowMs / options.windowMs);
  const key = `${prefix}:${id}:${bucket}`;

  const count = await redis.incr(key);
  // <-- a crash here leaves `key` with no TTL, forever
  await redis.pexpire(key, options.windowMs);

  return count <= options.limit;
}

/** The key `naiveFixedWindowAllow` would use, so tests can inspect it. */
export function naiveFixedWindowKey(
  id: string,
  options: NaiveOptions,
  nowMs: number,
): string {
  const prefix = options.keyPrefix ?? "naive-fixed";
  return `${prefix}:${id}:${Math.floor(nowMs / options.windowMs)}`;
}

/**
 * Sliding window with the right data structure and the wrong atomicity.
 *
 * The algorithm is correct. The bug is that trim, count and record are three
 * separate round trips, so N concurrent callers can all observe `used < limit`
 * before any of them has recorded anything, and all N are admitted. Under load
 * this overshoots by roughly the concurrency level.
 *
 * MULTI would not fix it either: MULTI is atomic execution, not
 * read-then-decide. The decision has to happen server-side, which is why the
 * real implementation is a Lua script.
 */
export async function naiveClientSideSlidingWindowAllow(
  redis: Redis,
  id: string,
  options: NaiveOptions,
  nowMs: number = Date.now(),
): Promise<boolean> {
  const prefix = options.keyPrefix ?? "naive-sliding";
  const key = `${prefix}:${id}`;

  await redis.zremrangebyscore(key, "-inf", nowMs - options.windowMs);
  const used = await redis.zcard(key);
  if (used >= options.limit) return false;
  // <-- every concurrent caller that got here has already passed the check
  await redis.zadd(key, nowMs, randomUUID());
  await redis.pexpire(key, options.windowMs);
  return true;
}
