import { afterAll, beforeEach, describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import {
  naiveClientSideSlidingWindowAllow,
  naiveFixedWindowAllow,
  naiveFixedWindowKey,
} from "../src/naive.js";
import { SlidingWindowRateLimiter } from "../src/rate-limiter.js";
import {
  closeAllClients,
  createClient,
  maxInAnyWindow,
  sleep,
} from "./support/redis.js";

let redis: Redis;

beforeEach(async () => {
  redis ??= createClient();
  await redis.flushdb();
});

afterAll(async () => {
  await closeAllClients();
});

const WINDOW_MS = 1_000;
const LIMIT = 5;
// A bucket boundary, so "end of the window" and "start of the next" are exact.
const BASE = 1_700_000_000_000;

/**
 * The traffic pattern that breaks a fixed window: spend the whole allowance in
 * the last 100ms of one bucket, then the whole allowance again in the first
 * 100ms of the next. Every request lands inside a 104ms span.
 */
const BURST_AT_BOUNDARY = [
  ...Array.from({ length: LIMIT }, (_, i) => BASE + 900 + i),
  ...Array.from({ length: LIMIT }, (_, i) => BASE + 1_000 + i),
];

describe("the naive fixed window", () => {
  it("lets 2x the limit through across a window boundary", async () => {
    const allowedAt: number[] = [];

    for (const now of BURST_AT_BOUNDARY) {
      const ok = await naiveFixedWindowAllow(
        redis,
        "tenant-1",
        { windowMs: WINDOW_MS, limit: LIMIT },
        now,
      );
      if (ok) allowedAt.push(now);
    }

    // Every request was admitted: 5 in bucket N, 5 in bucket N+1.
    expect(allowedAt).toHaveLength(2 * LIMIT);

    // And they all fit inside a span of 104ms, against a limit of 5 per second.
    const span = allowedAt.at(-1)! - allowedAt[0]!;
    expect(span).toBe(104);
    expect(maxInAnyWindow(allowedAt, WINDOW_MS)).toBe(2 * LIMIT);
  });

  it("leaks a counter with no TTL if the process dies between INCR and EXPIRE", async () => {
    const key = naiveFixedWindowKey(
      "tenant-1",
      { windowMs: WINDOW_MS, limit: LIMIT },
      BASE,
    );

    // The first half of the non-atomic pair, which is all a crashed process
    // manages to do.
    await redis.incr(key);

    expect(await redis.pttl(key)).toBe(-1); // -1 = no expiry set
    await sleep(50);
    expect(await redis.exists(key)).toBe(1); // still there, and always will be
  });
});

describe("the sliding window", () => {
  it("holds the limit through the same boundary burst", async () => {
    const limiter = new SlidingWindowRateLimiter(redis, {
      windowMs: WINDOW_MS,
      limit: LIMIT,
      keyPrefix: "sliding-boundary",
    });
    const allowedAt: number[] = [];

    for (const now of BURST_AT_BOUNDARY) {
      const decision = await limiter.allow("tenant-1", now);
      if (decision.allowed) allowedAt.push(now);
    }

    expect(allowedAt).toHaveLength(LIMIT);
    expect(maxInAnyWindow(allowedAt, WINDOW_MS)).toBe(LIMIT);
  });

  it("admits again only as requests age out of the window, not on a reset", async () => {
    const limiter = new SlidingWindowRateLimiter(redis, {
      windowMs: WINDOW_MS,
      limit: 3,
      keyPrefix: "sliding-ageout",
    });

    for (const offset of [0, 10, 20]) {
      expect((await limiter.allow("tenant-1", BASE + offset)).allowed).toBe(true);
    }

    const denied = await limiter.allow("tenant-1", BASE + 30);
    expect(denied.allowed).toBe(false);
    // The oldest request leaves the window at BASE + 1000.
    expect(denied.retryAfterMs).toBe(970);

    // One millisecond before that, still denied.
    expect((await limiter.allow("tenant-1", BASE + 999)).allowed).toBe(false);
    // At exactly BASE + 1000 the first request has aged out, and exactly one
    // slot opens - not three.
    expect((await limiter.allow("tenant-1", BASE + 1_000)).allowed).toBe(true);
    expect((await limiter.allow("tenant-1", BASE + 1_001)).allowed).toBe(false);
  });

  it("uses Redis' clock when no clock is injected", async () => {
    const limiter = new SlidingWindowRateLimiter(redis, {
      windowMs: 300,
      limit: 2,
      keyPrefix: "sliding-serverclock",
    });

    expect((await limiter.allow("tenant-1")).allowed).toBe(true);
    expect((await limiter.allow("tenant-1")).allowed).toBe(true);
    expect((await limiter.allow("tenant-1")).allowed).toBe(false);

    await sleep(360);
    expect((await limiter.allow("tenant-1")).allowed).toBe(true);
  });
});

describe("atomicity under real concurrency", () => {
  const CONCURRENCY = 100;

  it("the client-side sliding window overshoots badly", async () => {
    // Correct algorithm, three round trips. Every caller reads ZCARD before
    // any caller has run ZADD, so every caller sees room.
    const results = await Promise.all(
      Array.from({ length: CONCURRENCY }, () =>
        naiveClientSideSlidingWindowAllow(
          redis,
          "tenant-1",
          { windowMs: WINDOW_MS, limit: LIMIT },
          BASE,
        ),
      ),
    );

    const allowed = results.filter(Boolean).length;
    console.log(
      `naive client-side sliding window: ${allowed}/${CONCURRENCY} admitted against a limit of ${LIMIT}`,
    );
    expect(allowed).toBeGreaterThan(LIMIT);
  });

  it("the Lua sliding window admits exactly the limit", async () => {
    const limiter = new SlidingWindowRateLimiter(redis, {
      windowMs: WINDOW_MS,
      limit: 50,
      keyPrefix: "sliding-concurrent",
    });

    const results = await Promise.all(
      Array.from({ length: 200 }, () => limiter.allow("tenant-1", BASE)),
    );

    expect(results.filter((r) => r.allowed)).toHaveLength(50);
    expect(await redis.zcard(limiter.key("tenant-1"))).toBe(50);
  });
});
