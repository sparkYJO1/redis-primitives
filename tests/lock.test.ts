import { afterAll, beforeEach, describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import { RedisLock } from "../src/lock.js";
import { closeAllClients, createClient, sleep } from "./support/redis.js";

let redis: Redis;
let lock: RedisLock;

beforeEach(async () => {
  redis ??= createClient();
  lock = new RedisLock(redis, { keyPrefix: "test-lock" });
  await redis.flushdb();
});

afterAll(async () => {
  await closeAllClients();
});

describe("mutual exclusion", () => {
  it("refuses a second acquire while the lease is live", async () => {
    const first = await lock.acquire("job", 5_000);
    expect(first).not.toBeNull();

    const second = await lock.acquire("job", 5_000);
    expect(second).toBeNull();
  });

  it("lets exactly one of many concurrent acquires win, every round", async () => {
    const CONCURRENCY = 40;
    const ROUNDS = 15;
    const winningTokens: number[] = [];

    for (let round = 0; round < ROUNDS; round++) {
      // One shared connection on purpose. ioredis multiplexes, so 40 pending
      // acquires interleave at the server exactly as 40 request handlers in a
      // real service would.
      const results = await Promise.all(
        Array.from({ length: CONCURRENCY }, () => lock.acquire("job", 5_000)),
      );

      const winners = results.filter((lease) => lease !== null);
      expect(winners).toHaveLength(1);

      const winner = winners[0]!;
      winningTokens.push(winner.token);
      expect(await lock.release(winner)).toBe(true);
    }

    // Strictly increasing, with no repeats, across every acquisition.
    for (let i = 1; i < winningTokens.length; i++) {
      expect(winningTokens[i]!).toBeGreaterThan(winningTokens[i - 1]!);
    }
  });
});

describe("release is a compare-and-delete, not a DEL", () => {
  it("does not delete the next holder's lock when our lease has expired", async () => {
    const a = await lock.acquire("job", 150);
    expect(a).not.toBeNull();

    await sleep(250); // a's lease expires while a is still "working"

    const b = await lock.acquire("job", 5_000);
    expect(b).not.toBeNull();
    expect(b!.token).toBeGreaterThan(a!.token);

    // a finishes and tidies up, unaware it lost the lease.
    expect(await lock.release(a!)).toBe(false);
    expect(await lock.currentToken("job")).toBe(b!.token);

    // What the same tidy-up does when release is implemented as a bare DEL,
    // which is the version in most blog posts:
    await redis.del(lock.lockKey("job"));
    expect(await lock.currentToken("job")).toBeNull();

    // b still believes it holds the lock, and a third process can now take it.
    const c = await lock.acquire("job", 5_000);
    expect(c).not.toBeNull();
  });
});

describe("extend", () => {
  it("keeps a live lease alive", async () => {
    const lease = await lock.acquire("job", 200);
    expect(lease).not.toBeNull();

    await sleep(120);
    expect(await lock.extend(lease!, 400)).toBe(true);

    await sleep(200); // past the original 200ms expiry
    expect(await lock.currentToken("job")).toBe(lease!.token);
  });

  it("fails once the lease is gone, and does not resurrect it", async () => {
    const lease = await lock.acquire("job", 120);
    expect(lease).not.toBeNull();

    await sleep(220);
    expect(await lock.extend(lease!, 5_000)).toBe(false);
    expect(await lock.currentToken("job")).toBeNull();
  });

  it("cannot extend a lock someone else now holds", async () => {
    const a = await lock.acquire("job", 120);
    await sleep(200);
    const b = await lock.acquire("job", 5_000);

    expect(await lock.extend(a!, 60_000)).toBe(false);
    expect(await lock.currentToken("job")).toBe(b!.token);
  });
});

describe("withLock", () => {
  it("runs the body under the lock and releases afterwards", async () => {
    const result = await lock.withLock("job", 5_000, async (lease) => {
      expect(await lock.currentToken("job")).toBe(lease.token);
      return "done";
    });

    expect(result).toEqual({ ran: true, value: "done" });
    expect(await lock.currentToken("job")).toBeNull();
  });

  it("does not run the body when the lock is taken", async () => {
    await lock.acquire("job", 5_000);
    let ran = false;

    const result = await lock.withLock("job", 5_000, async () => {
      ran = true;
      return "done";
    });

    expect(ran).toBe(false);
    expect(result.ran).toBe(false);
  });

  it("releases nothing when the body outlives the lease", async () => {
    const outer = lock.withLock("job", 100, async () => {
      await sleep(250); // overruns the lease
      return "finished anyway";
    });

    await sleep(180);
    const stolen = await lock.acquire("job", 5_000);
    expect(stolen).not.toBeNull();

    await outer; // its finally-block release runs here
    // The overrunning body did not delete the new holder's lock.
    expect(await lock.currentToken("job")).toBe(stolen!.token);
  });
});
