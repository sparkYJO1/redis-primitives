import { afterAll, beforeEach, describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import { FencedStore } from "../src/fenced-store.js";
import { RedisLock } from "../src/lock.js";
import { closeAllClients, createClient, sleep } from "./support/redis.js";

let redis: Redis;
let lock: RedisLock;
let store: FencedStore;

beforeEach(async () => {
  redis ??= createClient();
  lock = new RedisLock(redis, { keyPrefix: "fencing-lock" });
  store = new FencedStore(redis, "fencing-store");
  await redis.flushdb();
});

afterAll(async () => {
  await closeAllClients();
});

/**
 * The scenario the whole repo is built around.
 *
 * A holds a 200ms lease and does 350ms of work. Nothing warns it. The lease
 * expires at 200ms, B acquires at 220ms, and from 220ms to 350ms two processes
 * are inside a critical section that is supposed to admit one. No lock
 * implementation can prevent this - the lease expired precisely because A was
 * not responding, and a Redis key cannot reach into A and stop it.
 *
 * What can be prevented is A's *write* landing after B's.
 */
describe("a lease that expires mid critical section", () => {
  it("lets a second holder in, and the fenced resource rejects the first one's write", async () => {
    const a = await lock.acquire("account-42", 200);
    expect(a).not.toBeNull();

    // A is now inside its critical section and will take 350ms.
    const aWork = (async () => {
      await sleep(350);
      return store.write("account-42", "written-by-A", a!.token);
    })();

    await sleep(220); // A's lease has expired
    const b = await lock.acquire("account-42", 5_000);
    expect(b).not.toBeNull();
    expect(b!.token).toBeGreaterThan(a!.token);

    const bWrite = await store.write("account-42", "written-by-B", b!.token);
    expect(bWrite.accepted).toBe(true);

    const aWrite = await aWork;
    expect(aWrite).toEqual({
      accepted: false,
      rejectedToken: a!.token,
      highestSeenToken: b!.token,
    });

    // B's write survived. A never learned it had been fenced out; it only
    // learns because write() told it so.
    expect(await store.read("account-42")).toBe("written-by-B");
  });

  it("without fencing, the same sequence silently clobbers the new holder's write", async () => {
    const unfencedKey = "unfenced:account-42";

    const a = await lock.acquire("account-42", 200);
    const aWork = (async () => {
      await sleep(350);
      // No token, no check. This is what a normal write looks like.
      await redis.set(unfencedKey, "written-by-A");
    })();

    await sleep(220);
    const b = await lock.acquire("account-42", 5_000);
    expect(b!.token).toBeGreaterThan(a!.token);
    await redis.set(unfencedKey, "written-by-B");

    await aWork;

    // A's write - made with a lease that expired 150ms earlier - won.
    expect(await redis.get(unfencedKey)).toBe("written-by-A");
  });
});

describe("what the fence actually promises", () => {
  it("accepts repeated writes from the current holder", async () => {
    const lease = await lock.acquire("account-42", 5_000);
    for (let i = 0; i < 5; i++) {
      const result = await store.write("account-42", `v${i}`, lease!.token);
      expect(result.accepted).toBe(true);
    }
    expect(await store.read("account-42")).toBe("v4");
  });

  it("does NOT reject a stale writer that gets there first", async () => {
    // The honest limitation. A fence orders writes; it does not detect
    // staleness on its own. If the expired holder writes before the new holder
    // has written anything, the resource has never seen a higher token and has
    // no basis to reject.
    const a = await lock.acquire("account-42", 150);
    await sleep(220);
    const b = await lock.acquire("account-42", 5_000);
    expect(b!.token).toBeGreaterThan(a!.token);

    const staleFirst = await store.write("account-42", "written-by-A", a!.token);
    expect(staleFirst.accepted).toBe(true);
    expect(await store.read("account-42")).toBe("written-by-A");

    // Once B writes, A is fenced out for good.
    expect((await store.write("account-42", "written-by-B", b!.token)).accepted).toBe(true);
    expect((await store.write("account-42", "again-by-A", a!.token)).accepted).toBe(false);
  });

  it("keeps tokens strictly increasing across expiries, releases and crashes", async () => {
    const tokens: number[] = [];

    // acquired and released cleanly
    const clean = await lock.acquire("resource", 5_000);
    tokens.push(clean!.token);
    await lock.release(clean!);

    // acquired and left to expire, as a crashed process would
    const crashed = await lock.acquire("resource", 80);
    tokens.push(crashed!.token);
    await sleep(150);

    const next = await lock.acquire("resource", 5_000);
    tokens.push(next!.token);

    expect(tokens).toEqual([...tokens].sort((x, y) => x - y));
    expect(new Set(tokens).size).toBe(tokens.length);
  });

  it("loses every guarantee if the fencing counter is lost", async () => {
    // Documented failure mode, asserted so nobody has to take it on faith.
    // The counter key has no TTL for this reason; it must also never be
    // evicted (see docker-compose.yml: maxmemory-policy noeviction).
    const first = await lock.acquire("resource", 50);
    await sleep(100);

    await redis.del(lock.fenceKey("resource")); // eviction, or a flush, or a restart

    const second = await lock.acquire("resource", 5_000);
    // The same token has now been handed to two different holders.
    expect(second!.token).toBe(first!.token);

    // And now a genuinely stale writer is indistinguishable from a fresh one.
    expect((await store.write("resource", "stale", first!.token)).accepted).toBe(true);
  });
});
