/**
 * Evidence for docs/decisions/0002-lua-over-watch-multi.md.
 *
 * Question: can the sliding-window rate limiter be built with WATCH/MULTI
 * instead of a Lua script? WATCH/MULTI is the obvious first answer - it is
 * Redis' own optimistic-concurrency mechanism and it needs no scripting.
 *
 * Run:  npm run redis:up && npm run experiment:watch-multi
 */
import { randomUUID } from "node:crypto";
import Redis from "ioredis";
import { SlidingWindowRateLimiter } from "../src/rate-limiter.js";

const REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:56380";
const WINDOW_MS = 60_000;
const LIMIT = 5;
const CONCURRENCY = 100;
const NOW = 1_700_000_000_000;

/** Check-then-record guarded by WATCH. `exec()` returns null when it aborts. */
async function watchMultiAllow(client: Redis, key: string): Promise<boolean> {
  await client.watch(key);
  const used = await client.zcount(key, `(${NOW - WINDOW_MS}`, "+inf");
  if (used >= LIMIT) {
    await client.unwatch();
    return false;
  }
  const result = await client
    .multi()
    .zremrangebyscore(key, "-inf", NOW - WINDOW_MS)
    .zadd(key, NOW, randomUUID())
    .pexpire(key, WINDOW_MS)
    .exec();
  return result !== null;
}

async function main(): Promise<void> {
  const admin = new Redis(REDIS_URL);
  await admin.flushdb();

  // ---- A: WATCH/MULTI on one shared connection --------------------------
  // The shape every service already has: one ioredis client, many concurrent
  // request handlers.
  const shared = new Redis(REDIS_URL);
  const keyA = "experiment:shared";
  const a = await Promise.all(
    Array.from({ length: CONCURRENCY }, () => watchMultiAllow(shared, keyA)),
  );
  console.log(
    `A  shared connection      admitted ${a.filter(Boolean).length}/${CONCURRENCY}  (limit ${LIMIT})`,
  );
  await shared.quit();

  // ---- B: WATCH/MULTI with one connection per caller, plus retries -------
  const keyB = "experiment:dedicated";
  const clients = Array.from({ length: CONCURRENCY }, () => new Redis(REDIS_URL));
  let retries = 0;
  const startB = Date.now();
  const b = await Promise.all(
    clients.map(async (client) => {
      for (let attempt = 0; attempt < 100; attempt++) {
        await client.watch(keyB);
        const used = await client.zcount(keyB, `(${NOW - WINDOW_MS}`, "+inf");
        if (used >= LIMIT) {
          await client.unwatch();
          return false;
        }
        const result = await client
          .multi()
          .zremrangebyscore(keyB, "-inf", NOW - WINDOW_MS)
          .zadd(keyB, NOW, randomUUID())
          .pexpire(keyB, WINDOW_MS)
          .exec();
        if (result !== null) return true;
        retries++;
      }
      throw new Error("gave up after 100 attempts");
    }),
  );
  const msB = Date.now() - startB;
  console.log(
    `B  connection per caller  admitted ${b.filter(Boolean).length}/${CONCURRENCY}  (limit ${LIMIT})  ` +
      `${retries} retries, ${CONCURRENCY} connections, ${msB}ms`,
  );
  await Promise.all(clients.map((c) => c.quit()));

  // ---- C: the Lua script, one shared connection -------------------------
  const lua = new Redis(REDIS_URL);
  const limiter = new SlidingWindowRateLimiter(lua, {
    windowMs: WINDOW_MS,
    limit: LIMIT,
    keyPrefix: "experiment:lua",
  });
  const startC = Date.now();
  const c = await Promise.all(
    Array.from({ length: CONCURRENCY }, () => limiter.allow("tenant", NOW)),
  );
  const msC = Date.now() - startC;
  console.log(
    `C  Lua, shared connection admitted ${c.filter((r) => r.allowed).length}/${CONCURRENCY}  (limit ${LIMIT})  ` +
      `0 retries, 1 connection, ${msC}ms`,
  );
  await lua.quit();

  await admin.flushdb();
  await admin.quit();
}

void main();
