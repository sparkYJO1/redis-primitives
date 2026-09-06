import { randomUUID } from "node:crypto";
import type { Redis } from "ioredis";
import { defineScript, type Script } from "./script.js";

// Sliding window log.
//
// One ZSET per key, one member per allowed request, scored with the timestamp
// the request was allowed at. Trim, count, admit-or-reject, record - all inside
// one script, because the gap between "count" and "record" is exactly where a
// client-side implementation lets ten concurrent callers each see room for one
// more. See src/naive.ts.
//
// Time comes from Redis' own clock (TIME) rather than from the caller, so
// clock skew between app servers cannot widen or narrow the window. ARGV[4]
// overrides it only so tests can pin the clock; production always passes -1.
//
// TIME makes the script non-deterministic. That is fine on Redis 5+, which
// replicates script *effects* rather than the script itself.
const ALLOW_LUA = `
local now
if tonumber(ARGV[4]) >= 0 then
  now = tonumber(ARGV[4])
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local windowMs = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

-- Drop everything that has fallen out of the window. The window is
-- (now - windowMs, now]: an entry exactly windowMs old is already gone.
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - windowMs)

local used = redis.call('ZCARD', KEYS[1])
if used >= limit then
  local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  local retryAfter = 0
  if oldest[2] then
    retryAfter = math.ceil(tonumber(oldest[2]) + windowMs - now)
  end
  return {0, used, retryAfter}
end

redis.call('ZADD', KEYS[1], now, ARGV[3])
-- Refreshed on every admission so an idle key expires instead of leaking.
redis.call('PEXPIRE', KEYS[1], windowMs)
return {1, used + 1, 0}
`;

export interface RateLimitDecision {
  allowed: boolean;
  /** Requests inside the window after this decision. */
  count: number;
  /** Milliseconds until the oldest request leaves the window. 0 when allowed. */
  retryAfterMs: number;
}

export interface SlidingWindowOptions {
  windowMs: number;
  limit: number;
  keyPrefix?: string;
}

/**
 * Sliding window log rate limiter.
 *
 * Cost: one ZSET member per allowed request per key, so memory is O(limit) per
 * key and the script's trim is O(log N + M). Fine for limit in the hundreds.
 * For "10,000 requests per hour per tenant" this is the wrong algorithm - use a
 * sliding window *counter* (two fixed buckets, weighted) and accept the
 * approximation.
 */
export class SlidingWindowRateLimiter {
  private readonly prefix: string;
  private readonly allowScript: Script<[number, number, number]>;

  constructor(
    redis: Redis,
    private readonly options: SlidingWindowOptions,
  ) {
    if (options.limit <= 0) throw new Error("limit must be positive");
    if (options.windowMs <= 0) throw new Error("windowMs must be positive");
    this.prefix = options.keyPrefix ?? "ratelimit";
    this.allowScript = defineScript(redis, "rpRateLimitAllow", 1, ALLOW_LUA);
  }

  key(id: string): string {
    return `${this.prefix}:${id}`;
  }

  /**
   * @param nowMs pin the clock; tests only. Omit in production so Redis' clock
   *   is used and no app server's skew can affect the window.
   */
  async allow(id: string, nowMs?: number): Promise<RateLimitDecision> {
    const [allowed, count, retryAfterMs] = await this.allowScript(
      [this.key(id)],
      [
        this.options.windowMs,
        this.options.limit,
        // Unique per request: ZADD with a repeated member updates a score
        // instead of adding a row, which would silently undercount.
        randomUUID(),
        nowMs ?? -1,
      ],
    );
    return { allowed: allowed === 1, count, retryAfterMs };
  }
}
