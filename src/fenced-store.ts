import type { Redis } from "ioredis";
import { defineScript, type Script } from "./script.js";

// Reject any write carrying a token older than the highest one already seen.
//
// `<` and not `<=`: the current holder writes many times during one critical
// section with the same token, and all of those must be accepted. Only a
// *strictly older* writer is stale.
const WRITE_LUA = `
local seen = tonumber(redis.call('GET', KEYS[2]) or '0')
local token = tonumber(ARGV[1])
if token < seen then
  return {0, seen}
end
redis.call('SET', KEYS[2], token)
redis.call('SET', KEYS[1], ARGV[2])
return {1, token}
`;

export type FencedWriteResult =
  | { accepted: true; token: number }
  | { accepted: false; rejectedToken: number; highestSeenToken: number };

/**
 * A resource that refuses writes from a stale lock holder.
 *
 * This is the half of fencing that people skip. Handing out a token does
 * nothing on its own - a process whose lease expired 200ms ago will happily
 * keep writing, and the lock cannot stop it, because from Redis' point of view
 * that process is just a client issuing a normal command. The only place the
 * stale write can be caught is at the resource being written to.
 *
 * Implemented here on Redis so the tests can run, but the pattern is what
 * matters, not the storage. In Postgres it is:
 *
 *   UPDATE account SET balance = $1, fence_token = $2
 *   WHERE id = $3 AND fence_token <= $2
 *
 * and if `rowCount` is 0 you were fenced out. Anything that cannot express
 * that check - an append-only log, a plain S3 PUT, a webhook to a third party -
 * cannot be fenced, and a lock in front of it is best-effort no matter how the
 * lock is implemented.
 */
export class FencedStore {
  private readonly writeScript: Script<[number, number]>;

  constructor(
    private readonly redis: Redis,
    private readonly prefix = "fenced",
  ) {
    this.writeScript = defineScript(redis, "rpFencedWrite", 2, WRITE_LUA);
  }

  valueKey(key: string): string {
    return `${this.prefix}:${key}`;
  }

  tokenKey(key: string): string {
    return `${this.prefix}:${key}:token`;
  }

  async write(
    key: string,
    value: string,
    token: number,
  ): Promise<FencedWriteResult> {
    const [ok, seen] = await this.writeScript(
      [this.valueKey(key), this.tokenKey(key)],
      [token, value],
    );
    if (ok === 1) return { accepted: true, token };
    return { accepted: false, rejectedToken: token, highestSeenToken: seen };
  }

  async read(key: string): Promise<string | null> {
    return this.redis.get(this.valueKey(key));
  }

  async highestSeenToken(key: string): Promise<number> {
    const raw = await this.redis.get(this.tokenKey(key));
    return raw === null ? 0 : Number(raw);
  }
}
