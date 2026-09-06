import type { Redis } from "ioredis";

/**
 * A Lua script bound to a connection, callable as `script(keys, argv)`.
 */
export type Script<TResult> = (
  keys: string[],
  argv: (string | number)[],
) => Promise<TResult>;

type ScriptCapableRedis = Redis &
  Record<string, undefined | ((...args: (string | number)[]) => Promise<unknown>)>;

/**
 * Register a Lua script on a connection and return a typed caller.
 *
 * ioredis' `defineCommand` sends EVALSHA and only falls back to EVAL on
 * NOSCRIPT, so the script body crosses the wire once per server, not once per
 * call. `eval()` would ship the whole script every time.
 *
 * The `typeof` guard exists because several primitives can share one
 * connection; redefining the same command name is harmless but pointless.
 */
export function defineScript<TResult>(
  redis: Redis,
  name: string,
  numberOfKeys: number,
  lua: string,
): Script<TResult> {
  const client = redis as ScriptCapableRedis;
  if (typeof client[name] !== "function") {
    redis.defineCommand(name, { numberOfKeys, lua });
  }
  return async (keys, argv) => {
    if (keys.length !== numberOfKeys) {
      throw new Error(
        `script ${name} expects ${numberOfKeys} keys, got ${keys.length}`,
      );
    }
    const call = client[name];
    if (!call) throw new Error(`script ${name} was not defined on this connection`);
    // .call(redis, ...) because ioredis' generated command reads `this.options`.
    return (await call.call(redis, ...keys, ...argv)) as TResult;
  };
}
