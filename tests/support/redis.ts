import Redis from "ioredis";

/** Port 56380 so this never fights another project's Redis on 6379. */
export const REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:56380";

const openClients = new Set<Redis>();

export function createClient(): Redis {
  const client = new Redis(REDIS_URL, { maxRetriesPerRequest: 2 });
  openClients.add(client);
  return client;
}

export async function closeAllClients(): Promise<void> {
  await Promise.all([...openClients].map((c) => c.quit().catch(() => c.disconnect())));
  openClients.clear();
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Poll until `predicate` is true. Returns the elapsed milliseconds. */
export async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  { timeoutMs = 10_000, pollMs = 5, label = "condition" } = {},
): Promise<number> {
  const start = Date.now();
  for (;;) {
    if (await predicate()) return Date.now() - start;
    if (Date.now() - start > timeoutMs) {
      throw new Error(`timed out after ${timeoutMs}ms waiting for ${label}`);
    }
    await sleep(pollMs);
  }
}

/**
 * The largest number of timestamps falling inside any window of `windowMs`.
 *
 * This is the only definition of "rate limited" that means anything: not
 * "resets every minute" but "no windowMs-wide span ever contains more than
 * limit requests".
 */
export function maxInAnyWindow(timestamps: number[], windowMs: number): number {
  const sorted = [...timestamps].sort((a, b) => a - b);
  let best = 0;
  let start = 0;
  for (let end = 0; end < sorted.length; end++) {
    while (sorted[end]! - sorted[start]! >= windowMs) start++;
    best = Math.max(best, end - start + 1);
  }
  return best;
}
