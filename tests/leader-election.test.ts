import { afterAll, afterEach, beforeEach, describe, expect, it } from "vitest";
import type { Redis } from "ioredis";
import { FencedStore } from "../src/fenced-store.js";
import { LeaderElection } from "../src/leader-election.js";
import { RedisLock } from "../src/lock.js";
import { closeAllClients, createClient, sleep, waitFor } from "./support/redis.js";

let redis: Redis;
let lock: RedisLock;
let store: FencedStore;
const started: LeaderElection[] = [];

beforeEach(async () => {
  redis ??= createClient();
  lock = new RedisLock(redis, { keyPrefix: "election" });
  store = new FencedStore(redis, "election-store");
  await redis.flushdb();
});

afterEach(async () => {
  await Promise.all(started.splice(0).map((node) => node.stop()));
});

afterAll(async () => {
  await closeAllClients();
});

interface NodeHandle {
  id: string;
  election: LeaderElection;
  demotions: string[];
}

function spawn(
  id: string,
  overrides: Partial<{
    leaseTtlMs: number;
    renewIntervalMs: number;
    campaignIntervalMs: number;
  }> = {},
): NodeHandle {
  const demotions: string[] = [];
  const election = new LeaderElection(lock, {
    name: "scheduler",
    nodeId: id,
    leaseTtlMs: overrides.leaseTtlMs ?? 400,
    renewIntervalMs: overrides.renewIntervalMs ?? 100,
    campaignIntervalMs: overrides.campaignIntervalMs ?? 25,
    onDemoted: (reason) => demotions.push(reason),
  });
  started.push(election);
  election.start();
  return { id, election, demotions };
}

describe("one leader at a time", () => {
  it("elects exactly one of five nodes and keeps it there", async () => {
    // Generous margins: this test is about the invariant, not about timing.
    const nodes = ["n1", "n2", "n3", "n4", "n5"].map((id) =>
      spawn(id, { leaseTtlMs: 1_500, renewIntervalMs: 150 }),
    );

    await waitFor(() => nodes.some((n) => n.election.isLeader), {
      label: "first election",
    });

    // Sample for 1.5s across many renew intervals.
    for (let i = 0; i < 60; i++) {
      const leaders = nodes.filter((n) => n.election.isLeader);
      expect(leaders).toHaveLength(1);
      await sleep(25);
    }

    const leader = nodes.find((n) => n.election.isLeader)!;
    expect(leader.election.epoch).toBe(1); // no failover happened
  });

  it("rejects a renew interval that cannot keep the lease alive", () => {
    expect(
      () =>
        new LeaderElection(lock, {
          name: "scheduler",
          nodeId: "n1",
          leaseTtlMs: 500,
          renewIntervalMs: 500,
        }),
    ).toThrow(/shorter than leaseTtlMs/);
  });
});

describe("failover", () => {
  it("hands over almost immediately when the leader steps down cleanly", async () => {
    const nodes = [spawn("n1"), spawn("n2")];
    await waitFor(() => nodes.some((n) => n.election.isLeader), { label: "election" });

    const first = nodes.find((n) => n.election.isLeader)!;
    const other = nodes.find((n) => n !== first)!;
    const firstEpoch = first.election.epoch!;

    const start = Date.now();
    await first.election.stop(); // releases the lease instead of letting it expire
    const elapsed = await waitFor(() => other.election.isLeader, {
      label: "second election",
    });

    console.log(`clean handover took ${Date.now() - start}ms`);
    expect(other.election.epoch!).toBeGreaterThan(firstEpoch);
    // Bounded by the campaign interval, not by the lease TTL.
    expect(elapsed).toBeLessThan(400);
  });

  it("takes between (ttl - renewInterval) and (ttl + campaignInterval) when the leader stalls", async () => {
    const leaseTtlMs = 400;
    const renewIntervalMs = 100;
    const campaignIntervalMs = 25;
    const opts = { leaseTtlMs, renewIntervalMs, campaignIntervalMs };

    const nodes = [spawn("n1", opts), spawn("n2", opts)];
    await waitFor(() => nodes.some((n) => n.election.isLeader), { label: "election" });

    const old = nodes.find((n) => n.election.isLeader)!;
    const fresh = nodes.find((n) => n !== old)!;

    const stalledAt = Date.now();
    old.election.simulateProcessStall(5_000);

    await waitFor(() => fresh.election.isLeader, { label: "failover" });
    const failoverMs = Date.now() - stalledAt;

    console.log(`failover took ${failoverMs}ms (lease ${leaseTtlMs}ms)`);
    // The lease had at least (ttl - renewInterval) left when the stall began.
    expect(failoverMs).toBeGreaterThanOrEqual(leaseTtlMs - renewIntervalMs - 20);
    // And at most one full lease plus a campaign round trip. 200ms of slack
    // for a loaded machine; the point is the shape, not the constant.
    expect(failoverMs).toBeLessThanOrEqual(leaseTtlMs + campaignIntervalMs + 200);
  });
});

describe("the split-brain window, measured", () => {
  it("lasts as long as the old leader's process is stalled, not one lease TTL", async () => {
    const leaseTtlMs = 400;
    const renewIntervalMs = 100;
    const stallMs = 1_500;
    const opts = { leaseTtlMs, renewIntervalMs, campaignIntervalMs: 25 };

    const nodes = [spawn("n1", opts), spawn("n2", opts)];
    await waitFor(() => nodes.some((n) => n.election.isLeader), { label: "election" });

    const old = nodes.find((n) => n.election.isLeader)!;
    const fresh = nodes.find((n) => n !== old)!;
    const oldEpoch = old.election.epoch!;

    old.election.simulateProcessStall(stallMs);

    await waitFor(() => fresh.election.isLeader, { label: "failover" });
    const bothBelieveFrom = Date.now();
    const freshEpoch = fresh.election.epoch!;

    // Two processes, both convinced they are the leader. This is not a bug in
    // the implementation; it is what a lease-based election is.
    expect(old.election.isLeader).toBe(true);
    expect(fresh.election.isLeader).toBe(true);
    expect(freshEpoch).toBeGreaterThan(oldEpoch);

    // The new leader records its epoch at the resource...
    expect((await store.write("schedule", "run-by-n2", freshEpoch)).accepted).toBe(true);

    // ...and the stalled leader's work is refused from that point on, even
    // though it is still running, still confident, and still holds a lease
    // object that says it is the leader.
    const staleWrite = await store.write("schedule", "run-by-n1", oldEpoch);
    expect(staleWrite).toEqual({
      accepted: false,
      rejectedToken: oldEpoch,
      highestSeenToken: freshEpoch,
    });
    expect(await store.read("schedule")).toBe("run-by-n2");

    // Now wait for the stalled leader to come back and find out.
    await waitFor(() => !old.election.isLeader, {
      timeoutMs: stallMs + 2_000,
      label: "old leader noticing",
    });
    const splitBrainMs = Date.now() - bothBelieveFrom;

    console.log(
      `split-brain window: ${splitBrainMs}ms (lease TTL ${leaseTtlMs}ms, stall ${stallMs}ms)`,
    );
    expect(old.demotions).toContain("lease-lost");

    // The interesting assertion: the window is far longer than one lease TTL.
    // "Two nodes can overlap for at most one TTL" is the intuition, and it is
    // wrong - the overlap is bounded by how long the old leader was unable to
    // check, which nothing in Redis controls.
    expect(splitBrainMs).toBeGreaterThan(leaseTtlMs);
    expect(splitBrainMs).toBeLessThanOrEqual(stallMs + renewIntervalMs + 400);
  });
});
