import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    // Every test talks to one real Redis on 56380. Running files in parallel
    // would let one file's FLUSHDB wipe another file's keys mid-assertion.
    fileParallelism: false,
    // Timing tests wait on real TTLs, so the default 5s is too tight.
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
