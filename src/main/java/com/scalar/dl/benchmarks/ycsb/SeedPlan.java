package com.scalar.dl.benchmarks.ycsb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Deterministic seeding plan that distributes target assets uniformly over the loaded key space
 * [0, totalAssets) and groups them into contract executions.
 *
 * <p>With W seeded workloads and M assets per workload, the total slot count is S = M * W and slot
 * j (0 &lt;= j &lt; S) maps to key floor(j * N / S). When both workloads are seeded, even slots
 * belong to F and odd slots to C, so each workload is itself uniformly distributed over the whole
 * key space. Ops per transaction (K) groups K consecutive slots of the same workload into one
 * execution.
 */
public final class SeedPlan {

  public enum Workload {
    F,
    C
  }

  /** A single contract execution: K user IDs of one workload sharing one transaction. */
  public static final class Execution {
    private final Workload workload;
    private final int index;
    private final long[] userIds;

    private Execution(Workload workload, int index, long[] userIds) {
      this.workload = workload;
      this.index = index;
      this.userIds = userIds;
    }

    public Workload getWorkload() {
      return workload;
    }

    public int getIndex() {
      return index;
    }

    public long[] getUserIds() {
      return userIds;
    }
  }

  private final List<Workload> workloads;
  private final long numAssets;
  private final long totalAssets;
  private final int opsPerTx;
  private final long skipExecutions;

  private SeedPlan(
      List<Workload> workloads,
      long numAssets,
      long totalAssets,
      int opsPerTx,
      long skipExecutions) {
    this.workloads = workloads;
    this.numAssets = numAssets;
    this.totalAssets = totalAssets;
    this.opsPerTx = opsPerTx;
    this.skipExecutions = skipExecutions;
  }

  public static SeedPlan of(
      List<Workload> workloads, long numAssets, long totalAssets, int opsPerTx) {
    return of(workloads, numAssets, totalAssets, opsPerTx, 0);
  }

  /**
   * @param skipExecutions how many executions to drop from the front of the planned sequence, to
   *     resume an interrupted run without seeding its assets a second time
   */
  public static SeedPlan of(
      List<Workload> workloads,
      long numAssets,
      long totalAssets,
      int opsPerTx,
      long skipExecutions) {
    if (workloads.isEmpty()) {
      throw new IllegalArgumentException("at least one workload must be specified");
    }
    if (numAssets < 1) {
      throw new IllegalArgumentException("--num-assets must be >= 1, but was " + numAssets);
    }
    if (totalAssets < 1) {
      throw new IllegalArgumentException("--total-assets must be >= 1, but was " + totalAssets);
    }
    if (opsPerTx < 1) {
      throw new IllegalArgumentException("--ops-per-tx must be >= 1, but was " + opsPerTx);
    }
    if (numAssets % opsPerTx != 0) {
      throw new IllegalArgumentException(
          "--num-assets ("
              + numAssets
              + ") must be divisible by --ops-per-tx ("
              + opsPerTx
              + ") to keep the seeded counts exact");
    }
    int numWorkloads = workloads.size();
    if (numAssets > Long.MAX_VALUE / numWorkloads
        || totalAssets > Long.MAX_VALUE / (numAssets * numWorkloads)) {
      throw new IllegalArgumentException(
          "--num-assets x --total-assets is too large to compute key slots");
    }
    long slots = numAssets * numWorkloads;
    if (slots > totalAssets) {
      throw new IllegalArgumentException(
          "--num-assets ("
              + numAssets
              + ") x "
              + numWorkloads
              + " workload(s) exceeds --total-assets ("
              + totalAssets
              + "); keys would not be distinct");
    }
    if (numAssets / opsPerTx > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "the number of executions per workload (--num-assets / --ops-per-tx) is too large");
    }
    if (skipExecutions < 0) {
      throw new IllegalArgumentException(
          "--skip-executions must be >= 0, but was " + skipExecutions);
    }
    long totalExecutions = numAssets / opsPerTx * workloads.size();
    if (skipExecutions >= totalExecutions) {
      throw new IllegalArgumentException(
          "--skip-executions ("
              + skipExecutions
              + ") leaves nothing to seed; the plan has "
              + totalExecutions
              + " executions in total");
    }
    return new SeedPlan(
        Collections.unmodifiableList(new ArrayList<>(workloads)),
        numAssets,
        totalAssets,
        opsPerTx,
        skipExecutions);
  }

  public List<Workload> getWorkloads() {
    return workloads;
  }

  public long getNumAssets() {
    return numAssets;
  }

  public long getTotalAssets() {
    return totalAssets;
  }

  public int getOpsPerTx() {
    return opsPerTx;
  }

  public long getSkipExecutions() {
    return skipExecutions;
  }

  /** How many leading executions of the given workload {@code --skip-executions} drops. */
  public int getSkippedExecutions(Workload workload) {
    int workloadIndex = workloads.indexOf(workload);
    if (workloadIndex < 0) {
      throw new IllegalArgumentException("workload " + workload + " is not part of this plan");
    }
    return skippedFor(workloadIndex);
  }

  /**
   * The workloads run one after another, so the skip budget is spent on the earlier ones first: a
   * skip that outlasts the first workload carries the remainder into the next.
   */
  private int skippedFor(int workloadIndex) {
    int numExecutions = (int) (numAssets / opsPerTx);
    long remaining = skipExecutions - (long) workloadIndex * numExecutions;
    if (remaining <= 0) {
      return 0;
    }
    return (int) Math.min(remaining, numExecutions);
  }

  /**
   * Returns the executions of the given workload, K consecutive own-slots per execution, minus the
   * ones {@code --skip-executions} drops. The dropped ones keep their place in the numbering, so an
   * execution's index and keys are the same as they would have been in the interrupted run.
   */
  public List<Execution> executionsFor(Workload workload) {
    int workloadIndex = workloads.indexOf(workload);
    if (workloadIndex < 0) {
      throw new IllegalArgumentException("workload " + workload + " is not part of this plan");
    }
    int numWorkloads = workloads.size();
    long slots = numAssets * numWorkloads;
    int numExecutions = (int) (numAssets / opsPerTx);
    int skipped = skippedFor(workloadIndex);
    List<Execution> executions = new ArrayList<>(numExecutions - skipped);
    for (int i = skipped; i < numExecutions; i++) {
      long[] userIds = new long[opsPerTx];
      for (int k = 0; k < opsPerTx; k++) {
        long ownSlot = (long) i * opsPerTx + k;
        long globalSlot = ownSlot * numWorkloads + workloadIndex;
        userIds[k] = globalSlot * totalAssets / slots;
      }
      executions.add(new Execution(workload, i, userIds));
    }
    return executions;
  }

  /** Human-readable description of the key distribution of the given workload, for the summary. */
  public String describeKeys(Workload workload) {
    int numWorkloads = workloads.size();
    long slots = numAssets * numWorkloads;
    long stride = totalAssets / slots;
    String slotDescription;
    if (numWorkloads == 1) {
      slotDescription = "all slots";
    } else {
      slotDescription = workloads.indexOf(workload) == 0 ? "even slots" : "odd slots";
    }
    int skipped = skippedFor(workloads.indexOf(workload));
    return (numAssets - (long) skipped * opsPerTx)
        + " of [0, "
        + totalAssets
        + "), uniformly distributed (stride "
        + stride
        + ", "
        + slotDescription
        + (skipped == 0 ? "" : ", resuming after the first " + skipped + " executions")
        + ")";
  }
}
