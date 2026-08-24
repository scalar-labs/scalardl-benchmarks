package com.scalar.dl.benchmarks.ycsb;

import com.scalar.dl.benchmarks.ycsb.SeedExecutor.WorkloadResult;
import com.scalar.dl.benchmarks.ycsb.SeedPlan.Workload;
import com.scalar.dl.client.config.ClientConfig;
import com.scalar.dl.client.service.ClientService;
import com.scalar.dl.client.service.ClientServiceFactory;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Option;

/**
 * Test-only command that seeds recovery-needed states for scalardl-cleanup testing: it executes
 * the YCSB contracts registered by YcsbLoader while the ScalarDB coordinator is intentionally made
 * unwritable, so that every execution fails with UNKNOWN_TRANSACTION_STATUS (501) and leaves
 * PREPARED records on the Ledger (workload F) and asset locks on the Auditor (F: write, C: read)
 * in exact, countable numbers.
 */
@Command(
    name = "recovery-seeder",
    mixinStandardHelpOptions = true,
    description = {
      "Seeds recovery-needed states for scalardl-cleanup testing by executing the YCSB "
          + "contracts (registered by YcsbLoader) while the ScalarDB coordinator is intentionally "
          + "made unwritable (e.g., the Cosmos DB stored procedure of the coordinator container "
          + "is deleted, or the MySQL coordinator state table is dropped).",
      "Every execution is EXPECTED to fail with UNKNOWN_TRANSACTION_STATUS (501). Workload F "
          + "leaves 2 PREPARED records (asset + asset_metadata) and 1 auditor write lock per "
          + "asset; workload C leaves 1 auditor read lock per asset.",
      "NOTE: keys are assigned deterministically, so re-running with the same parameters after a "
          + "partial failure seeds the same assets twice and corrupts the counts. Resume with "
          + "--skip-executions instead, or resolve all seeded states with the cleanup tools "
          + "before seeding again."
    })
public class RecoveryStateSeeder implements Callable<Integer> {

  enum WorkloadOption {
    C,
    F,
    BOTH
  }

  private static final long SHUTDOWN_DRAIN_SECONDS = 30;
  private static final long SUMMARY_WAIT_SECONDS = 10;

  private final CountDownLatch summaryPrinted = new CountDownLatch(1);

  @Option(
      names = "--properties",
      required = true,
      paramLabel = "<file>",
      description = "ScalarDL client.properties.")
  private File properties;

  @Option(
      names = "--num-assets",
      required = true,
      paramLabel = "<M>",
      description =
          "Number of assets to drive into a recovery-needed state PER workload (BOTH seeds M "
              + "assets with F and another M with C).")
  private long numAssets;

  @Option(
      names = "--total-assets",
      required = true,
      paramLabel = "<N>",
      description =
          "Total number of assets loaded by YcsbLoader. Seeded keys are distributed uniformly "
              + "over [0, N).")
  private long totalAssets;

  @Option(
      names = "--workload",
      defaultValue = "BOTH",
      paramLabel = "<C|F|BOTH>",
      description = "Workload(s) to seed with (default: ${DEFAULT-VALUE}).")
  private WorkloadOption workload;

  @Option(
      names = "--ops-per-tx",
      defaultValue = "1",
      paramLabel = "<K>",
      description =
          "Number of assets per contract execution; the K assets of one execution share one "
              + "transaction ID (default: ${DEFAULT-VALUE}).")
  private int opsPerTx;

  @Option(
      names = "--concurrency",
      defaultValue = "1",
      paramLabel = "<n>",
      description = "Number of client threads (default: ${DEFAULT-VALUE}).")
  private int concurrency;

  @Option(
      names = "--skip-executions",
      defaultValue = "0",
      paramLabel = "<n>",
      description =
          "Resume an interrupted run: skip the first n executions of the planned sequence "
              + "(workload F first, then C), which the previous run already seeded. Pass its "
              + "planned minus not-started, summed over the workloads. Every other option must "
              + "match that run, or the keys shift. Only exact if it used --concurrency 1, since "
              + "only then are the executions it started a contiguous prefix "
              + "(default: ${DEFAULT-VALUE}).")
  private long skipExecutions;

  @Option(
      names = "--contract-id-c",
      defaultValue = "C",
      paramLabel = "<id>",
      description =
          "Contract ID of WorkloadC; matches the YcsbLoader default registration "
              + "(default: ${DEFAULT-VALUE}).")
  private String contractIdC;

  @Option(
      names = "--contract-id-f",
      defaultValue = "F",
      paramLabel = "<id>",
      description =
          "Contract ID of WorkloadF; matches the YcsbLoader default registration "
              + "(default: ${DEFAULT-VALUE}).")
  private String contractIdF;

  public static void main(String[] args) {
    System.exit(new CommandLine(new RecoveryStateSeeder()).execute(args));
  }

  @Override
  public Integer call() throws Exception {
    // Validate everything before touching the network so that option errors are testable and
    // fail with the usage exit code.
    SeedPlan plan;
    ClientConfig clientConfig;
    try {
      if (concurrency < 1) {
        throw new IllegalArgumentException("--concurrency must be >= 1, but was " + concurrency);
      }
      if (!properties.isFile() || !properties.canRead()) {
        throw new IllegalArgumentException(
            "--properties file does not exist or is not readable: " + properties);
      }
      plan =
          SeedPlan.of(
              toWorkloads(workload), numAssets, totalAssets, opsPerTx, skipExecutions);
      clientConfig = new ClientConfig(properties);
    } catch (IllegalArgumentException | IOException e) {
      System.err.println("Error: " + e.getMessage());
      return ExitCode.USAGE;
    }

    boolean auditorEnabled = clientConfig.isAuditorEnabled();
    if (!auditorEnabled) {
      if (plan.getWorkloads().equals(Collections.singletonList(Workload.C))) {
        System.err.println(
            "Error: the auditor is disabled in the client properties; workload C would leave "
                + "nothing to recover (read-only executions leave no prepared records). "
                + "Enable the auditor or seed with workload F.");
        return ExitCode.USAGE;
      }
      System.err.println(
          "Warning: the auditor is disabled in the client properties; no asset locks will be "
              + "left, only the Ledger-side PREPARED records of workload F.");
    }

    ClientServiceFactory factory = new ClientServiceFactory();
    try {
      ClientService service = factory.create(clientConfig);
      SeedExecutor executor = new SeedExecutor(service::executeContract, concurrency);
      Thread shutdownHook = new Thread(() -> drainOnShutdown(executor));
      Runtime.getRuntime().addShutdownHook(shutdownHook);

      Map<Workload, WorkloadResult> results = new LinkedHashMap<>();
      try {
        for (Workload w : plan.getWorkloads()) {
          if (executor.isStopped()) {
            break; // fail-fast in a previous workload; skip the rest entirely
          }
          results.put(w, executor.runWorkload(contractIdFor(w), plan.executionsFor(w)));
        }
      } finally {
        // The summary is the only record of what was written to the database, so print it on
        // every exit path, including an interrupted run.
        removeShutdownHookQuietly(shutdownHook);
        printSummary(plan, results, auditorEnabled);
        summaryPrinted.countDown();
      }
      boolean allExpected =
          results.size() == plan.getWorkloads().size()
              && results.values().stream().allMatch(WorkloadResult::isAllExpected);
      return allExpected ? ExitCode.OK : ExitCode.SOFTWARE;
    } finally {
      factory.close();
    }
  }

  private static List<Workload> toWorkloads(WorkloadOption option) {
    switch (option) {
      case C:
        return Collections.singletonList(Workload.C);
      case F:
        return Collections.singletonList(Workload.F);
      case BOTH:
      default:
        // F first so that it takes the even slots, then C on the odd slots.
        return Arrays.asList(Workload.F, Workload.C);
    }
  }

  private String contractIdFor(Workload workload) {
    return workload == Workload.F ? contractIdF : contractIdC;
  }

  private void drainOnShutdown(SeedExecutor executor) {
    executor.requestStop();
    System.err.println(
        "Interrupted: draining in-flight executions (up to "
            + SHUTDOWN_DRAIN_SECONDS
            + " seconds). The seeded counts are NOT guaranteed; resolve all states with the "
            + "cleanup tools before seeding again.");
    try {
      executor.awaitDrain(SHUTDOWN_DRAIN_SECONDS);
      // The JVM halts as soon as every shutdown hook returns, which would kill the main thread
      // before it can print the summary. Hold the hook open until the summary is out.
      summaryPrinted.await(SUMMARY_WAIT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Removes the hook, tolerating the {@link IllegalStateException} that {@code removeShutdownHook}
   * raises once shutdown is already in progress (the Ctrl-C path).
   */
  private static void removeShutdownHookQuietly(Thread shutdownHook) {
    try {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    } catch (IllegalStateException e) {
      // Shutdown already started; the hook is waiting for the summary below.
    }
  }

  private void printSummary(
      SeedPlan plan, Map<Workload, WorkloadResult> results, boolean auditorEnabled) {
    System.out.println();
    if (plan.getSkipExecutions() > 0) {
      System.out.println(
          "resumed run: the first "
              + plan.getSkipExecutions()
              + " executions of the plan were skipped, so everything below counts this run only; "
              + "add it to what the interrupted run seeded");
    }
    for (Workload w : plan.getWorkloads()) {
      WorkloadResult result = results.get(w);
      if (result == null) {
        System.out.println("workload " + w + ": not run (fail-fast or interruption)");
        continue;
      }
      if (result.getPlanned() == 0 && plan.getSkippedExecutions(w) > 0) {
        System.out.println(
            "workload " + w + ": already seeded in full by the interrupted run; nothing to do");
        continue;
      }
      long seededAssets = result.getExpected() * plan.getOpsPerTx();
      if (result.isAllExpected()) {
        System.out.println(
            "workload "
                + w
                + ": "
                + result.getPlanned()
                + " executions (K="
                + plan.getOpsPerTx()
                + "), all failed as expected (UNKNOWN_TRANSACTION_STATUS)");
      } else {
        System.out.println(
            "workload "
                + w
                + ": "
                + result.getPlanned()
                + " executions planned (K="
                + plan.getOpsPerTx()
                + "): expected-failure="
                + result.getExpected()
                + ", unexpected-success="
                + result.getUnexpectedSuccesses()
                + ", unexpected-error="
                + result.getUnexpectedErrors()
                + ", not-started="
                + result.getNotStarted());
      }
      if (result.getAccounted() != result.getPlanned()) {
        System.out.println(
            "  -> WARNING: only "
                + result.getAccounted()
                + " of "
                + result.getPlanned()
                + " executions were accounted for; the seeded counts below are NOT reliable");
      }
      System.out.println("  -> " + expectation(w, seededAssets, auditorEnabled));
      if (result.isAllExpected()) {
        System.out.println("  -> keys: " + plan.describeKeys(w));
      } else {
        System.out.println(
            "  -> keys: planned "
                + plan.describeKeys(w)
                + "; the subset actually touched is indeterminate because the run stopped early");
      }
      if (result.getFirstFailure() != null) {
        System.out.println("  -> first failure: " + result.getFirstFailure());
      }
    }
    boolean anyUnexpectedError =
        results.values().stream().anyMatch(r -> r.getUnexpectedErrors() > 0);
    if (anyUnexpectedError) {
      System.out.println(
          "Hint: verify that --total-assets matches the number of records loaded by YcsbLoader, "
              + "that the contract IDs are registered, and that the coordinator is actually made "
              + "unwritable.");
    }
  }

  private static String expectation(Workload workload, long seededAssets, boolean auditorEnabled) {
    if (workload == Workload.F) {
      String locks = auditorEnabled ? ", " + seededAssets + " write locks" : "";
      return "expected: "
          + (2 * seededAssets)
          + " prepared records (asset + asset_metadata)"
          + locks;
    }
    return auditorEnabled
        ? "expected: " + seededAssets + " read locks"
        : "expected: nothing (auditor disabled)";
  }
}
