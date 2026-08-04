package com.scalar.dl.benchmarks.ycsb;

import com.scalar.dl.benchmarks.ycsb.IntervalExecutor.ContractStats;
import com.scalar.dl.benchmarks.ycsb.IntervalExecutor.Result;
import com.scalar.dl.client.config.ClientConfig;
import com.scalar.dl.client.service.ClientService;
import com.scalar.dl.client.service.ClientServiceFactory;
import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Option;

/**
 * Test-only command that generates a slow, steady trickle of YCSB contract executions against
 * uniformly random assets. It is meant to run alongside the scalardl-cleanup tools, so that they
 * are exercised while a client keeps touching the same key space (lazy recovery, contention), which
 * is what {@link RecoveryStateSeeder} deliberately avoids.
 *
 * <p>The two commands cover different halves of cleanup testing and must not be combined: reading a
 * seeded asset triggers lazy recovery, which resolves its state before the cleanup tools see it.
 * Verify a seeder run's counts on an idle key space; use this command, without seeding, to check
 * behaviour under concurrent traffic.
 */
@Command(
    name = "random-executor",
    mixinStandardHelpOptions = true,
    description = {
      "Executes the YCSB contracts (registered by YcsbLoader) on uniformly random assets, one "
          + "execution per interval, until the duration elapses or the command is interrupted.",
      "This is not a benchmark: the fixed interval keeps the load low on purpose, so that the "
          + "traffic can run alongside the scalardl-cleanup tools without dominating the target.",
      "Run this WITHOUT a recovery-seeder run in place: reading a seeded asset resolves its "
          + "recovery-needed state (lazy recovery) before the cleanup tools see it, which destroys "
          + "the counts that run is verified against.",
      "Failures are not fatal: every execution is tallied per outcome (status code) and the run "
          + "continues. Interrupting the command (Ctrl-C) prints the summary of what ran so far."
    })
public class RandomTrafficRunner implements Callable<Integer> {

  enum WorkloadOption {
    C,
    F,
    MIXED
  }

  private static final double DEFAULT_MIXED_READ_RATIO = 0.5;

  /**
   * Number of sequential gRPC calls one contract execution makes: with the auditor enabled it is
   * order (auditor), execute (ledger), validate (auditor); without it, execute alone. Used to bound
   * how long a stop can take, since an execution is only interruptible between the calls.
   */
  private static final int RPCS_PER_EXECUTION_WITH_AUDITOR = 3;

  /** Headroom on top of the RPC deadlines for printing the summary itself. */
  private static final long SUMMARY_WAIT_MARGIN_MILLIS = 5_000;

  private final CountDownLatch summaryPrinted = new CountDownLatch(1);

  @Option(
      names = "--properties",
      required = true,
      paramLabel = "<file>",
      description = "ScalarDL client.properties.")
  private File properties;

  @Option(
      names = "--total-assets",
      required = true,
      paramLabel = "<N>",
      description =
          "Total number of assets loaded by YcsbLoader. Keys are drawn uniformly from [0, N).")
  private long totalAssets;

  @Option(
      names = "--interval-millis",
      defaultValue = "100",
      paramLabel = "<ms>",
      description =
          "Interval between the start of consecutive executions (default: ${DEFAULT-VALUE}).")
  private long intervalMillis;

  @Option(
      names = "--duration-seconds",
      defaultValue = "0",
      paramLabel = "<s>",
      description =
          "Stop after this many seconds; 0 runs until interrupted (default: ${DEFAULT-VALUE}).")
  private long durationSeconds;

  @Option(
      names = "--workload",
      defaultValue = "MIXED",
      paramLabel = "<C|F|MIXED>",
      description =
          "Workload to execute: C (read-only), F (read-modify-write), or MIXED, which picks one "
              + "at random per execution (default: ${DEFAULT-VALUE}).")
  private WorkloadOption workload;

  @Option(
      names = "--read-ratio",
      paramLabel = "<r>",
      description =
          "Fraction of executions that run workload C, between 0 and 1. Only valid with "
              + "--workload MIXED (default: 0.5).")
  private Double readRatio;

  @Option(
      names = "--ops-per-tx",
      defaultValue = "1",
      paramLabel = "<K>",
      description =
          "Number of assets per contract execution; the K assets of one execution are distinct "
              + "and share one transaction (default: ${DEFAULT-VALUE}).")
  private int opsPerTx;

  @Option(
      names = "--payload-size",
      defaultValue = "1000",
      paramLabel = "<bytes>",
      description = "Payload size written by workload F (default: ${DEFAULT-VALUE}).")
  private int payloadSize;

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
    System.exit(new CommandLine(new RandomTrafficRunner()).execute(args));
  }

  @Override
  public Integer call() throws Exception {
    // Validate everything before touching the network so that option errors are testable and fail
    // with the usage exit code.
    RandomWorkloadGenerator generator;
    ClientConfig clientConfig;
    try {
      if (intervalMillis < 1) {
        throw new IllegalArgumentException(
            "--interval-millis must be >= 1, but was " + intervalMillis);
      }
      if (durationSeconds < 0) {
        throw new IllegalArgumentException(
            "--duration-seconds must be >= 0, but was " + durationSeconds);
      }
      if (!properties.isFile() || !properties.canRead()) {
        throw new IllegalArgumentException(
            "--properties file does not exist or is not readable: " + properties);
      }
      generator =
          new RandomWorkloadGenerator(
              contractIdC,
              contractIdF,
              resolveReadRatio(),
              totalAssets,
              opsPerTx,
              payloadSize,
              new Random());
      clientConfig = new ClientConfig(properties);
    } catch (IllegalArgumentException | IOException e) {
      System.err.println("Error: " + e.getMessage());
      return ExitCode.USAGE;
    }

    ClientServiceFactory factory = new ClientServiceFactory();
    try {
      ClientService service = factory.create(clientConfig);
      IntervalExecutor executor =
          new IntervalExecutor(
              service::executeContract, generator, intervalMillis, durationSeconds);
      long stopWaitSeconds = stopWaitSeconds(clientConfig);
      Thread shutdownHook = new Thread(() -> stopOnShutdown(executor, stopWaitSeconds));
      Runtime.getRuntime().addShutdownHook(shutdownHook);

      Result result = null;
      try {
        result = executor.run();
      } finally {
        try {
          // The summary is the only record of what ran, so print it on every exit path, including
          // an interrupted run. It is null only when run() threw, and that throw propagates.
          removeShutdownHookQuietly(shutdownHook);
          if (result != null) {
            printSummary(result);
          }
        } finally {
          // Own finally: the shutdown hook blocks until this fires, so nothing above may skip it.
          summaryPrinted.countDown();
        }
      }
      // Nothing succeeded at all: the target, the contract IDs or the loaded key space is wrong,
      // and the run generated no traffic worth testing against.
      return result.getExecutions() > 0 && result.getSuccesses() == 0
          ? ExitCode.SOFTWARE
          : ExitCode.OK;
    } finally {
      factory.close();
    }
  }

  /** Resolves the read ratio from --workload and --read-ratio, which must not contradict. */
  private double resolveReadRatio() {
    if (workload != WorkloadOption.MIXED && readRatio != null) {
      throw new IllegalArgumentException(
          "--read-ratio is only valid with --workload MIXED, but --workload was " + workload);
    }
    switch (workload) {
      case C:
        return 1;
      case F:
        return 0;
      case MIXED:
      default:
        return readRatio != null ? readRatio : DEFAULT_MIXED_READ_RATIO;
    }
  }

  /**
   * How long the shutdown hook may wait for the summary. A stop only takes effect between
   * executions, so the wait has to cover the gRPC deadlines of one whole execution: a hook that
   * returns while an execution is still in flight lets the JVM halt, which loses the summary —
   * the only record of what ran.
   */
  private static long stopWaitSeconds(ClientConfig clientConfig) {
    long deadlineMillis = clientConfig.getGrpcClientConfig().getDeadlineDurationMillis();
    long rpcs = clientConfig.isAuditorEnabled() ? RPCS_PER_EXECUTION_WITH_AUDITOR : 1;
    return TimeUnit.MILLISECONDS.toSeconds(rpcs * deadlineMillis + SUMMARY_WAIT_MARGIN_MILLIS);
  }

  private void stopOnShutdown(IntervalExecutor executor, long stopWaitSeconds) {
    executor.requestStop();
    System.err.println(
        "Interrupted: stopping after the in-flight execution (up to "
            + stopWaitSeconds
            + " seconds).");
    try {
      // The JVM halts as soon as every shutdown hook returns, which would kill the main thread
      // before it can print the summary. Hold the hook open until the summary is out.
      summaryPrinted.await(stopWaitSeconds, TimeUnit.SECONDS);
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

  private void printSummary(Result result) {
    double elapsedSeconds = result.getElapsedNanos() / 1e9;
    System.out.println();
    System.out.println(
        String.format(
            Locale.ROOT,
            "%d executions in %.1f seconds (%.1f/s, interval %d ms): %d succeeded, %d failed",
            result.getExecutions(),
            elapsedSeconds,
            elapsedSeconds > 0 ? result.getExecutions() / elapsedSeconds : 0,
            intervalMillis,
            result.getSuccesses(),
            result.getFailures()));
    for (Map.Entry<String, ContractStats> entry : result.getStatsByContract().entrySet()) {
      ContractStats stats = entry.getValue();
      System.out.println(
          "  contract "
              + entry.getKey()
              + ": "
              + stats.getExecutions()
              + " executions, "
              + stats.getSuccesses()
              + " succeeded, "
              + stats.getFailures()
              + " failed"
              + describeFailures(stats));
    }
    if (result.getBehindSchedule() > 0) {
      System.out.println(
          "  -> WARNING: "
              + result.getBehindSchedule()
              + " executions took longer than --interval-millis ("
              + intervalMillis
              + " ms), so the actual rate was lower than requested");
    }
    if (result.getExecutions() > 0 && result.getSuccesses() == 0) {
      System.out.println(
          "  -> WARNING: every execution failed; verify that --total-assets matches the number of "
              + "records loaded by YcsbLoader, that the contract IDs are registered, and that the "
              + "coordinator is writable");
    }
  }

  private static String describeFailures(ContractStats stats) {
    if (stats.getFailures() == 0) {
      return "";
    }
    StringBuilder builder = new StringBuilder(" (");
    String separator = "";
    for (Map.Entry<String, Long> entry : stats.getFailuresByOutcome().entrySet()) {
      builder.append(separator).append(entry.getKey()).append("=").append(entry.getValue());
      separator = ", ";
    }
    return builder.append(")").toString();
  }
}
