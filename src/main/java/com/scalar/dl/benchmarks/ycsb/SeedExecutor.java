package com.scalar.dl.benchmarks.ycsb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.dl.benchmarks.ycsb.SeedPlan.Execution;
import com.scalar.dl.benchmarks.ycsb.SeedPlan.Workload;
import com.scalar.dl.benchmarks.ycsb.contract.Const;
import com.scalar.dl.client.exception.ClientException;
import com.scalar.dl.ledger.service.StatusCode;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes the planned contract executions against a broken coordinator and classifies the
 * results. Every execution is EXPECTED to fail with {@code UNKNOWN_TRANSACTION_STATUS} (501); a
 * successful commit or any other error triggers fail-fast: no new executions are submitted,
 * in-flight ones are drained, and every slot is accounted for in the result.
 */
public class SeedExecutor {

  /** Runs one contract execution. Implementations wrap {@code ClientService#executeContract}. */
  public interface ContractRunner {
    void run(String contractId, JsonNode argument);
  }

  /** Per-workload tally. All executions of the workload fall into exactly one category. */
  public static final class WorkloadResult {
    private final long expected;
    private final long unexpectedSuccesses;
    private final long unexpectedErrors;
    private final long notStarted;
    private final String firstFailure;

    WorkloadResult(
        long expected,
        long unexpectedSuccesses,
        long unexpectedErrors,
        long notStarted,
        String firstFailure) {
      this.expected = expected;
      this.unexpectedSuccesses = unexpectedSuccesses;
      this.unexpectedErrors = unexpectedErrors;
      this.notStarted = notStarted;
      this.firstFailure = firstFailure;
    }

    public long getExpected() {
      return expected;
    }

    public long getUnexpectedSuccesses() {
      return unexpectedSuccesses;
    }

    public long getUnexpectedErrors() {
      return unexpectedErrors;
    }

    public long getNotStarted() {
      return notStarted;
    }

    /** Details of the execution that triggered fail-fast, or null if all went as expected. */
    public String getFirstFailure() {
      return firstFailure;
    }

    public boolean isAllExpected() {
      return unexpectedSuccesses == 0 && unexpectedErrors == 0 && notStarted == 0;
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(SeedExecutor.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int PAYLOAD_SIZE = 100;
  private static final int PROGRESS_INTERVAL = 1000;

  private final ContractRunner runner;
  private final int concurrency;
  private final AtomicBoolean stopped = new AtomicBoolean();
  private volatile ExecutorService currentPool;

  public SeedExecutor(ContractRunner runner, int concurrency) {
    if (concurrency < 1) {
      throw new IllegalArgumentException("--concurrency must be >= 1, but was " + concurrency);
    }
    this.runner = runner;
    this.concurrency = concurrency;
  }

  /**
   * Requests fail-fast: no new executions start; in-flight ones are drained. Used by fail-fast
   * itself and by the shutdown hook on Ctrl-C / SIGTERM.
   */
  public void requestStop() {
    stopped.set(true);
  }

  public boolean isStopped() {
    return stopped.get();
  }

  /**
   * Waits until the in-flight executions have drained, up to the given timeout. Returns true if
   * drained. Meant for the shutdown hook; no-op when nothing is running.
   */
  public boolean awaitDrain(long timeoutSeconds) throws InterruptedException {
    ExecutorService pool = currentPool;
    return pool == null || pool.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
  }

  /** Runs all executions of one workload and blocks until every slot is accounted for. */
  public WorkloadResult runWorkload(String contractId, List<Execution> executions)
      throws InterruptedException {
    AtomicLong expected = new AtomicLong();
    AtomicLong unexpectedSuccesses = new AtomicLong();
    AtomicLong unexpectedErrors = new AtomicLong();
    AtomicLong notStarted = new AtomicLong();
    AtomicReference<String> firstFailure = new AtomicReference<>();

    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
    currentPool = pool;
    try {
      for (Execution execution : executions) {
        pool.execute(
            () -> runOne(execution, contractId, expected, unexpectedSuccesses, unexpectedErrors,
                notStarted, firstFailure));
      }
    } finally {
      pool.shutdown();
      while (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
        logger.info("waiting for in-flight executions to drain...");
      }
    }
    return new WorkloadResult(
        expected.get(),
        unexpectedSuccesses.get(),
        unexpectedErrors.get(),
        notStarted.get(),
        firstFailure.get());
  }

  private void runOne(
      Execution execution,
      String contractId,
      AtomicLong expected,
      AtomicLong unexpectedSuccesses,
      AtomicLong unexpectedErrors,
      AtomicLong notStarted,
      AtomicReference<String> firstFailure) {
    if (stopped.get()) {
      notStarted.incrementAndGet();
      return;
    }
    JsonNode argument = buildArgument(execution);
    try {
      runner.run(contractId, argument);
      // The transaction committed: the coordinator is writable, i.e., the fault injection is not
      // in place. Continuing would break the seeded-count guarantees.
      firstFailure.compareAndSet(null, describe(execution, "unexpected success: the transaction "
          + "committed; the coordinator seems writable (fault injection not in place?)"));
      unexpectedSuccesses.incrementAndGet();
      requestStop();
    } catch (ClientException e) {
      if (e.getStatusCode() == StatusCode.UNKNOWN_TRANSACTION_STATUS) {
        long count = expected.incrementAndGet();
        if (count % PROGRESS_INTERVAL == 0) {
          logger.info(
              "workload {}: {} executions failed as expected", execution.getWorkload(), count);
        }
      } else {
        firstFailure.compareAndSet(
            null, describe(execution, "unexpected error: status=" + e.getStatusCode() + ", "
                + e.getMessage()));
        unexpectedErrors.incrementAndGet();
        requestStop();
      }
    } catch (RuntimeException e) {
      firstFailure.compareAndSet(null, describe(execution, "unexpected error: " + e));
      unexpectedErrors.incrementAndGet();
      requestStop();
    }
  }

  private static JsonNode buildArgument(Execution execution) {
    ObjectNode argument = MAPPER.createObjectNode();
    ArrayNode userIds = MAPPER.createArrayNode();
    for (long userId : execution.getUserIds()) {
      userIds.add(userId);
    }
    argument.set(Const.KEY_USER_IDS, userIds);
    if (execution.getWorkload() == Workload.F) {
      ArrayNode payloads = MAPPER.createArrayNode();
      char[] payload = new char[PAYLOAD_SIZE];
      for (int i = 0; i < execution.getUserIds().length; i++) {
        YcsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
        payloads.add(new String(payload));
      }
      argument.set(Const.KEY_PAYLOADS, payloads);
    }
    return argument;
  }

  private static String describe(Execution execution, String message) {
    return "workload "
        + execution.getWorkload()
        + ", execution #"
        + execution.getIndex()
        + ", user_ids "
        + Arrays.toString(execution.getUserIds())
        + ": "
        + message;
  }
}
