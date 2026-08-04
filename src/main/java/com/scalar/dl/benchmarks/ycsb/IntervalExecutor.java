package com.scalar.dl.benchmarks.ycsb;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.dl.benchmarks.ycsb.RandomWorkloadGenerator.Request;
import com.scalar.dl.client.exception.ClientException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs generated contract executions one at a time at a fixed interval, until the duration elapses
 * or a stop is requested, and tallies the outcome of every execution.
 *
 * <p>Unlike {@link SeedExecutor}, a failure is not fatal: the traffic is the point, so the loop
 * keeps going and the failures are reported per outcome in the result.
 */
public class IntervalExecutor {

  /** Runs one contract execution. Implementations wrap {@code ClientService#executeContract}. */
  public interface ContractRunner {
    void run(String contractId, JsonNode argument);
  }

  /** Time source and interruptible wait; virtualized in tests. */
  interface Pacer {
    long nanoTime();

    /** Waits up to the given duration, returning early once a stop is requested. */
    void await(long timeoutNanos) throws InterruptedException;
  }

  /** Per-contract tally. Every execution is either a success or a failure of one outcome. */
  public static final class ContractStats {
    private long successes;
    private final Map<String, Long> failures = new LinkedHashMap<>();

    private void recordSuccess() {
      successes++;
    }

    /** Records a failure and returns how many times this outcome has occurred so far. */
    private long recordFailure(String outcome) {
      Long count = failures.get(outcome);
      long updated = count == null ? 1 : count + 1;
      failures.put(outcome, updated);
      return updated;
    }

    public long getSuccesses() {
      return successes;
    }

    public long getFailures() {
      long total = 0;
      for (long count : failures.values()) {
        total += count;
      }
      return total;
    }

    public long getExecutions() {
      return successes + getFailures();
    }

    /** Failure counts by outcome (status code name, or exception class), in first-seen order. */
    public Map<String, Long> getFailuresByOutcome() {
      return Collections.unmodifiableMap(failures);
    }
  }

  /** Outcome of one run. */
  public static final class Result {
    private final Map<String, ContractStats> statsByContract;
    private final long behindSchedule;
    private final long elapsedNanos;

    private Result(
        Map<String, ContractStats> statsByContract, long behindSchedule, long elapsedNanos) {
      this.statsByContract = Collections.unmodifiableMap(statsByContract);
      this.behindSchedule = behindSchedule;
      this.elapsedNanos = elapsedNanos;
    }

    /** Per-contract tallies, keyed by contract ID in the order the generator reports them. */
    public Map<String, ContractStats> getStatsByContract() {
      return statsByContract;
    }

    /**
     * Number of executions that finished after the next one was already due. A non-zero value means
     * the actual rate was below the configured interval.
     */
    public long getBehindSchedule() {
      return behindSchedule;
    }

    public long getElapsedNanos() {
      return elapsedNanos;
    }

    public long getExecutions() {
      return sum(true, true);
    }

    public long getSuccesses() {
      return sum(true, false);
    }

    public long getFailures() {
      return sum(false, true);
    }

    private long sum(boolean successes, boolean failures) {
      long total = 0;
      for (ContractStats stats : statsByContract.values()) {
        total += (successes ? stats.getSuccesses() : 0) + (failures ? stats.getFailures() : 0);
      }
      return total;
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(IntervalExecutor.class);
  private static final long PROGRESS_INTERVAL = 100;

  private final ContractRunner runner;
  private final RandomWorkloadGenerator generator;
  private final long intervalNanos;
  private final long durationNanos;
  private final Pacer pacer;
  private final AtomicBoolean stopped = new AtomicBoolean();
  private final CountDownLatch stopLatch = new CountDownLatch(1);

  /**
   * @param intervalMillis interval between the start of consecutive executions
   * @param durationSeconds how long to run; 0 runs until {@link #requestStop()} is called
   */
  public IntervalExecutor(
      ContractRunner runner,
      RandomWorkloadGenerator generator,
      long intervalMillis,
      long durationSeconds) {
    this(runner, generator, intervalMillis, durationSeconds, null);
  }

  IntervalExecutor(
      ContractRunner runner,
      RandomWorkloadGenerator generator,
      long intervalMillis,
      long durationSeconds,
      Pacer pacer) {
    if (intervalMillis < 1) {
      throw new IllegalArgumentException(
          "--interval-millis must be >= 1, but was " + intervalMillis);
    }
    if (durationSeconds < 0) {
      throw new IllegalArgumentException(
          "--duration-seconds must be >= 0, but was " + durationSeconds);
    }
    this.runner = runner;
    this.generator = generator;
    this.intervalNanos = TimeUnit.MILLISECONDS.toNanos(intervalMillis);
    this.durationNanos = TimeUnit.SECONDS.toNanos(durationSeconds);
    this.pacer = pacer != null ? pacer : new LatchPacer();
  }

  /** Requests the loop to stop after the in-flight execution; wakes it out of its wait. */
  public void requestStop() {
    stopped.set(true);
    stopLatch.countDown();
  }

  public boolean isStopped() {
    return stopped.get();
  }

  /**
   * Runs until the duration elapses or a stop is requested. Always returns the tally of what was
   * executed, including when the calling thread is interrupted.
   */
  public Result run() {
    Map<String, ContractStats> statsByContract = new LinkedHashMap<>();
    for (String contractId : generator.getContractIds()) {
      statsByContract.put(contractId, new ContractStats());
    }
    long start = pacer.nanoTime();
    long deadline = start;
    long behindSchedule = 0;
    long executions = 0;
    long failures = 0;
    while (!stopped.get()) {
      long now = pacer.nanoTime();
      if (durationNanos > 0 && now - start >= durationNanos) {
        break;
      }
      Request request;
      try {
        request = generator.next();
      } catch (Throwable t) {
        // Generating a request cannot fail because of the target, so this is a client-side defect
        // or an exhausted heap: stop rather than spin on it, and return normally so that the tally
        // of everything executed so far still reaches the summary.
        logger.error("failed to generate the next execution; stopping", t);
        requestStop();
        break;
      }
      executions++;
      ContractStats stats =
          statsByContract.computeIfAbsent(request.getContractId(), id -> new ContractStats());
      if (!runOne(request, stats)) {
        failures++;
      }
      if (executions % PROGRESS_INTERVAL == 0) {
        logger.info("{} executions so far ({} failed)", executions, failures);
      }

      deadline += intervalNanos;
      long afterRun = pacer.nanoTime();
      if (afterRun >= deadline) {
        // The execution outlasted its interval. Resync instead of firing the next ones back to
        // back, which would turn a slow target into a burst of load.
        behindSchedule++;
        deadline = afterRun;
        continue;
      }
      long waitNanos = deadline - afterRun;
      if (durationNanos > 0) {
        // Never wait past the end of the run: an execution that outlasted the remaining duration
        // would otherwise keep the command alive for one more interval.
        waitNanos = Math.max(0, Math.min(waitNanos, start + durationNanos - afterRun));
      }
      try {
        pacer.await(waitNanos);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        requestStop();
      }
    }
    return new Result(statsByContract, behindSchedule, pacer.nanoTime() - start);
  }

  /** Runs one execution and records its outcome. Returns true when it succeeded. */
  private boolean runOne(Request request, ContractStats stats) {
    try {
      runner.run(request.getContractId(), request.getArgument());
      stats.recordSuccess();
      return true;
    } catch (ClientException e) {
      recordFailure(request, stats, e.getStatusCode().name(), e.getMessage());
    } catch (Throwable t) {
      // Throwable, not RuntimeException: an Error must not escape and kill the loop silently.
      recordFailure(request, stats, t.getClass().getSimpleName(), String.valueOf(t.getMessage()));
    }
    return false;
  }

  /** Records a failure, logging only its first occurrence per outcome to avoid log spam. */
  private void recordFailure(Request request, ContractStats stats, String outcome, String message) {
    if (stats.recordFailure(outcome) == 1) {
      logger.warn(
          "first {} on contract {} (user_ids {}): {}",
          outcome,
          request.getContractId(),
          Arrays.toString(request.getUserIds()),
          message);
    }
  }

  /** Real-time pacer whose wait ends as soon as a stop is requested. */
  private final class LatchPacer implements Pacer {
    @Override
    public long nanoTime() {
      return System.nanoTime();
    }

    @Override
    public void await(long timeoutNanos) throws InterruptedException {
      stopLatch.await(timeoutNanos, TimeUnit.NANOSECONDS);
    }
  }
}
