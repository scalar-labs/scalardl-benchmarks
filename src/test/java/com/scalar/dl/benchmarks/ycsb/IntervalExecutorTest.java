package com.scalar.dl.benchmarks.ycsb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.dl.benchmarks.ycsb.IntervalExecutor.ContractRunner;
import com.scalar.dl.benchmarks.ycsb.IntervalExecutor.ContractStats;
import com.scalar.dl.benchmarks.ycsb.IntervalExecutor.Result;
import com.scalar.dl.client.exception.ClientException;
import com.scalar.dl.ledger.service.StatusCode;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class IntervalExecutorTest {

  private static final long INTERVAL_MILLIS = 100;
  private static final long DURATION_SECONDS = 1;

  /** Virtual clock: waiting simply moves time forward, so the pacing is deterministic. */
  private static final class VirtualPacer implements IntervalExecutor.Pacer {
    private long nanos;

    @Override
    public long nanoTime() {
      return nanos;
    }

    @Override
    public void await(long timeoutNanos) {
      nanos += timeoutNanos;
    }

    void advanceMillis(long millis) {
      nanos += TimeUnit.MILLISECONDS.toNanos(millis);
    }
  }

  private static RandomWorkloadGenerator generator(double readRatio) {
    return new RandomWorkloadGenerator("C", "F", readRatio, 1000, 1, 10, new Random(42));
  }

  /** An executor bound to the given pacer, running the mixed workload for one second. */
  private static IntervalExecutor executor(ContractRunner runner, VirtualPacer pacer) {
    return new IntervalExecutor(runner, generator(0.5), INTERVAL_MILLIS, DURATION_SECONDS, pacer);
  }

  /** Total number of failures with the given outcome, across the contracts. */
  private static long failures(Result result, String outcome) {
    long total = 0;
    for (ContractStats stats : result.getStatsByContract().values()) {
      Long count = stats.getFailuresByOutcome().get(outcome);
      total += count == null ? 0 : count;
    }
    return total;
  }

  @Test
  public void run_durationElapses_shouldExecuteOncePerInterval() {
    // Arrange: one second at one execution per 100 ms
    AtomicInteger calls = new AtomicInteger();
    VirtualPacer pacer = new VirtualPacer();
    IntervalExecutor executor = executor((contractId, argument) -> calls.incrementAndGet(), pacer);

    // Act
    Result result = executor.run();

    // Assert
    assertThat(calls.get()).isEqualTo(10);
    assertThat(result.getExecutions()).isEqualTo(10);
    assertThat(result.getSuccesses()).isEqualTo(10);
    assertThat(result.getFailures()).isZero();
    assertThat(result.getBehindSchedule()).isZero();
    assertThat(result.getElapsedNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(DURATION_SECONDS));
  }

  @Test
  public void run_executionsFasterThanTheInterval_shouldAbsorbTheirCostIntoTheInterval() {
    // Arrange: 40 ms executions on a 100 ms interval. The interval is measured start-to-start, so
    // the execution cost comes out of the wait rather than being added on top of it. A fixed-DELAY
    // loop (wait a whole interval after each execution) would manage only 8 executions and overrun
    // the second; this is what separates the two.
    VirtualPacer pacer = new VirtualPacer();
    IntervalExecutor executor = executor((contractId, argument) -> pacer.advanceMillis(40), pacer);

    // Act
    Result result = executor.run();

    // Assert
    assertThat(result.getExecutions()).isEqualTo(10);
    assertThat(result.getElapsedNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(DURATION_SECONDS));
    assertThat(result.getBehindSchedule()).isZero();
  }

  @Test
  public void run_stopRequestedWhileWaiting_shouldReturnWithoutWaitingOutTheInterval()
      throws Exception {
    // Arrange: the REAL pacer (no virtual clock) and a one-minute interval, so a run that waited
    // out the interval instead of waking on the stop request would visibly hang. This is the only
    // test that covers the pacer the command actually ships with.
    CountDownLatch firstExecution = new CountDownLatch(1);
    IntervalExecutor executor =
        new IntervalExecutor(
            (contractId, argument) -> firstExecution.countDown(),
            generator(0.5),
            TimeUnit.MINUTES.toMillis(1),
            0);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      // Act
      Future<Result> running = pool.submit(executor::run);
      assertThat(firstExecution.await(30, TimeUnit.SECONDS)).isTrue();
      executor.requestStop();
      Result result = running.get(30, TimeUnit.SECONDS);

      // Assert
      assertThat(result.getExecutions()).isEqualTo(1);
      assertThat(result.getElapsedNanos()).isLessThan(TimeUnit.SECONDS.toNanos(30));
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void run_zeroDuration_shouldRunUntilStopRequested() {
    // Arrange: without a duration bound, only the stop request ends the run
    AtomicInteger calls = new AtomicInteger();
    IntervalExecutor[] holder = new IntervalExecutor[1];
    IntervalExecutor executor =
        new IntervalExecutor(
            (contractId, argument) -> {
              if (calls.incrementAndGet() == 3) {
                holder[0].requestStop();
              }
            },
            generator(0.5),
            INTERVAL_MILLIS,
            0,
            new VirtualPacer());
    holder[0] = executor;

    // Act
    Result result = executor.run();

    // Assert
    assertThat(result.getExecutions()).isEqualTo(3);
    assertThat(executor.isStopped()).isTrue();
  }

  @Test
  public void run_requestGenerationFails_shouldStopButStillReturnTheTally() {
    // Arrange: a payload size no JVM can allocate, so generating a workload F request throws an
    // Error while workload C (which carries no payload) keeps succeeding. A generation failure must
    // not take the tally of everything already executed down with it.
    VirtualPacer pacer = new VirtualPacer();
    IntervalExecutor executor =
        new IntervalExecutor(
            (contractId, argument) -> {},
            new RandomWorkloadGenerator(
                "C", "F", 0.5, 1000, 1, Integer.MAX_VALUE, new Random(42)),
            INTERVAL_MILLIS,
            DURATION_SECONDS,
            pacer);

    // Act: returns normally rather than propagating the Error
    Result result = executor.run();

    // Assert: it stopped at the first F, and every C that ran before it is still accounted for
    assertThat(executor.isStopped()).isTrue();
    assertThat(result.getExecutions()).isEqualTo(2);
    assertThat(result.getSuccesses()).isEqualTo(2);
    assertThat(result.getFailures()).isZero();
    assertThat(result.getStatsByContract().get("C").getSuccesses()).isEqualTo(2);
  }

  @Test
  public void run_contractFails_shouldTallyTheOutcomeAndKeepRunning() {
    // Arrange: every other execution conflicts
    AtomicInteger calls = new AtomicInteger();
    VirtualPacer pacer = new VirtualPacer();
    IntervalExecutor executor =
        executor(
            (contractId, argument) -> {
              if (calls.incrementAndGet() % 2 == 0) {
                throw new ClientException("conflict", StatusCode.CONFLICT);
              }
            },
            pacer);

    // Act
    Result result = executor.run();

    // Assert
    assertThat(result.getExecutions()).isEqualTo(10);
    assertThat(result.getSuccesses()).isEqualTo(5);
    assertThat(result.getFailures()).isEqualTo(5);
    assertThat(failures(result, StatusCode.CONFLICT.name())).isEqualTo(5);
  }

  @Test
  public void run_errorThrown_shouldBeTalliedByClassAndKeepRunning() {
    // Arrange: an Error (not a RuntimeException) must not kill the loop silently
    VirtualPacer pacer = new VirtualPacer();
    IntervalExecutor executor =
        executor(
            (contractId, argument) -> {
              throw new StackOverflowError("simulated Error inside the contract call");
            },
            pacer);

    // Act
    Result result = executor.run();

    // Assert
    assertThat(result.getExecutions()).isEqualTo(10);
    assertThat(result.getSuccesses()).isZero();
    assertThat(result.getFailures()).isEqualTo(10);
    assertThat(failures(result, "StackOverflowError")).isEqualTo(10);
  }

  @Test
  public void run_executionsSlowerThanTheInterval_shouldReportBehindScheduleWithoutBursting() {
    // Arrange: each execution takes 300 ms against a 100 ms interval
    VirtualPacer pacer = new VirtualPacer();
    IntervalExecutor executor = executor((contractId, argument) -> pacer.advanceMillis(300), pacer);

    // Act
    Result result = executor.run();

    // Assert: 4 executions fit into the second, and none was fired back to back to catch up
    assertThat(result.getExecutions()).isEqualTo(4);
    assertThat(result.getBehindSchedule()).isEqualTo(4);
  }

  @Test
  public void run_executionOutlastsTheRemainingDuration_shouldNotWaitPastTheEnd() {
    // Arrange: a 1.5 second execution on a 5 second interval, bounded to one second overall
    VirtualPacer pacer = new VirtualPacer();
    IntervalExecutor executor =
        new IntervalExecutor(
            (contractId, argument) -> pacer.advanceMillis(1500),
            generator(0.5),
            5000,
            DURATION_SECONDS,
            pacer);

    // Act
    Result result = executor.run();

    // Assert: the run ends right after the execution, without waiting out the interval
    assertThat(result.getExecutions()).isEqualTo(1);
    assertThat(result.getElapsedNanos()).isEqualTo(TimeUnit.MILLISECONDS.toNanos(1500));
  }

  @Test
  public void run_singleWorkload_shouldReportOnlyThatContract() {
    // Arrange
    VirtualPacer pacer = new VirtualPacer();
    IntervalExecutor executor =
        new IntervalExecutor(
            (contractId, argument) -> {}, generator(1), INTERVAL_MILLIS, DURATION_SECONDS, pacer);

    // Act
    Result result = executor.run();

    // Assert
    assertThat(result.getStatsByContract().keySet()).containsExactly("C");
    assertThat(result.getStatsByContract().get("C").getExecutions()).isEqualTo(10);
  }

  @Test
  public void run_stopRequestedBeforeStart_shouldNotExecuteAnything() {
    // Arrange
    AtomicInteger calls = new AtomicInteger();
    VirtualPacer pacer = new VirtualPacer();
    IntervalExecutor executor = executor((contractId, argument) -> calls.incrementAndGet(), pacer);
    executor.requestStop();

    // Act
    Result result = executor.run();

    // Assert
    assertThat(calls.get()).isZero();
    assertThat(result.getExecutions()).isZero();
  }

  @Test
  public void constructor_invalidArguments_shouldThrowIllegalArgumentException() {
    // Act, Assert
    assertThatThrownBy(
            () -> new IntervalExecutor((contractId, argument) -> {}, generator(0.5), 0, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--interval-millis");
    assertThatThrownBy(
            () -> new IntervalExecutor((contractId, argument) -> {}, generator(0.5), 100, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--duration-seconds");
  }
}
