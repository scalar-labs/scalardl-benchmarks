package com.scalar.dl.benchmarks.ycsb;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.dl.benchmarks.ycsb.SeedExecutor.WorkloadResult;
import com.scalar.dl.benchmarks.ycsb.SeedPlan.Execution;
import com.scalar.dl.benchmarks.ycsb.SeedPlan.Workload;
import com.scalar.dl.benchmarks.ycsb.contract.Const;
import com.scalar.dl.client.exception.ClientException;
import com.scalar.dl.ledger.service.StatusCode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class SeedExecutorTest {

  private static final ClientException EXPECTED_FAILURE =
      new ClientException("unknown status", StatusCode.UNKNOWN_TRANSACTION_STATUS);

  private static List<Execution> executions(int count) {
    return SeedPlan.of(Collections.singletonList(Workload.F), count, count * 10L, 1)
        .executionsFor(Workload.F);
  }

  @Test
  public void runWorkload_allExecutionsFailAsExpected_shouldReportAllExpected()
      throws InterruptedException {
    // Arrange
    SeedExecutor executor =
        new SeedExecutor(
            (contractId, argument) -> {
              throw EXPECTED_FAILURE;
            },
            2);

    // Act
    WorkloadResult result = executor.runWorkload("F", executions(20));

    // Assert
    assertThat(result.isAllExpected()).isTrue();
    assertThat(result.getExpected()).isEqualTo(20);
    assertThat(result.getNotStarted()).isZero();
    assertThat(result.getFirstFailure()).isNull();
    assertThat(executor.isStopped()).isFalse();
  }

  @Test
  public void runWorkload_commitUnexpectedlySucceeds_shouldFailFastAndAccountAllSlots()
      throws InterruptedException {
    // Arrange: the 6th call commits (does not throw); concurrency 1 keeps it deterministic
    AtomicInteger calls = new AtomicInteger();
    SeedExecutor executor =
        new SeedExecutor(
            (contractId, argument) -> {
              if (calls.incrementAndGet() == 6) {
                return; // commit went through
              }
              throw EXPECTED_FAILURE;
            },
            1);

    // Act
    WorkloadResult result = executor.runWorkload("F", executions(10));

    // Assert
    assertThat(result.isAllExpected()).isFalse();
    assertThat(result.getExpected()).isEqualTo(5);
    assertThat(result.getUnexpectedSuccesses()).isEqualTo(1);
    assertThat(result.getUnexpectedErrors()).isZero();
    assertThat(result.getNotStarted()).isEqualTo(4);
    assertThat(result.getFirstFailure()).contains("unexpected success").contains("user_ids");
    assertThat(executor.isStopped()).isTrue();
  }

  @Test
  public void runWorkload_unexpectedStatusCodeReturned_shouldFailFastWithDetails()
      throws InterruptedException {
    // Arrange
    SeedExecutor executor =
        new SeedExecutor(
            (contractId, argument) -> {
              throw new ClientException("conflict", StatusCode.CONFLICT);
            },
            1);

    // Act
    WorkloadResult result = executor.runWorkload("F", executions(5));

    // Assert
    assertThat(result.getUnexpectedErrors()).isEqualTo(1);
    assertThat(result.getExpected()).isZero();
    assertThat(result.getNotStarted()).isEqualTo(4);
    assertThat(result.getFirstFailure()).contains("CONFLICT");
    assertThat(executor.isStopped()).isTrue();
  }

  @Test
  public void runWorkload_errorThrown_shouldBeCountedAndNotReportedAsAllExpected()
      throws InterruptedException {
    // Arrange: an Error (not a RuntimeException) would be swallowed by the thread pool, so the
    // execution must still be accounted for; otherwise the summary reports success with a
    // silently reduced seeded count.
    AtomicInteger calls = new AtomicInteger();
    SeedExecutor executor =
        new SeedExecutor(
            (contractId, argument) -> {
              if (calls.incrementAndGet() == 4) {
                throw new StackOverflowError("simulated Error inside the contract call");
              }
              throw EXPECTED_FAILURE;
            },
            1);

    // Act
    WorkloadResult result = executor.runWorkload("F", executions(10));

    // Assert
    assertThat(result.getAccounted()).isEqualTo(result.getPlanned());
    assertThat(result.getUnexpectedErrors()).isEqualTo(1);
    assertThat(result.getExpected()).isEqualTo(3);
    assertThat(result.getNotStarted()).isEqualTo(6);
    assertThat(result.isAllExpected()).isFalse();
    assertThat(result.getFirstFailure()).contains("StackOverflowError");
  }

  @Test
  public void runWorkload_allExpected_shouldAccountForEveryPlannedExecution()
      throws InterruptedException {
    // Arrange
    SeedExecutor executor =
        new SeedExecutor(
            (contractId, argument) -> {
              throw EXPECTED_FAILURE;
            },
            4);

    // Act
    WorkloadResult result = executor.runWorkload("F", executions(40));

    // Assert
    assertThat(result.getPlanned()).isEqualTo(40);
    assertThat(result.getAccounted()).isEqualTo(40);
    assertThat(result.isAllExpected()).isTrue();
  }

  @Test
  public void runWorkload_runtimeExceptionThrown_shouldFailFast() throws InterruptedException {
    // Arrange
    SeedExecutor executor =
        new SeedExecutor(
            (contractId, argument) -> {
              throw new IllegalStateException("broken pipe");
            },
            1);

    // Act
    WorkloadResult result = executor.runWorkload("F", executions(3));

    // Assert
    assertThat(result.getUnexpectedErrors()).isEqualTo(1);
    assertThat(result.getNotStarted()).isEqualTo(2);
    assertThat(result.getFirstFailure()).contains("broken pipe");
  }

  @Test
  public void runWorkload_argumentsBuilt_shouldMatchContractFormat() throws InterruptedException {
    // Arrange: capture the argument passed to the runner
    AtomicReference<JsonNode> fArgument = new AtomicReference<>();
    AtomicReference<JsonNode> cArgument = new AtomicReference<>();
    SeedPlan plan = SeedPlan.of(Arrays.asList(Workload.F, Workload.C), 2, 40, 2);
    SeedExecutor executor =
        new SeedExecutor(
            (contractId, argument) -> {
              if (contractId.equals("F")) {
                fArgument.set(argument);
              } else {
                cArgument.set(argument);
              }
              throw EXPECTED_FAILURE;
            },
            1);

    // Act
    executor.runWorkload("F", plan.executionsFor(Workload.F));
    executor.runWorkload("C", plan.executionsFor(Workload.C));

    // Assert: F carries user_ids and same-sized payloads; C carries user_ids only
    JsonNode f = fArgument.get();
    assertThat(f.get(Const.KEY_USER_IDS)).hasSize(2);
    assertThat(f.get(Const.KEY_PAYLOADS)).hasSize(2);
    assertThat(f.get(Const.KEY_PAYLOADS).get(0).asText()).isNotEmpty();
    JsonNode c = cArgument.get();
    assertThat(c.get(Const.KEY_USER_IDS)).hasSize(2);
    assertThat(c.has(Const.KEY_PAYLOADS)).isFalse();
  }

  @Test
  public void requestStop_calledBeforeRun_shouldMarkAllExecutionsNotStarted()
      throws InterruptedException {
    // Arrange
    SeedExecutor executor =
        new SeedExecutor(
            (contractId, argument) -> {
              throw EXPECTED_FAILURE;
            },
            2);
    executor.requestStop();

    // Act
    WorkloadResult result = executor.runWorkload("F", executions(10));

    // Assert
    assertThat(result.getNotStarted()).isEqualTo(10);
    assertThat(result.getExpected()).isZero();
    assertThat(executor.awaitDrain(1)).isTrue();
  }
}
