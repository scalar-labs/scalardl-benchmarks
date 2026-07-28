package com.scalar.dl.benchmarks.ycsb;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.dl.benchmarks.ycsb.YcsbLoadFailedRanges.Range;
import com.scalar.dl.ledger.service.StatusCode;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

public class YcsbLoaderTest {

  @Test
  public void splitIntoBatches_shouldCoverRangeExactlyWithoutOverlap() {
    // Act
    List<Range> batches = YcsbLoader.splitIntoBatches(new Range(0, 105), 50);

    // Assert: 0-50, 50-100, 100-105
    assertThat(batches).hasSize(3);
    assertThat(batches.get(0).getStart()).isEqualTo(0);
    assertThat(batches.get(0).getEnd()).isEqualTo(50);
    assertThat(batches.get(1).getStart()).isEqualTo(50);
    assertThat(batches.get(1).getEnd()).isEqualTo(100);
    assertThat(batches.get(2).getStart()).isEqualTo(100);
    assertThat(batches.get(2).getEnd()).isEqualTo(105);
  }

  @Test
  public void splitIntoBatches_rangeSmallerThanBatchGiven_shouldReturnSingleBatch() {
    // Act
    List<Range> batches = YcsbLoader.splitIntoBatches(new Range(1200, 1210), 50);

    // Assert
    assertThat(batches).hasSize(1);
    assertThat(batches.get(0).getStart()).isEqualTo(1200);
    assertThat(batches.get(0).getEnd()).isEqualTo(1210);
  }

  @Test
  public void isRetriable_shouldClassifyStatusCodes() {
    // Transient failures, including transport-level errors which the client maps to
    // UNKNOWN_TRANSACTION_STATUS when no ScalarDL status trailer is present.
    assertThat(YcsbLoader.isRetriable(StatusCode.CONFLICT)).isTrue();
    assertThat(YcsbLoader.isRetriable(StatusCode.DATABASE_ERROR)).isTrue();
    assertThat(YcsbLoader.isRetriable(StatusCode.UNKNOWN_TRANSACTION_STATUS)).isTrue();
    assertThat(YcsbLoader.isRetriable(StatusCode.RUNTIME_ERROR)).isTrue();
    assertThat(YcsbLoader.isRetriable(StatusCode.UNAVAILABLE)).isTrue();
    // Deterministic failures that would fail every batch identically.
    assertThat(YcsbLoader.isRetriable(StatusCode.CONTRACT_NOT_FOUND)).isFalse();
    assertThat(YcsbLoader.isRetriable(StatusCode.INVALID_SIGNATURE)).isFalse();
    assertThat(YcsbLoader.isRetriable(StatusCode.CONTRACT_CONTEXTUAL_ERROR)).isFalse();
  }

  @Test
  public void backoffMillis_shouldGrowExponentiallyAndStayCapped() {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    for (int i = 0; i < 100; i++) {
      // attempt 1: 500ms base -> [250, 500]
      assertThat(YcsbLoader.backoffMillis(1, random)).isBetween(250L, 500L);
      // attempt 3: 2000ms -> [1000, 2000]
      assertThat(YcsbLoader.backoffMillis(3, random)).isBetween(1000L, 2000L);
      // attempt 30+: capped at 16000ms -> [8000, 16000], and no overflow
      assertThat(YcsbLoader.backoffMillis(30, random)).isBetween(8000L, 16000L);
      assertThat(YcsbLoader.backoffMillis(1000, random)).isBetween(8000L, 16000L);
    }
  }
}
