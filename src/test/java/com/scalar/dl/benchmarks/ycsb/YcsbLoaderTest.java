package com.scalar.dl.benchmarks.ycsb;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.dl.benchmarks.ycsb.YcsbLoadFailedRanges.Range;
import com.scalar.dl.ledger.service.StatusCode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

public class YcsbLoaderTest {

  private static List<Range> allBatches(YcsbLoader.BatchPlan plan) {
    List<Range> batches = new ArrayList<>();
    for (long i = 0; i < plan.batchCount(); i++) {
      batches.add(plan.batchAt(i));
    }
    return batches;
  }

  @Test
  public void batchAt_shouldCoverRangeExactlyWithoutOverlap() {
    // Act
    YcsbLoader.BatchPlan plan =
        YcsbLoader.BatchPlan.of(Collections.singletonList(new Range(0, 105)), 50);
    List<Range> batches = allBatches(plan);

    // Assert: 0-50, 50-100, 100-105
    assertThat(plan.batchCount()).isEqualTo(3);
    assertThat(plan.recordCount()).isEqualTo(105);
    assertThat(batches.get(0).getStart()).isEqualTo(0);
    assertThat(batches.get(0).getEnd()).isEqualTo(50);
    assertThat(batches.get(1).getStart()).isEqualTo(50);
    assertThat(batches.get(1).getEnd()).isEqualTo(100);
    assertThat(batches.get(2).getStart()).isEqualTo(100);
    assertThat(batches.get(2).getEnd()).isEqualTo(105);
  }

  @Test
  public void batchAt_multipleSourceRangesGiven_shouldSplitEachIndependently() {
    // Arrange: resuming from two recorded ranges
    YcsbLoader.BatchPlan plan =
        YcsbLoader.BatchPlan.of(
            Arrays.asList(new Range(1200, 1210), new Range(5000, 5120)), 50);

    // Act
    List<Range> batches = allBatches(plan);

    // Assert: the small range stays whole, the large one splits, and no batch spans both
    assertThat(plan.batchCount()).isEqualTo(4);
    assertThat(plan.recordCount()).isEqualTo(130);
    assertThat(batches.get(0).getStart()).isEqualTo(1200);
    assertThat(batches.get(0).getEnd()).isEqualTo(1210);
    assertThat(batches.get(1).getStart()).isEqualTo(5000);
    assertThat(batches.get(1).getEnd()).isEqualTo(5050);
    assertThat(batches.get(3).getStart()).isEqualTo(5100);
    assertThat(batches.get(3).getEnd()).isEqualTo(5120);
  }

  @Test
  public void batchAt_largePlanGiven_shouldBeDerivedWithoutMaterializing() {
    // Arrange: the shape of a 100M-record load with the default batch size
    YcsbLoader.BatchPlan plan =
        YcsbLoader.BatchPlan.of(Collections.singletonList(new Range(0, 100_000_000)), 1);

    // Assert: batches are computed on demand, so even the last one is cheap to ask for
    assertThat(plan.batchCount()).isEqualTo(100_000_000L);
    assertThat(plan.batchAt(0).getStart()).isEqualTo(0);
    assertThat(plan.batchAt(99_999_999L).getStart()).isEqualTo(99_999_999);
    assertThat(plan.batchAt(99_999_999L).getEnd()).isEqualTo(100_000_000);
  }

  @Test
  public void coalesce_shouldMergeAdjacentBatchesAndSort() {
    // Arrange: workers record batches out of order, and with batch size 1 they are single records
    List<Range> recorded =
        Arrays.asList(
            new Range(7, 8),
            new Range(5, 6),
            new Range(6, 7),
            new Range(100, 101),
            new Range(50, 60));

    // Act
    List<Range> merged = YcsbLoader.coalesce(recorded);

    // Assert: 5-8 collapses into one entry, the rest stay separate and sorted
    assertThat(merged).hasSize(3);
    assertThat(merged.get(0).getStart()).isEqualTo(5);
    assertThat(merged.get(0).getEnd()).isEqualTo(8);
    assertThat(merged.get(1).getStart()).isEqualTo(50);
    assertThat(merged.get(1).getEnd()).isEqualTo(60);
    assertThat(merged.get(2).getStart()).isEqualTo(100);
  }

  @Test
  public void coalesce_noBatchesGiven_shouldReturnEmpty() {
    assertThat(YcsbLoader.coalesce(Collections.emptyList())).isEmpty();
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
