package com.scalar.dl.benchmarks.ycsb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.dl.benchmarks.ycsb.SeedPlan.Execution;
import com.scalar.dl.benchmarks.ycsb.SeedPlan.Workload;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class SeedPlanTest {

  private static final List<Workload> BOTH = Arrays.asList(Workload.F, Workload.C);
  private static final List<Workload> F_ONLY = Collections.singletonList(Workload.F);

  private static List<Long> allKeys(SeedPlan plan, Workload workload) {
    Set<Long> keys = new HashSet<>();
    long count = 0;
    for (Execution execution : plan.executionsFor(workload)) {
      for (long userId : execution.getUserIds()) {
        keys.add(userId);
        count++;
      }
    }
    // distinctness within the workload
    assertThat(keys).hasSize((int) count);
    return keys.stream().sorted().collect(java.util.stream.Collectors.toList());
  }

  @Test
  public void executionsFor_bothWorkloadsGiven_shouldAssignDistinctUniformKeysInRange() {
    // Arrange: N divisible by S so that the stride is exact
    SeedPlan plan = SeedPlan.of(BOTH, 100, 10000, 1);

    // Act
    List<Long> fKeys = allKeys(plan, Workload.F);
    List<Long> cKeys = allKeys(plan, Workload.C);

    // Assert: counts and range
    assertThat(fKeys).hasSize(100);
    assertThat(cKeys).hasSize(100);
    assertThat(fKeys.get(0)).isGreaterThanOrEqualTo(0);
    assertThat(fKeys.get(fKeys.size() - 1)).isLessThan(10000);
    assertThat(cKeys.get(cKeys.size() - 1)).isLessThan(10000);

    // Assert: F and C are disjoint
    Set<Long> union = new HashSet<>(fKeys);
    union.addAll(cKeys);
    assertThat(union).hasSize(200);

    // Assert: exact uniform stride within each workload (S = 200, global stride = 50,
    // same-workload stride = 100), and F on even slots (keys 0, 100, ...), C on odd
    // (keys 50, 150, ...)
    for (int i = 0; i < fKeys.size(); i++) {
      assertThat(fKeys.get(i)).isEqualTo(100L * i);
      assertThat(cKeys.get(i)).isEqualTo(100L * i + 50);
    }
  }

  @Test
  public void executionsFor_singleWorkloadGiven_shouldUseAllSlots() {
    // Arrange
    SeedPlan plan = SeedPlan.of(F_ONLY, 10, 100, 1);

    // Act
    List<Long> keys = allKeys(plan, Workload.F);

    // Assert: stride N/M = 10 over all slots
    for (int i = 0; i < keys.size(); i++) {
      assertThat(keys.get(i)).isEqualTo(10L * i);
    }
  }

  @Test
  public void executionsFor_indivisibleTotalGiven_shouldStillProduceDistinctKeysInRange() {
    // Arrange: N not divisible by S; floor rounding must keep keys distinct
    SeedPlan plan = SeedPlan.of(BOTH, 33, 101, 1);

    // Act
    List<Long> fKeys = allKeys(plan, Workload.F);
    List<Long> cKeys = allKeys(plan, Workload.C);
    Set<Long> union = new HashSet<>(fKeys);
    union.addAll(cKeys);

    // Assert
    assertThat(union).hasSize(66);
    assertThat(Collections.max(union)).isLessThan(101);
    assertThat(Collections.min(union)).isGreaterThanOrEqualTo(0);
  }

  @Test
  public void executionsFor_opsPerTxGiven_shouldGroupConsecutiveOwnSlots() {
    // Arrange: M=6, K=3 -> 2 executions per workload
    SeedPlan plan = SeedPlan.of(BOTH, 6, 1200, 3);

    // Act
    List<Execution> fExecutions = plan.executionsFor(Workload.F);
    List<Execution> cExecutions = plan.executionsFor(Workload.C);

    // Assert: S = 12, global stride = 100; F own slots are global 0,2,4,... -> keys 0,200,400,...
    assertThat(fExecutions).hasSize(2);
    assertThat(fExecutions.get(0).getUserIds()).containsExactly(0L, 200L, 400L);
    assertThat(fExecutions.get(1).getUserIds()).containsExactly(600L, 800L, 1000L);
    // C own slots are global 1,3,5,... -> keys 100,300,500,...
    assertThat(cExecutions.get(0).getUserIds()).containsExactly(100L, 300L, 500L);
    assertThat(cExecutions.get(1).getUserIds()).containsExactly(700L, 900L, 1100L);
    assertThat(fExecutions.get(0).getWorkload()).isEqualTo(Workload.F);
    assertThat(fExecutions.get(1).getIndex()).isEqualTo(1);
  }

  @Test
  public void executionsFor_largeValuesGiven_shouldNotOverflow() {
    // Arrange: keys beyond int range; j * N exceeds int arithmetic by far
    long totalAssets = 4_000_000_000L;
    SeedPlan plan = SeedPlan.of(BOTH, 1_000_000, totalAssets, 1);

    // Act
    List<Execution> fExecutions = plan.executionsFor(Workload.F);
    List<Execution> cExecutions = plan.executionsFor(Workload.C);

    // Assert: last C key is near N and positive (would be negative on int overflow)
    long lastCKey = cExecutions.get(cExecutions.size() - 1).getUserIds()[0];
    assertThat(lastCKey).isGreaterThan(Integer.MAX_VALUE).isLessThan(totalAssets);
    assertThat(fExecutions.get(0).getUserIds()[0]).isEqualTo(0L);
  }

  @Test
  public void of_invalidValuesGiven_shouldThrowIllegalArgumentException() {
    assertThatThrownBy(() -> SeedPlan.of(BOTH, 0, 100, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--num-assets");
    assertThatThrownBy(() -> SeedPlan.of(BOTH, 10, 0, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--total-assets");
    assertThatThrownBy(() -> SeedPlan.of(BOTH, 10, 100, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--ops-per-tx");
    assertThatThrownBy(() -> SeedPlan.of(BOTH, 10, 100, 3))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("divisible");
    assertThatThrownBy(() -> SeedPlan.of(BOTH, 10, 15, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("keys would not be distinct");
    assertThatThrownBy(() -> SeedPlan.of(Collections.emptyList(), 10, 100, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("workload");
  }

  @Test
  public void describeKeys_shouldDescribeStrideAndSlots() {
    SeedPlan both = SeedPlan.of(BOTH, 10000, 1000000, 1);
    assertThat(both.describeKeys(Workload.F))
        .isEqualTo("10000 of [0, 1000000), uniformly distributed (stride 50, even slots)");
    assertThat(both.describeKeys(Workload.C))
        .isEqualTo("10000 of [0, 1000000), uniformly distributed (stride 50, odd slots)");

    SeedPlan single = SeedPlan.of(F_ONLY, 10000, 1000000, 1);
    assertThat(single.describeKeys(Workload.F))
        .isEqualTo("10000 of [0, 1000000), uniformly distributed (stride 100, all slots)");
  }
}
