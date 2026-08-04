package com.scalar.dl.benchmarks.ycsb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.dl.benchmarks.ycsb.RandomWorkloadGenerator.Request;
import com.scalar.dl.benchmarks.ycsb.contract.Const;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class RandomWorkloadGeneratorTest {

  private static RandomWorkloadGenerator generator(
      double readRatio, long totalAssets, int opsPerTx) {
    return new RandomWorkloadGenerator(
        "C", "F", readRatio, totalAssets, opsPerTx, 100, new Random(42));
  }

  @Test
  public void next_readRatioOne_shouldAlwaysGenerateWorkloadC() {
    // Arrange
    RandomWorkloadGenerator generator = generator(1, 1000, 2);

    // Act, Assert
    for (int i = 0; i < 100; i++) {
      Request request = generator.next();
      assertThat(request.getContractId()).isEqualTo("C");
      assertThat(request.getArgument().get(Const.KEY_USER_IDS)).hasSize(2);
      assertThat(request.getArgument().has(Const.KEY_PAYLOADS)).isFalse();
    }
    assertThat(generator.getContractIds()).containsExactly("C");
  }

  @Test
  public void next_readRatioZero_shouldAlwaysGenerateWorkloadFWithPayloads() {
    // Arrange
    RandomWorkloadGenerator generator =
        new RandomWorkloadGenerator("C", "F", 0, 1000, 2, 64, new Random(42));

    // Act, Assert
    for (int i = 0; i < 100; i++) {
      Request request = generator.next();
      assertThat(request.getContractId()).isEqualTo("F");
      JsonNode argument = request.getArgument();
      assertThat(argument.get(Const.KEY_USER_IDS)).hasSize(2);
      assertThat(argument.get(Const.KEY_PAYLOADS)).hasSize(2);
      assertThat(argument.get(Const.KEY_PAYLOADS).get(0).asText()).hasSize(64);
    }
    assertThat(generator.getContractIds()).containsExactly("F");
  }

  @Test
  public void next_mixedReadRatio_shouldGenerateBothWorkloads() {
    // Arrange
    RandomWorkloadGenerator generator = generator(0.5, 1000, 1);

    // Act
    int reads = 0;
    for (int i = 0; i < 1000; i++) {
      if (generator.next().getContractId().equals("C")) {
        reads++;
      }
    }

    // Assert: a loose band; the point is that both workloads are produced at the given ratio
    assertThat(reads).isBetween(400, 600);
    assertThat(generator.getContractIds()).containsExactly("C", "F");
  }

  @Test
  public void next_customContractIds_shouldBeUsed() {
    // Arrange
    RandomWorkloadGenerator generator =
        new RandomWorkloadGenerator("ycsb-c", "ycsb-f", 0.5, 100, 1, 10, new Random(1));

    // Act, Assert
    for (int i = 0; i < 50; i++) {
      assertThat(generator.next().getContractId()).isIn("ycsb-c", "ycsb-f");
    }
    assertThat(generator.getContractIds()).containsExactly("ycsb-c", "ycsb-f");
  }

  @Test
  public void next_anyWorkload_shouldDrawKeysWithinTheLoadedKeySpace() {
    // Arrange
    RandomWorkloadGenerator generator = generator(0.5, 10, 1);

    // Act
    Set<Long> seen = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      long[] userIds = generator.next().getUserIds();
      assertThat(userIds[0]).isBetween(0L, 9L);
      seen.add(userIds[0]);
    }

    // Assert: uniform over [0, 10), so every key shows up within 1000 draws
    assertThat(seen).hasSize(10);
  }

  @Test
  public void next_opsPerTxEqualToTotalAssets_shouldGenerateDistinctKeys() {
    // Arrange: the only distinct triple over [0, 3) is {0, 1, 2}, so a duplicate would be visible
    RandomWorkloadGenerator generator = generator(0, 3, 3);

    // Act, Assert
    for (int i = 0; i < 100; i++) {
      long[] userIds = generator.next().getUserIds();
      assertThat(userIds).containsExactlyInAnyOrder(0L, 1L, 2L);
    }
  }

  @Test
  public void constructor_invalidArguments_shouldThrowIllegalArgumentException() {
    // Act, Assert
    assertThatThrownBy(() -> generator(1.5, 100, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--read-ratio");
    assertThatThrownBy(() -> generator(-0.5, 100, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--read-ratio");
    assertThatThrownBy(() -> generator(0.5, 0, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--total-assets");
    assertThatThrownBy(() -> generator(0.5, 100, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--ops-per-tx");
    // More assets per execution than exist would make the distinct-key draw impossible
    assertThatThrownBy(() -> generator(0.5, 2, 3))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--ops-per-tx");
    assertThatThrownBy(() -> new RandomWorkloadGenerator("C", "F", 0.5, 100, 1, 0, new Random(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--payload-size");
    assertThatThrownBy(() -> new RandomWorkloadGenerator("", "F", 0.5, 100, 1, 10, new Random(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("--contract-id-c");
  }
}
