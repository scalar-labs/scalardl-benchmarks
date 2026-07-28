package com.scalar.dl.benchmarks.ycsb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.dl.benchmarks.ycsb.YcsbLoadFailedRanges.Range;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class YcsbLoadFailedRangesTest {

  @Test
  public void writeAndRead_shouldRoundTrip(@TempDir Path tempDir) {
    // Arrange
    File file = tempDir.resolve("failed.json").toFile();
    YcsbLoadFailedRanges original =
        new YcsbLoadFailedRanges(10000, Arrays.asList(new Range(0, 50), new Range(1200, 1250)));

    // Act
    original.write(file);
    YcsbLoadFailedRanges loaded = YcsbLoadFailedRanges.read(file);

    // Assert
    assertThat(loaded.getRecordCount()).isEqualTo(10000);
    assertThat(loaded.getRanges()).hasSize(2);
    assertThat(loaded.getRanges().get(0).getStart()).isEqualTo(0);
    assertThat(loaded.getRanges().get(0).getEnd()).isEqualTo(50);
    assertThat(loaded.getRanges().get(1).getStart()).isEqualTo(1200);
    assertThat(loaded.getRanges().get(1).getEnd()).isEqualTo(1250);
    assertThat(loaded.totalRecords()).isEqualTo(100);
  }

  @Test
  public void read_malformedContentGiven_shouldThrow(@TempDir Path tempDir) throws IOException {
    // Arrange
    File noKeys = tempDir.resolve("no-keys.json").toFile();
    Files.write(noKeys.toPath(), "{}".getBytes(StandardCharsets.UTF_8));
    File badRange = tempDir.resolve("bad-range.json").toFile();
    Files.write(
        badRange.toPath(),
        "{\"record_count\":10,\"failed_ranges\":[{\"start\":5}]}".getBytes(StandardCharsets.UTF_8));
    File missing = tempDir.resolve("missing.json").toFile();

    // Act & Assert
    assertThatThrownBy(() -> YcsbLoadFailedRanges.read(noKeys))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> YcsbLoadFailedRanges.read(badRange))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> YcsbLoadFailedRanges.read(missing)).isInstanceOf(Exception.class);
  }

  @Test
  public void range_invalidBoundsGiven_shouldThrow() {
    assertThatThrownBy(() -> new Range(-1, 5)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new Range(5, 5)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new Range(6, 5)).isInstanceOf(IllegalArgumentException.class);
  }
}
