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

  /**
   * Every case here used to be accepted silently. A non-array or empty failed_ranges turned into a
   * "nothing to load" run reported as a success, and the others would have loaded record IDs twice
   * or out of range, which cannot be undone because the Create contract appends blindly.
   */
  @Test
  public void read_unusableContentGiven_shouldThrowInsteadOfAccepting(@TempDir Path tempDir)
      throws IOException {
    String[][] cases = {
      {"null-ranges", "{\"record_count\":1000,\"failed_ranges\":null}"},
      {"scalar-ranges", "{\"record_count\":1000,\"failed_ranges\":123}"},
      {"object-ranges", "{\"record_count\":1000,\"failed_ranges\":{\"a\":{\"start\":0,\"end\":5}}}"},
      {"empty-ranges", "{\"record_count\":1000,\"failed_ranges\":[]}"},
      {
        "overlapping",
        "{\"record_count\":1000,\"failed_ranges\":[{\"start\":0,\"end\":100},"
            + "{\"start\":50,\"end\":150}]}"
      },
      {
        "duplicated",
        "{\"record_count\":1000,\"failed_ranges\":[{\"start\":0,\"end\":100},"
            + "{\"start\":0,\"end\":100}]}"
      },
      {"beyond-record-count", "{\"record_count\":1000,\"failed_ranges\":[{\"start\":0,\"end\":5000}]}"},
      {
        "end-exceeds-int",
        "{\"record_count\":1000,\"failed_ranges\":[{\"start\":0,\"end\":99999999999}]}"
      },
      {"non-numeric-start", "{\"record_count\":1000,\"failed_ranges\":[{\"start\":\"abc\",\"end\":50}]}"},
      {"non-numeric-count", "{\"record_count\":\"oops\",\"failed_ranges\":[{\"start\":0,\"end\":50}]}"},
      {"zero-record-count", "{\"record_count\":0,\"failed_ranges\":[{\"start\":0,\"end\":50}]}"},
    };
    for (String[] testCase : cases) {
      File file = tempDir.resolve(testCase[0] + ".json").toFile();
      Files.write(file.toPath(), testCase[1].getBytes(StandardCharsets.UTF_8));
      assertThatThrownBy(() -> YcsbLoadFailedRanges.read(file))
          .as("case: %s", testCase[0])
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void read_unsortedButDisjointRangesGiven_shouldAcceptAndSort(@TempDir Path tempDir)
      throws IOException {
    // Arrange: order carries no meaning, only overlap does
    File file = tempDir.resolve("unsorted.json").toFile();
    Files.write(
        file.toPath(),
        ("{\"record_count\":1000,\"failed_ranges\":[{\"start\":500,\"end\":600},"
                + "{\"start\":0,\"end\":100}]}")
            .getBytes(StandardCharsets.UTF_8));

    // Act
    YcsbLoadFailedRanges loaded = YcsbLoadFailedRanges.read(file);

    // Assert
    assertThat(loaded.getRanges().get(0).getStart()).isEqualTo(0);
    assertThat(loaded.getRanges().get(1).getStart()).isEqualTo(500);
    assertThat(loaded.totalRecords()).isEqualTo(200);
  }

  @Test
  public void range_invalidBoundsGiven_shouldThrow() {
    assertThatThrownBy(() -> new Range(-1, 5)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new Range(5, 5)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new Range(6, 5)).isInstanceOf(IllegalArgumentException.class);
  }
}
