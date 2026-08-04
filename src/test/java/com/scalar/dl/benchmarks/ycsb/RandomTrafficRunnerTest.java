package com.scalar.dl.benchmarks.ycsb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;

/** Option-level tests; none of these reach the network. */
public class RandomTrafficRunnerTest {

  private static int execute(String... args) {
    return new CommandLine(new RandomTrafficRunner()).execute(args);
  }

  /**
   * Runs the command and returns what it reported on stderr. Needed because the exit code alone
   * cannot tell the constraint checks apart: the properties file these tests pass is not a usable
   * client configuration either, so {@code ClientConfig} would fail with the same usage exit code
   * even if a constraint check were missing. The message is what pins which check fired.
   */
  private static String executeReportingError(String... args) throws UnsupportedEncodingException {
    PrintStream originalErr = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    try {
      System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8.name()));
      assertThat(execute(args)).isEqualTo(ExitCode.USAGE);
    } finally {
      System.setErr(originalErr);
    }
    return captured.toString(StandardCharsets.UTF_8.name());
  }

  @Test
  public void execute_requiredOptionsMissing_shouldExitWithUsage() {
    assertThat(execute("--total-assets", "100")).isEqualTo(ExitCode.USAGE);
  }

  @Test
  public void execute_helpRequested_shouldExitNormally() {
    assertThat(execute("--help")).isEqualTo(ExitCode.OK);
  }

  @Test
  public void execute_propertiesFileMissing_shouldExitWithUsage()
      throws UnsupportedEncodingException {
    assertThat(
            executeReportingError(
                "--properties", "/nonexistent/client.properties",
                "--total-assets", "100"))
        .contains("--properties");
  }

  @Test
  public void execute_constraintsViolated_shouldExitWithUsageAndNameTheOption(@TempDir Path tempDir)
      throws IOException {
    // Arrange: an existing readable file so that validation reaches the generator
    Path properties = tempDir.resolve("client.properties");
    Files.write(properties, "".getBytes(StandardCharsets.UTF_8));
    String path = properties.toString();

    // Assert: interval below one millisecond
    assertThat(
            executeReportingError(
                "--properties", path, "--total-assets", "100", "--interval-millis", "0"))
        .contains("--interval-millis");
    // Assert: negative duration
    assertThat(
            executeReportingError(
                "--properties", path, "--total-assets", "100", "--duration-seconds", "-1"))
        .contains("--duration-seconds");
    // Assert: read ratio out of range
    assertThat(
            executeReportingError(
                "--properties", path, "--total-assets", "100", "--read-ratio", "1.5"))
        .contains("--read-ratio");
    // Assert: read ratio contradicting a single-workload run
    assertThat(
            executeReportingError(
                "--properties", path,
                "--total-assets", "100",
                "--workload", "C",
                "--read-ratio", "0.5"))
        .contains("--read-ratio")
        .contains("MIXED");
    // Assert: more assets per execution than exist
    assertThat(
            executeReportingError("--properties", path, "--total-assets", "2", "--ops-per-tx", "3"))
        .contains("--ops-per-tx");
    // Assert: zero assets
    assertThat(executeReportingError("--properties", path, "--total-assets", "0"))
        .contains("--total-assets");
    // Assert: invalid workload value is rejected by picocli itself
    assertThat(
            executeReportingError("--properties", path, "--total-assets", "100", "--workload", "A"))
        .contains("--workload");
  }
}
