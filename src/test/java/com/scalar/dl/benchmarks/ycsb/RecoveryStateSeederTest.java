package com.scalar.dl.benchmarks.ycsb;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;

/** Option-level tests; none of these reach the network. */
public class RecoveryStateSeederTest {

  private static int execute(String... args) {
    return new CommandLine(new RecoveryStateSeeder()).execute(args);
  }

  @Test
  public void execute_requiredOptionsMissing_shouldExitWithUsage() {
    assertThat(execute("--num-assets", "10")).isEqualTo(ExitCode.USAGE);
  }

  @Test
  public void execute_helpRequested_shouldExitNormally() {
    assertThat(execute("--help")).isEqualTo(ExitCode.OK);
  }

  @Test
  public void execute_propertiesFileMissing_shouldExitWithUsage() {
    assertThat(
            execute(
                "--properties", "/nonexistent/client.properties",
                "--num-assets", "10",
                "--total-assets", "100"))
        .isEqualTo(ExitCode.USAGE);
  }

  @Test
  public void execute_constraintsViolated_shouldExitWithUsage(@TempDir Path tempDir)
      throws IOException {
    // Arrange: an existing readable file so that validation reaches SeedPlan
    Path properties = tempDir.resolve("client.properties");
    Files.write(properties, "".getBytes(StandardCharsets.UTF_8));

    // Assert: M x W > N
    assertThat(
            execute(
                "--properties", properties.toString(),
                "--num-assets", "10",
                "--total-assets", "15"))
        .isEqualTo(ExitCode.USAGE);
    // Assert: M not divisible by K
    assertThat(
            execute(
                "--properties", properties.toString(),
                "--num-assets", "10",
                "--total-assets", "100",
                "--ops-per-tx", "3"))
        .isEqualTo(ExitCode.USAGE);
    // Assert: invalid concurrency
    assertThat(
            execute(
                "--properties", properties.toString(),
                "--num-assets", "10",
                "--total-assets", "100",
                "--concurrency", "0"))
        .isEqualTo(ExitCode.USAGE);
    // Assert: invalid workload value is rejected by picocli itself
    assertThat(
            execute(
                "--properties", properties.toString(),
                "--num-assets", "10",
                "--total-assets", "100",
                "--workload", "A"))
        .isEqualTo(ExitCode.USAGE);
  }
}
