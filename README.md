# ScalarDL Benchmarks

This repository contains benchmark programs for ScalarDL.

## Available workloads

- SmallBank
- TPC-C (New-Order and Payment transactions only)
- YCSB (Workload A, C and F)

## Prerequisites

- Java (OpenJDK 8 or higher)
- Gradle
- Kelpie

The benchmark uses Kelpie, which is a simple yet general framework for performing end-to-end testing such as system benchmarking and verification. Get the latest version of Kelpie from [here](https://github.com/scalar-labs/kelpie) and unzip the archive.

## Usage

### Set up an environment

This benchmark requires the followings:
- A client to execute this benchmark
- A target Ledger server
- A target Auditor server (optional)

Set up the above components and then properly configure Client, Ledger and Auditor properties by following the getting-started guides. Note that you do not need to download Client SDK and manually register your certificate. As described later, this benchmark suite will automatically register the required certificate and contracts.

- [Ledger-only configuration](https://github.com/scalar-labs/scalardl/blob/master/docs/getting-started.md)
- [Ledger and Auditor configuration](https://github.com/scalar-labs/scalardl/blob/master/docs/getting-started-auditor.md)

### Build

```console
./gradlew shadowJar
```

### Load and run

1. Prepare a configuration file
   - A configuration file requires at least the locations of workload modules to run and the client configuration. The following example shows the case for running TPC-C benchmark. The client configuration should be matched with the benchmark environment set up above. You can use the `client.properties` file instead of specifying each configuration item. If the `config_file` is specified, all other configuration items will be ignored.
     ```
     [modules]
     [modules.preprocessor]
     name = "com.scalar.dl.benchmarks.tpcc.TpccLoader"
     path = "./build/libs/scalardl-benchmarks-all.jar"
     [modules.processor]
     name = "com.scalar.dl.benchmarks.tpcc.TpccBench"
     path = "./build/libs/scalardl-benchmarks-all.jar"
     [modules.postprocessor]
     name = "com.scalar.dl.benchmarks.tpcc.TpccReporter"
     path = "./build/libs/scalardl-benchmarks-all.jar"

     [client_config]
     ledger_host = "localhost"
     auditor_host = "localhost"
     auditor_enabled = "true"
     cert_holder_id = "test_holder"
     certificate = "/path/to/client.pem"
     private_key = "/path/to/client-key.pem"
     #config_file = "/path/to/client.properties"
     ```
   - You can define static parameters to pass to modules in the file. For details, see example configuration files such as `tpcc-benchmark-config.toml` and available parameters in [the following section](#common-parameters).
2. Run a benchmark
   ```
   ${kelpie}/bin/kelpie --config your_config.toml
   ```
   - `${kelpie}` is a Kelpie directory, which is extracted from the archive you downloaded [above](#prerequisites).
   - There are other options such as `--only-pre` (i.e., registering certificates and contracts and loading data) and `--only-process` (i.e., running benchmark), which run only the specified process. `--except-pre` and `--except-process` run a job without the specified process.

## Common parameters

| name           | description                                             | default |
|:---------------|:--------------------------------------------------------|:--------|
| `concurrency`  | Number of threads for benchmarking.                     | 1       |
| `run_for_sec`  | Duration of benchmark (in seconds).                     | 60      |
| `ramp_for_sec` | Duration of ramp up time before benchmark (in seconds). | 0       |

## Workload-specific parameters

### SmallBank

| name               | description                                         | default |
|:-------------------|:----------------------------------------------------|:--------|
| `num_accounts`     | Number of bank accounts for benchmarking.           | 100000  |
| `load_concurrency` | Number of threads for loading.                      | 1       |
| `load_batch_size`  | Number of accounts in a single loading transaction. | 1       |

### TPC-C

| name               | description                                           | default |
|:-------------------|:------------------------------------------------------|:--------|
| `num_warehouses`   | Number of warehouses (scale factor) for benchmarking. | 1       |
| `rate_payment`     | Percentage of payment transaction.                    | 50      |
| `load_concurrency` | Number of threads for loading.                        | 1       |

### YCSB

| name               | description                                        | default |
|:-------------------|:---------------------------------------------------|:--------|
| `record_count`     | Number of records for benchmarking.                | 1000    |
| `payload_size`     | Payload size (in bytes) of each record.            | 1000    |
| `ops_per_tx`       | Number of operations in a single transaction       | 2       |
| `workload`         | Workload type (A, C or F).                         | A       |
| `load_concurrency` | Number of threads for loading.                     | 1       |
| `load_batch_size`  | Number of records in a single loading transaction. | 1       |
