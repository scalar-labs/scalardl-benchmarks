package com.scalar.dl.benchmarks.ycsb;

import static com.scalar.dl.benchmarks.Common.getClientConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.dl.benchmarks.ycsb.YcsbLoadFailedRanges.Range;
import com.scalar.dl.benchmarks.ycsb.contract.Const;
import com.scalar.dl.client.config.ClientConfig;
import com.scalar.dl.client.exception.ClientException;
import com.scalar.dl.client.service.ClientService;
import com.scalar.dl.client.service.ClientServiceFactory;
import com.scalar.dl.ledger.service.StatusCode;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.PreProcessException;
import com.scalar.kelpie.modules.PreProcessor;
import java.io.File;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class YcsbLoader extends PreProcessor {
  private static final long BACKOFF_BASE_MILLIS = 500;
  private static final long BACKOFF_MAX_MILLIS = 16000;

  /**
   * Status codes worth retrying a load batch for. Note that transport-level failures (a reset
   * stream, an unreachable server, ...) surface as UNKNOWN_TRANSACTION_STATUS because the client
   * maps any error without a ScalarDL status trailer to it. Retrying after
   * UNKNOWN_TRANSACTION_STATUS may re-insert a batch whose first attempt actually committed; the
   * Create contract appends blindly, so the affected assets end up with an extra version, which is
   * harmless for the benchmark (workloads only touch the latest version).
   */
  private static final Set<StatusCode> RETRIABLE_CODES =
      EnumSet.of(
          StatusCode.CONFLICT,
          StatusCode.DATABASE_ERROR,
          StatusCode.UNKNOWN_TRANSACTION_STATUS,
          StatusCode.RUNTIME_ERROR,
          StatusCode.UNAVAILABLE);

  private final ObjectMapper mapper = new ObjectMapper();
  private final AtomicLong counter = new AtomicLong(0);
  private final ClientServiceFactory factory;
  private final ClientService service;
  private final int concurrency;
  private final int batchSize;
  private final int recordCount;
  private final int payloadSize;
  private final int maxRetries;
  private final String failedRangesFile;
  private final String retryFile;
  private final String createContractId;
  private final String createContractName;
  private final String createContractPath;
  private final String workloadAContractId;
  private final String workloadAContractName;
  private final String workloadAContractPath;
  private final String workloadCContractId;
  private final String workloadCContractName;
  private final String workloadCContractPath;
  private final String workloadFContractId;
  private final String workloadFContractName;
  private final String workloadFContractPath;

  public YcsbLoader(Config config) {
    super(config);
    ClientConfig clientConfig = getClientConfig(config);
    this.factory = new ClientServiceFactory();
    this.service = factory.create(clientConfig);
    this.concurrency = YcsbCommon.getLoadConcurrency(config);
    this.batchSize = YcsbCommon.getLoadBatchSize(config);
    this.recordCount = YcsbCommon.getRecordCount(config);
    this.payloadSize = YcsbCommon.getPayloadSize(config);
    this.maxRetries = YcsbCommon.getLoadMaxRetries(config);
    this.failedRangesFile = YcsbCommon.getLoadFailedRangesFile(config);
    this.retryFile = YcsbCommon.getLoadRetryFile(config);
    this.createContractId = YcsbCommon.getCreateContractId(config);
    this.createContractName = YcsbCommon.getCreateContractName(config);
    this.createContractPath = YcsbCommon.getCreateContractPath(config);
    this.workloadAContractId = YcsbCommon.getWorkloadAContractId(config);
    this.workloadAContractName = YcsbCommon.getWorkloadAContractName(config);
    this.workloadAContractPath = YcsbCommon.getWorkloadAContractPath(config);
    this.workloadCContractId = YcsbCommon.getWorkloadCContractId(config);
    this.workloadCContractName = YcsbCommon.getWorkloadCContractName(config);
    this.workloadCContractPath = YcsbCommon.getWorkloadCContractPath(config);
    this.workloadFContractId = YcsbCommon.getWorkloadFContractId(config);
    this.workloadFContractName = YcsbCommon.getWorkloadFContractName(config);
    this.workloadFContractPath = YcsbCommon.getWorkloadFContractPath(config);
  }

  @Override
  public void execute() {
    bootstrapAndRegisterContracts();
    loadRecords();
  }

  @Override
  public void close() {
    factory.close();
  }

  private void bootstrapAndRegisterContracts() {
    // Tolerate already-registered identities and contracts so that a resumed load (or a re-run
    // against a bootstrapped environment) does not fail at registration.
    try {
      service.bootstrap();
    } catch (ClientException e) {
      if (e.getStatusCode() != StatusCode.CERTIFICATE_ALREADY_REGISTERED
          && e.getStatusCode() != StatusCode.SECRET_ALREADY_REGISTERED) {
        throw e;
      }
      logInfo("the identity has already been registered");
    }
    registerContract(createContractId, createContractName, createContractPath);
    registerContract(workloadAContractId, workloadAContractName, workloadAContractPath);
    registerContract(workloadCContractId, workloadCContractName, workloadCContractPath);
    registerContract(workloadFContractId, workloadFContractName, workloadFContractPath);
  }

  private void registerContract(String id, String name, String path) {
    try {
      service.registerContract(id, name, path);
    } catch (ClientException e) {
      if (e.getStatusCode() != StatusCode.CONTRACT_ALREADY_REGISTERED) {
        throw e;
      }
      logInfo("contract " + id + " has already been registered");
    }
  }

  private void loadRecords() {
    List<Range> batches = planBatches();
    if (batches.isEmpty()) {
      logInfo("nothing to load");
      return;
    }
    long plannedRecords = 0;
    for (Range batch : batches) {
      plannedRecords += batch.size();
    }

    AtomicInteger cursor = new AtomicInteger(0);
    Set<Integer> succeeded = ConcurrentHashMap.newKeySet();
    AtomicBoolean aborted = new AtomicBoolean(false);
    AtomicReference<ClientException> fatal = new AtomicReference<>();
    AtomicBoolean workersDone = new AtomicBoolean(false);

    ExecutorService executor = Executors.newCachedThreadPool();
    try {
      List<CompletableFuture<Void>> workers = new ArrayList<>();
      for (int i = 0; i < concurrency; i++) {
        workers.add(
            CompletableFuture.runAsync(
                () -> runWorker(batches, cursor, succeeded, aborted, fatal), executor));
      }
      long totalRecords = plannedRecords;
      CompletableFuture<Void> monitor =
          CompletableFuture.runAsync(
              () -> {
                while (!workersDone.get()) {
                  logInfo(counter.get() * 100 / totalRecords + "% records have been inserted");
                  Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
                }
              },
              executor);
      CompletableFuture.allOf(workers.toArray(new CompletableFuture[0])).join();
      workersDone.set(true);
      monitor.join();
    } finally {
      executor.shutdown();
    }

    List<Range> failed = new ArrayList<>();
    for (int i = 0; i < batches.size(); i++) {
      if (!succeeded.contains(i)) {
        failed.add(batches.get(i));
      }
    }
    if (failed.isEmpty()) {
      logInfo("all records have been inserted");
      return;
    }

    File file = new File(failedRangesFile);
    YcsbLoadFailedRanges failedRanges = new YcsbLoadFailedRanges(recordCount, failed);
    failedRanges.write(file);
    String summary =
        failed.size()
            + " batches ("
            + failedRanges.totalRecords()
            + " records) could not be loaded; their ranges were written to "
            + file.getAbsolutePath()
            + ". Set [ycsb_config] load_retry_file = \""
            + failedRangesFile
            + "\" to load only the missing ranges.";
    logError(summary);
    if (fatal.get() != null) {
      throw new PreProcessException("load failed: " + summary, fatal.get());
    }
    throw new PreProcessException("load failed: " + summary);
  }

  private List<Range> planBatches() {
    if (retryFile.isEmpty()) {
      return splitIntoBatches(new Range(0, recordCount), batchSize);
    }
    YcsbLoadFailedRanges failedRanges = YcsbLoadFailedRanges.read(new File(retryFile));
    if (failedRanges.getRecordCount() != recordCount) {
      logWarn(
          "record_count in "
              + retryFile
              + " ("
              + failedRanges.getRecordCount()
              + ") does not match the configured record_count ("
              + recordCount
              + "); check that the retry file belongs to this environment");
    }
    logInfo(
        "resuming the load from "
            + retryFile
            + ": "
            + failedRanges.getRanges().size()
            + " ranges ("
            + failedRanges.totalRecords()
            + " records)");
    List<Range> batches = new ArrayList<>();
    for (Range range : failedRanges.getRanges()) {
      batches.addAll(splitIntoBatches(range, batchSize));
    }
    return batches;
  }

  static List<Range> splitIntoBatches(Range range, int batchSize) {
    List<Range> batches = new ArrayList<>();
    for (int start = range.getStart(); start < range.getEnd(); start += batchSize) {
      batches.add(new Range(start, Math.min(start + batchSize, range.getEnd())));
    }
    return batches;
  }

  static boolean isRetriable(StatusCode code) {
    return RETRIABLE_CODES.contains(code);
  }

  /** Exponential backoff with jitter: base * 2^(attempt-1) capped, then halved and randomized. */
  static long backoffMillis(int attempt, ThreadLocalRandom random) {
    long backoff =
        Math.min(BACKOFF_BASE_MILLIS * (1L << Math.min(attempt - 1, 30)), BACKOFF_MAX_MILLIS);
    return backoff / 2 + random.nextLong(backoff / 2 + 1);
  }

  private void runWorker(
      List<Range> batches,
      AtomicInteger cursor,
      Set<Integer> succeeded,
      AtomicBoolean aborted,
      AtomicReference<ClientException> fatal) {
    char[] payload = new char[payloadSize];
    int index;
    while ((index = cursor.getAndIncrement()) < batches.size()) {
      if (aborted.get()) {
        return;
      }
      Range batch = batches.get(index);
      try {
        if (loadBatchWithRetry(batch, payload)) {
          succeeded.add(index);
          counter.getAndAdd(batch.size());
        }
        // false: retries exhausted; the batch stays out of `succeeded` and thus ends up in the
        // failed-ranges file, while the other batches keep loading.
      } catch (ClientException e) {
        // A non-retriable error (unregistered contract, invalid signature, ...) fails every batch
        // the same way, so stop the whole load instead of grinding through the rest.
        fatal.compareAndSet(null, e);
        aborted.set(true);
        logError("giving up the load due to a non-retriable error: " + e.getMessage());
        return;
      }
    }
  }

  private boolean loadBatchWithRetry(Range batch, char[] payload) {
    ObjectNode argument = buildArgument(batch, payload);
    for (int attempt = 1; attempt <= maxRetries + 1; attempt++) {
      try {
        service.executeContract(createContractId, argument);
        return true;
      } catch (ClientException e) {
        if (!isRetriable(e.getStatusCode())) {
          throw e;
        }
        if (attempt <= maxRetries) {
          long backoff = backoffMillis(attempt, ThreadLocalRandom.current());
          logWarn(
              "loading batch "
                  + batch
                  + " failed (attempt "
                  + attempt
                  + "/"
                  + (maxRetries + 1)
                  + ", status="
                  + e.getStatusCode()
                  + "); retrying in "
                  + backoff
                  + " ms: "
                  + e.getMessage());
          Uninterruptibles.sleepUninterruptibly(backoff, TimeUnit.MILLISECONDS);
        } else {
          logWarn(
              "giving up on batch "
                  + batch
                  + " after "
                  + (maxRetries + 1)
                  + " attempts (status="
                  + e.getStatusCode()
                  + "): "
                  + e.getMessage());
        }
      }
    }
    return false;
  }

  private ObjectNode buildArgument(Range batch, char[] payload) {
    ObjectNode argument = mapper.createObjectNode();
    ArrayNode userIds = mapper.createArrayNode();
    ArrayNode payloads = mapper.createArrayNode();
    for (int i = batch.getStart(); i < batch.getEnd(); ++i) {
      YcsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
      userIds.add(i);
      payloads.add(new String(payload));
    }
    argument.set(Const.KEY_USER_IDS, userIds);
    argument.set(Const.KEY_PAYLOADS, payloads);
    return argument;
  }
}
