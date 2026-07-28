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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
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
   * maps any error without a ScalarDL status trailer to it.
   *
   * <p>UNKNOWN_TRANSACTION_STATUS means the outcome is unknown, so both retrying it and giving up
   * on it are inexact. The argument is built once outside the retry loop, so a re-inserted batch
   * writes the same values and the workloads, which only read the latest version, observe no
   * difference. What does change is the number of asset versions, which affects how reproducible a
   * benchmark run is; and a batch that ends up in the failed-ranges file after this status may in
   * fact have been committed, which is why those ranges are reported as "may not have been loaded"
   * rather than as definitely missing.
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
  private final int maxConsecutiveFailures;
  private final long maxFailedRecords;
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
    this.maxConsecutiveFailures = YcsbCommon.getLoadMaxConsecutiveFailures(config);
    this.maxFailedRecords = YcsbCommon.getLoadMaxFailedRecords(config);
    this.failedRangesFile = YcsbCommon.getLoadFailedRangesFile(config);
    this.retryFile = YcsbCommon.getLoadRetryFile(config);
    checkConfig();
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

  private void checkConfig() {
    checkAtLeast(recordCount, 1, YcsbCommon.RECORD_COUNT);
    checkAtLeast(batchSize, 1, YcsbCommon.LOAD_BATCH_SIZE);
    checkAtLeast(concurrency, 1, YcsbCommon.LOAD_CONCURRENCY);
    checkAtLeast(maxRetries, 0, YcsbCommon.LOAD_MAX_RETRIES);
    checkAtLeast(maxConsecutiveFailures, 1, YcsbCommon.LOAD_MAX_CONSECUTIVE_FAILURES);
    checkAtLeast(maxFailedRecords, 1, YcsbCommon.LOAD_MAX_FAILED_RECORDS);
    // Fail before loading anything rather than when the failed ranges must be persisted.
    File file = new File(failedRangesFile).getAbsoluteFile();
    File parent = file.getParentFile();
    if (parent == null || !parent.isDirectory()) {
      throw new IllegalArgumentException(
          YcsbCommon.LOAD_FAILED_RANGES_FILE + " has no existing directory: " + file);
    }
    if (file.exists() ? !file.canWrite() : !parent.canWrite()) {
      throw new IllegalArgumentException(
          YcsbCommon.LOAD_FAILED_RANGES_FILE + " is not writable: " + file);
    }
  }

  private static void checkAtLeast(long value, long minimum, String name) {
    if (value < minimum) {
      throw new IllegalArgumentException(name + " must be >= " + minimum + ", but was " + value);
    }
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
    // ClientService.bootstrap() already tolerates an identity that is registered, so a resumed
    // load (or a re-run against a bootstrapped environment) passes through here.
    service.bootstrap();
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
      // Registration is keyed by contract ID only, so the deployed bytecode is NOT updated here.
      // Warn rather than inform: editing a contract class and re-running silently keeps running
      // the previously deployed version.
      logWarn(
          "contract "
              + id
              + " is already registered, so the deployed bytecode was left as it is; "
              + name
              + " from "
              + path
              + " was NOT deployed");
    }
  }

  private void loadRecords() {
    BatchPlan plan = planBatches();
    if (plan.batchCount() == 0) {
      logInfo("nothing to load");
      return;
    }
    long plannedRecords = plan.recordCount();

    AtomicLong cursor = new AtomicLong(0);
    LoadState state = new LoadState();
    AtomicBoolean workersDone = new AtomicBoolean(false);

    ExecutorService executor = Executors.newCachedThreadPool();
    try {
      List<CompletableFuture<Void>> workers = new ArrayList<>();
      for (int i = 0; i < concurrency; i++) {
        workers.add(CompletableFuture.runAsync(() -> runWorker(plan, cursor, state), executor));
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
      try {
        CompletableFuture.allOf(workers.toArray(new CompletableFuture[0])).join();
      } finally {
        // Must run even when join() throws, or the monitor keeps spinning and, more importantly,
        // the failed ranges below are never recorded.
        workersDone.set(true);
        monitor.join();
      }
    } finally {
      executor.shutdown();
    }

    String abortReason = state.abortReason.get();
    if (abortReason != null) {
      // A systemic failure. The partially loaded data cannot be topped up because the Create
      // contract appends blindly, so a resume file would be useless: say so instead of writing one.
      String summary =
          "load aborted: "
              + abortReason
              + ". "
              + counter.get()
              + " of "
              + plannedRecords
              + " records had been loaded. Fix the underlying problem, delete the loaded data, and"
              + " run the load again; no failed-ranges file was written because the partially"
              + " loaded data cannot be resumed onto.";
      logError(summary);
      throw new PreProcessException(summary, state.fatal.get());
    }

    // Without an abort, the worker loop ran to completion, so every batch either succeeded or was
    // recorded here; the not-loaded set is exactly what the workers collected.
    List<Range> failed = coalesce(state.failedBatches);
    if (failed.isEmpty()) {
      logInfo("all records have been inserted");
      return;
    }

    File file = new File(failedRangesFile).getAbsoluteFile();
    YcsbLoadFailedRanges failedRanges = new YcsbLoadFailedRanges(recordCount, failed);
    String summary =
        failed.size()
            + " batches ("
            + failedRanges.totalRecords()
            + " records) may not have been loaded; their ranges are being written to "
            + file
            + ". Set [ycsb_config] load_retry_file to that file to load only those ranges.";
    // Log before writing: if the write fails, this is the only remaining record of what is missing.
    logError(summary);
    try {
      failedRanges.write(file);
    } catch (RuntimeException e) {
      logError("could not write " + file + "; the ranges that may be missing are: " + failed);
      throw new PreProcessException("load failed: " + summary, e);
    }
    throw new PreProcessException("load failed: " + summary);
  }

  /** Failure bookkeeping shared by the workers. */
  private static final class LoadState {
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private final AtomicLong failedRecords = new AtomicLong();
    private final AtomicReference<String> abortReason = new AtomicReference<>();
    private final AtomicReference<Throwable> fatal = new AtomicReference<>();
    // Only the batches that could not be loaded are kept. The circuit breaker bounds how many
    // there can be, whereas materializing every batch would cost ~30 bytes each: 3 GB at 10^8
    // batches, which is the default shape of a large load (load_batch_size defaults to 1).
    private final Queue<Range> failedBatches = new ConcurrentLinkedQueue<>();
  }

  /**
   * The batches to load, derived arithmetically instead of being materialized. Batch {@code i} of
   * source range {@code k} covers {@code [start + (i - offset) * batchSize, ...)}, so the plan only
   * needs the source ranges: one for a full load, or one per recorded range when resuming.
   */
  static final class BatchPlan {
    private final List<Range> sources;
    private final long[] offsets; // offsets[k] = number of batches before sources[k]
    private final int batchSize;
    private final long records;

    private BatchPlan(List<Range> sources, long[] offsets, int batchSize, long records) {
      this.sources = sources;
      this.offsets = offsets;
      this.batchSize = batchSize;
      this.records = records;
    }

    static BatchPlan of(List<Range> sources, int batchSize) {
      if (batchSize < 1) {
        throw new IllegalArgumentException("batchSize must be >= 1, but was " + batchSize);
      }
      long[] offsets = new long[sources.size() + 1];
      long records = 0;
      for (int k = 0; k < sources.size(); k++) {
        Range source = sources.get(k);
        long batches = (source.size() + batchSize - 1L) / batchSize;
        offsets[k + 1] = offsets[k] + batches;
        records += source.size();
      }
      return BatchPlan.of(sources, offsets, batchSize, records);
    }

    private static BatchPlan of(
        List<Range> sources, long[] offsets, int batchSize, long records) {
      return new BatchPlan(Collections.unmodifiableList(new ArrayList<>(sources)), offsets,
          batchSize, records);
    }

    long batchCount() {
      return offsets[offsets.length - 1];
    }

    long recordCount() {
      return records;
    }

    Range batchAt(long index) {
      if (index < 0 || index >= batchCount()) {
        throw new IndexOutOfBoundsException("no batch at " + index);
      }
      int k = sourceIndexOf(index);
      Range source = sources.get(k);
      long start = source.getStart() + (index - offsets[k]) * batchSize;
      return new Range((int) start, (int) Math.min(start + batchSize, source.getEnd()));
    }

    /** The source range that batch {@code index} belongs to, by binary search over the offsets. */
    private int sourceIndexOf(long index) {
      int low = 0;
      int high = sources.size() - 1;
      while (low < high) {
        int mid = (low + high + 1) >>> 1;
        if (offsets[mid] <= index) {
          low = mid;
        } else {
          high = mid - 1;
        }
      }
      return low;
    }
  }

  private BatchPlan planBatches() {
    if (retryFile.isEmpty()) {
      return BatchPlan.of(Collections.singletonList(new Range(0, recordCount)), batchSize);
    }
    YcsbLoadFailedRanges failedRanges = YcsbLoadFailedRanges.read(new File(retryFile));
    if (failedRanges.getRecordCount() != recordCount) {
      // The ranges are coordinates into a different data set, so resuming from them would load
      // record IDs that already exist. That cannot be undone, so refuse rather than warn.
      throw new IllegalArgumentException(
          YcsbCommon.RECORD_COUNT
              + " in "
              + retryFile
              + " ("
              + failedRanges.getRecordCount()
              + ") does not match the configured "
              + YcsbCommon.RECORD_COUNT
              + " ("
              + recordCount
              + "); the retry file belongs to a different data set");
    }
    logInfo(
        "resuming the load from "
            + retryFile
            + ": "
            + failedRanges.getRanges().size()
            + " ranges ("
            + failedRanges.totalRecords()
            + " records)");
    return BatchPlan.of(failedRanges.getRanges(), batchSize);
  }

  /**
   * Merges the recorded batches into as few ranges as possible. With the default batch size of 1 a
   * throttled window produces one entry per record, and consecutive records fail together, so
   * merging keeps both the file and the memory needed to write it small.
   */
  static List<Range> coalesce(Collection<Range> batches) {
    List<Range> sorted = new ArrayList<>(batches);
    sorted.sort(Comparator.comparingInt(Range::getStart));
    List<Range> merged = new ArrayList<>();
    for (Range batch : sorted) {
      int last = merged.size() - 1;
      if (last >= 0 && merged.get(last).getEnd() == batch.getStart()) {
        merged.set(last, new Range(merged.get(last).getStart(), batch.getEnd()));
      } else {
        merged.add(batch);
      }
    }
    return merged;
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

  private void runWorker(BatchPlan plan, AtomicLong cursor, LoadState state) {
    char[] payload = new char[payloadSize];
    long index;
    while ((index = cursor.getAndIncrement()) < plan.batchCount()) {
      if (state.abortReason.get() != null) {
        return;
      }
      Range batch = plan.batchAt(index);
      try {
        if (loadBatchWithRetry(batch, payload)) {
          counter.getAndAdd(batch.size());
          state.consecutiveFailures.set(0);
        } else {
          // Retries exhausted. The batch is recorded for the failed-ranges file, while the other
          // batches keep loading.
          recordBatchFailure(batch, state);
        }
      } catch (Throwable t) {
        // Throwable, not ClientException: an unexpected RuntimeException or Error would otherwise
        // escape the worker, make allOf().join() throw, and skip the failed-ranges bookkeeping
        // entirely. A non-retriable ClientException (unregistered contract, invalid signature,
        // ...) fails every batch the same way, so both cases stop the whole load.
        state.fatal.compareAndSet(null, t);
        state.abortReason.compareAndSet(null, "a non-retriable error occurred (" + t + ")");
        logError("giving up the load due to a non-retriable error: " + t);
        return;
      }
    }
  }

  /**
   * Counts a batch that exhausted its retries and gives up on the whole load once the failures look
   * systemic. Consecutive failures are the health signal: a random per-batch failure rate
   * practically never produces a long run of them, while an unavailable backend does so within
   * seconds, and unlike a ratio the threshold needs no tuning per record count. The total is a
   * resource bound that keeps the failed-ranges file from growing without limit.
   */
  private void recordBatchFailure(Range batch, LoadState state) {
    state.failedBatches.add(batch);
    long records = state.failedRecords.addAndGet(batch.size());
    int consecutive = state.consecutiveFailures.incrementAndGet();
    if (consecutive >= maxConsecutiveFailures) {
      state.abortReason.compareAndSet(
          null,
          consecutive
              + " batches failed in a row, which indicates a problem with the environment rather"
              + " than transient errors");
    } else if (records >= maxFailedRecords) {
      state.abortReason.compareAndSet(
          null, records + " records failed, reaching " + YcsbCommon.LOAD_MAX_FAILED_RECORDS);
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
