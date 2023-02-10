package com.scalar.dl.benchmarks.ycsb;

import static com.scalar.dl.benchmarks.Common.getClientConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.dl.benchmarks.ycsb.contract.Const;
import com.scalar.dl.client.config.ClientConfig;
import com.scalar.dl.client.exception.ClientException;
import com.scalar.dl.client.service.ClientService;
import com.scalar.dl.client.service.ClientServiceFactory;
import com.scalar.dl.ledger.service.StatusCode;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.PreProcessException;
import com.scalar.kelpie.modules.PreProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class YcsbLoader extends PreProcessor {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final AtomicInteger counter = new AtomicInteger(0);
  private final ClientServiceFactory factory;
  private final ClientService service;
  private final int concurrency;
  private final int batchSize;
  private final int recordCount;
  private final int payloadSize;
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
    registerCertificateAndContracts();
    loadRecords();
  }

  @Override
  public void close() {
    factory.close();
  }

  private void registerCertificateAndContracts() {
    service.registerCertificate();
    service.registerContract(createContractId, createContractName, createContractPath);
    service.registerContract(workloadAContractId, workloadAContractName, workloadAContractPath);
    service.registerContract(workloadCContractId, workloadCContractName, workloadCContractPath);
    service.registerContract(workloadFContractId, workloadFContractName, workloadFContractPath);
  }

  private void loadRecords() {
    AtomicBoolean shutdown = new AtomicBoolean(false);
    ExecutorService executor = Executors.newCachedThreadPool();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(() -> new PopulationRunner(i).run(), executor)
                      .exceptionally(
                          e -> {
                            shutdown.set(true);
                            throw new PreProcessException("load failed", e);
                          });
              futures.add(future);
            });
    CompletableFuture<Void> monitor =
        CompletableFuture.runAsync(
            () -> {
              while (!shutdown.get() && counter.get() < recordCount) {
                int completionRate = counter.get() * 100 / recordCount;
                logInfo(completionRate + "% records have been inserted");
                Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
              }
            },
            executor);
    futures.add(monitor);

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    logInfo("all records have been inserted");
  }

  private class PopulationRunner {
    private final int id;
    private final char[] payload;

    public PopulationRunner(int threadId) {
      this.id = threadId;
      this.payload = new char[payloadSize];
    }

    public void run() {
      int numPerThread = (recordCount + concurrency - 1) / concurrency;
      int start = numPerThread * id;
      int end = Math.min(numPerThread * (id + 1), recordCount);
      IntStream.range(0, (numPerThread + batchSize - 1) / batchSize)
          .forEach(
              i -> {
                int startId = start + batchSize * i;
                int endId = Math.min(start + batchSize * (i + 1), end);
                populateWithTx(startId, endId);
              });
    }

    private void populateWithTx(int startId, int endId) {
      ObjectNode argument = mapper.createObjectNode();
      ArrayNode userIds = mapper.createArrayNode();
      ArrayNode payloads = mapper.createArrayNode();
      for (int i = startId; i < endId; ++i) {
        YcsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
        userIds.add(i);
        payloads.add(new String(payload));
      }
      argument.set(Const.KEY_USER_IDS, userIds);
      argument.set(Const.KEY_PAYLOADS, payloads);
      while (true) {
        try {
          service.executeContract(createContractId, argument);
          break;
        } catch (ClientException e) {
          if (e.getStatusCode() != StatusCode.CONFLICT) {
            logInfo("contract execution failed");
            throw e;
          }
        }
      }
      counter.getAndAdd(endId - startId);
    }
  }
}
