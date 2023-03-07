package com.scalar.dl.benchmarks.smallbank;

import static com.scalar.dl.benchmarks.Common.getClientConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.dl.benchmarks.smallbank.contract.Const;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class SmallBankLoader extends PreProcessor {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final AtomicInteger COUNTER = new AtomicInteger(0);
  private static final int DEFAULT_BALANCE = 100000;
  private final ClientServiceFactory factory;
  private final ClientService service;
  private final int concurrency;
  private final int batchSize;
  private final int numAccounts;
  private final String createContractId;
  private final String createContractName;
  private final String createContractPath;
  private final String balanceContractId;
  private final String balanceContractName;
  private final String balanceContractPath;
  private final String savingContractId;
  private final String savingContractName;
  private final String savingContractPath;
  private final String depositContractId;
  private final String depositContractName;
  private final String depositContractPath;
  private final String paymentContractId;
  private final String paymentContractName;
  private final String paymentContractPath;
  private final String checkContractId;
  private final String checkContractName;
  private final String checkContractPath;
  private final String amalgamateContractId;
  private final String amalgamateContractName;
  private final String amalgamateContractPath;

  public SmallBankLoader(Config config) {
    super(config);
    ClientConfig clientConfig = getClientConfig(config);
    this.factory = new ClientServiceFactory();
    this.service = factory.create(clientConfig);
    this.concurrency = SmallBankCommon.getLoadConcurrency(config);
    this.batchSize = SmallBankCommon.getLoadBatchSize(config);
    this.numAccounts = SmallBankCommon.getNumAccounts(config);
    this.createContractId = SmallBankCommon.getCreateContractId(config);
    this.createContractName = SmallBankCommon.getCreateContractName(config);
    this.createContractPath = SmallBankCommon.getCreateContractPath(config);
    this.balanceContractId = SmallBankCommon.getBalanceContractId(config);
    this.balanceContractName = SmallBankCommon.getBalanceContractName(config);
    this.balanceContractPath = SmallBankCommon.getBalanceContractPath(config);
    this.savingContractId = SmallBankCommon.getSavingContractId(config);
    this.savingContractName = SmallBankCommon.getSavingContractName(config);
    this.savingContractPath = SmallBankCommon.getSavingContractPath(config);
    this.depositContractId = SmallBankCommon.getDepositContractId(config);
    this.depositContractName = SmallBankCommon.getDepositContractName(config);
    this.depositContractPath = SmallBankCommon.getDepositContractPath(config);
    this.paymentContractId = SmallBankCommon.getPaymentContractId(config);
    this.paymentContractName = SmallBankCommon.getPaymentContractName(config);
    this.paymentContractPath = SmallBankCommon.getPaymentContractPath(config);
    this.checkContractId = SmallBankCommon.getCheckContractId(config);
    this.checkContractName = SmallBankCommon.getCheckContractName(config);
    this.checkContractPath = SmallBankCommon.getCheckContractPath(config);
    this.amalgamateContractId = SmallBankCommon.getAmalgamateContractId(config);
    this.amalgamateContractName = SmallBankCommon.getAmalgamateContractName(config);
    this.amalgamateContractPath = SmallBankCommon.getAmalgamateContractPath(config);
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
    service.registerContract(balanceContractId, balanceContractName, balanceContractPath);
    service.registerContract(savingContractId, savingContractName, savingContractPath);
    service.registerContract(depositContractId, depositContractName, depositContractPath);
    service.registerContract(paymentContractId, paymentContractName, paymentContractPath);
    service.registerContract(checkContractId, checkContractName, checkContractPath);
    service.registerContract(amalgamateContractId, amalgamateContractName, amalgamateContractPath);
  }

  private void loadRecords() {
    AtomicBoolean shutdown = new AtomicBoolean(false);
    ExecutorService executor = Executors.newCachedThreadPool();
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    IntStream.range(0, concurrency)
        .forEach(
            i -> {
              CompletableFuture<Void> future =
                  CompletableFuture.runAsync(
                          () -> new SmallBankLoader.PopulationRunner(i).run(), executor)
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
              while (!shutdown.get() && COUNTER.get() < numAccounts) {
                int completionRate = COUNTER.get() * 100 / numAccounts;
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

    public PopulationRunner(int threadId) {
      this.id = threadId;
    }

    public void run() {
      int numPerThread = (numAccounts + concurrency - 1) / concurrency;
      int start = numPerThread * id;
      int end = Math.min(numPerThread * (id + 1), numAccounts);
      IntStream.range(0, (numPerThread + batchSize - 1) / batchSize)
          .forEach(
              i -> {
                int startId = start + batchSize * i;
                int endId = Math.min(start + batchSize * (i + 1), end);
                populateWithTx(startId, endId);
              });
    }

    private void populateWithTx(int startId, int endId) {
      ObjectNode argument = MAPPER.createObjectNode();
      ArrayNode customerIds = MAPPER.createArrayNode();
      ArrayNode customers = MAPPER.createArrayNode();
      for (int i = startId; i < endId; ++i) {
        customerIds.add(i);
        ObjectNode customer = MAPPER.createObjectNode();
        customer.put(Const.KEY_CUSTOMER_NAME, "Name" + i);
        customer.put(Const.KEY_CHECKING_BALANCE, DEFAULT_BALANCE);
        customer.put(Const.KEY_SAVINGS_BALANCE, DEFAULT_BALANCE);
        customers.add(customer);
      }
      argument.set(Const.KEY_CUSTOMER_IDS, customerIds);
      argument.set(Const.KEY_CUSTOMERS, customers);
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
      COUNTER.getAndAdd(endId - startId);
    }
  }
}
