package com.scalar.dl.benchmarks.smallbank;

import static com.scalar.dl.benchmarks.Common.getClientConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.scalar.dl.benchmarks.smallbank.contract.Const;
import com.scalar.dl.client.config.ClientConfig;
import com.scalar.dl.client.exception.ClientException;
import com.scalar.dl.client.service.ClientService;
import com.scalar.dl.client.service.ClientServiceFactory;
import com.scalar.dl.ledger.service.StatusCode;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.json.Json;

public class SmallBankBench extends TimeBasedProcessor {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final List<Operation> OPERATIONS = ImmutableList.copyOf(Operation.values());
  private final AtomicInteger abortCounter = new AtomicInteger();
  private final ClientServiceFactory factory;
  private final ClientService service;
  private final int numAccounts;
  private final Map<Operation, String> contractIdMap = new HashMap<>();

  public SmallBankBench(Config config) {
    super(config);
    ClientConfig clientConfig = getClientConfig(config);
    this.factory = new ClientServiceFactory();
    this.service = factory.create(clientConfig);
    this.numAccounts = SmallBankCommon.getNumAccounts(config);
    contractIdMap.put(Operation.BALANCE, SmallBankCommon.getBalanceContractId(config));
    contractIdMap.put(Operation.TRANSACT_SAVINGS, SmallBankCommon.getSavingContractId(config));
    contractIdMap.put(Operation.DEPOSIT_CHECKING, SmallBankCommon.getDepositContractId(config));
    contractIdMap.put(Operation.SEND_PAYMENT, SmallBankCommon.getPaymentContractId(config));
    contractIdMap.put(Operation.WRITE_CHECK, SmallBankCommon.getCheckContractId(config));
    contractIdMap.put(Operation.AMALGAMATE, SmallBankCommon.getAmalgamateContractId(config));
  }

  @Override
  public void executeEach() {
    Operation operation = OPERATIONS.get(ThreadLocalRandom.current().nextInt(OPERATIONS.size()));
    JsonNode argument = generateArgument(operation);
    while (true) {
      try {
        service.executeContract(contractIdMap.get(operation), argument);
        break;
      } catch (ClientException e) {
        if (e.getStatusCode() == StatusCode.CONFLICT) {
          abortCounter.incrementAndGet();
        } else {
          throw e;
        }
      }
    }
  }

  @Override
  public void close() {
    setState(Json.createObjectBuilder().add("abort_count", abortCounter.toString()).build());
    factory.close();
  }

  private ObjectNode generateArgument(Operation operation) {
    int account1 = ThreadLocalRandom.current().nextInt(numAccounts);
    int account2 = ThreadLocalRandom.current().nextInt(numAccounts);
    if (account2 == account1) {
      account2 = (account2 + 1) % numAccounts;
    }
    int amount = ThreadLocalRandom.current().nextInt(100) + 1;

    ObjectNode node = MAPPER.createObjectNode();
    switch (operation) {
      case BALANCE:
        node.put(Const.KEY_CUSTOMER_ID, account1);
        break;
      case TRANSACT_SAVINGS:
      case DEPOSIT_CHECKING:
      case WRITE_CHECK:
        node.put(Const.KEY_CUSTOMER_ID, account1);
        node.put(Const.KEY_AMOUNT, amount);
        break;
      case SEND_PAYMENT:
        node.put(Const.KEY_SRC_CUSTOMER_ID, account1);
        node.put(Const.KEY_DST_CUSTOMER_ID, account2);
        node.put(Const.KEY_AMOUNT, amount);
        break;
      case AMALGAMATE:
        node.put(Const.KEY_SRC_CUSTOMER_ID, account1);
        node.put(Const.KEY_DST_CUSTOMER_ID, account2);
        break;
      default:
    }

    return node;
  }

  private enum Operation {
    BALANCE,
    TRANSACT_SAVINGS,
    DEPOSIT_CHECKING,
    WRITE_CHECK,
    SEND_PAYMENT,
    AMALGAMATE
  }
}
