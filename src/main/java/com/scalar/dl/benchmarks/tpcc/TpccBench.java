package com.scalar.dl.benchmarks.tpcc;

import static com.scalar.dl.benchmarks.Common.getClientConfig;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.CONFIG_NAME;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.DEFAULT_NUM_WAREHOUSES;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.NUM_WAREHOUSES;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.getNewOrderContractId;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.getPaymentContractId;

import com.scalar.dl.benchmarks.tpcc.TpccQuery.Type;
import com.scalar.dl.client.config.ClientConfig;
import com.scalar.dl.client.exception.ClientException;
import com.scalar.dl.client.service.ClientService;
import com.scalar.dl.client.service.ClientServiceFactory;
import com.scalar.dl.ledger.service.StatusCode;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.json.Json;

public class TpccBench extends TimeBasedProcessor {
  private static final String RATE_PAYMENT = "rate_payment";
  private static final long DEFAULT_RATE_PAYMENT = 50;
  private final AtomicInteger abortCounter = new AtomicInteger();
  private final ClientServiceFactory factory;
  private final ClientService service;
  private final int numWarehouse;
  private final int ratePayment;
  private final Map<TpccQuery.Type, String> contractIdMap = new HashMap<>();

  public TpccBench(Config config) {
    super(config);
    ClientConfig clientConfig = getClientConfig(config);
    this.factory = new ClientServiceFactory();
    this.service = factory.create(clientConfig);
    this.numWarehouse =
        (int) config.getUserLong(CONFIG_NAME, NUM_WAREHOUSES, DEFAULT_NUM_WAREHOUSES);
    this.ratePayment = (int) config.getUserLong(CONFIG_NAME, RATE_PAYMENT, DEFAULT_RATE_PAYMENT);
    contractIdMap.put(Type.NewOrder, getNewOrderContractId(config));
    contractIdMap.put(Type.Payment, getPaymentContractId(config));
  }

  @Override
  public void executeEach() {
    String nonce = UUID.randomUUID().toString();
    TpccQuery.Type type = decideType();
    String jsonArgument = generateArgument(nonce, type);
    while (true) {
      try {
        service.executeContract(nonce, contractIdMap.get(type), jsonArgument);
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

  private TpccQuery.Type decideType() {
    int x = TpccCommon.randomInt(1, 100);
    if (x <= ratePayment) {
      return TpccQuery.Type.Payment;
    } else {
      return TpccQuery.Type.NewOrder;
    }
  }

  private String generateArgument(String nonce, TpccQuery.Type type) {
    switch (type) {
      case Payment:
        return TpccQuery.Payment.generate(nonce, numWarehouse);
      case NewOrder:
        return TpccQuery.NewOrder.generate(nonce, numWarehouse);
      default:
        return "";
    }
  }
}
