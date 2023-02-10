package com.scalar.dl.benchmarks.ycsb;

import static com.scalar.dl.benchmarks.Common.getClientConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.dl.benchmarks.ycsb.YcsbQuery.Type;
import com.scalar.dl.client.config.ClientConfig;
import com.scalar.dl.client.exception.ClientException;
import com.scalar.dl.client.service.ClientService;
import com.scalar.dl.client.service.ClientServiceFactory;
import com.scalar.dl.ledger.service.StatusCode;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.json.Json;

public class YcsbBench extends TimeBasedProcessor {
  private final AtomicInteger abortCounter = new AtomicInteger();
  private final ClientServiceFactory factory;
  private final ClientService service;
  private final int recordCount;
  private final YcsbQuery.Type workload;
  private final int opsPerTx;
  private final int payloadSize;
  private final Map<YcsbQuery.Type, String> contractIdMap = new HashMap<>();

  public YcsbBench(Config config) {
    super(config);
    ClientConfig clientConfig = getClientConfig(config);
    this.factory = new ClientServiceFactory();
    this.service = factory.create(clientConfig);
    this.recordCount = YcsbCommon.getRecordCount(config);
    this.workload = YcsbQuery.Type.valueOf(YcsbCommon.getWorkload(config)); // "A", "C" or "F"
    this.opsPerTx = YcsbCommon.getOpsPerTx(config);
    this.payloadSize = YcsbCommon.getPayloadSize(config);
    contractIdMap.put(Type.A, YcsbCommon.getWorkloadAContractId(config));
    contractIdMap.put(Type.C, YcsbCommon.getWorkloadCContractId(config));
    contractIdMap.put(Type.F, YcsbCommon.getWorkloadFContractId(config));
  }

  @Override
  public void executeEach() {
    JsonNode argument = generateArgument(workload);
    while (true) {
      try {
        service.executeContract(contractIdMap.get(workload), argument);
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

  private ObjectNode generateArgument(YcsbQuery.Type workload) {
    switch (workload) {
      case A:
        return YcsbQuery.A.generate(recordCount, opsPerTx, payloadSize);
      case C:
        return YcsbQuery.C.generate(recordCount, opsPerTx);
      case F:
        return YcsbQuery.F.generate(recordCount, opsPerTx, payloadSize);
      default:
        return null;
    }
  }
}
