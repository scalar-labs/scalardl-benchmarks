package com.scalar.dl.benchmarks.ycsb.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.dl.ledger.contract.JacksonBasedContract;
import com.scalar.dl.ledger.exception.ContractContextException;
import com.scalar.dl.ledger.statemachine.Ledger;
import javax.annotation.Nullable;

public class WorkloadA extends JacksonBasedContract {

  @Nullable
  @Override
  public JsonNode invoke(
      Ledger<JsonNode> ledger, JsonNode argument, @Nullable JsonNode properties) {

    if (!argument.has(Const.KEY_USER_IDS) || !argument.has(Const.KEY_PAYLOADS)) {
      throw new ContractContextException(
          "Please set " + Const.KEY_USER_IDS + " and " + Const.KEY_PAYLOADS + " in the argument");
    }

    JsonNode userIdsNode = argument.get(Const.KEY_USER_IDS);
    int opsPerTx = userIdsNode.size();
    if (opsPerTx % 2 != 0) {
      throw new ContractContextException("The number of user IDs must be a multiple of 2.");
    }

    JsonNode payloadsNode = argument.get(Const.KEY_PAYLOADS);
    if (opsPerTx != payloadsNode.size() * 2) {
      throw new ContractContextException("Payloads must be half the number of user IDs.");
    }

    int i = 0;
    for (; i < opsPerTx / 2; ++i) {
      String userId = userIdsNode.get(i).asText();
      ledger.get(userId);
    }
    for (; i < opsPerTx; ++i) {
      String userId = userIdsNode.get(i).asText();
      String payload = payloadsNode.get(i - opsPerTx / 2).asText();
      // Since ScalarDL prohibits blind writes, the write is changed to read-modify-write
      ledger.get(userId);
      ledger.put(userId, getObjectMapper().createObjectNode().put(Const.KEY_PAYLOAD, payload));
    }

    return null;
  }
}
