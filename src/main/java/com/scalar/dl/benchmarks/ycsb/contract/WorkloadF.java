package com.scalar.dl.benchmarks.ycsb.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.dl.ledger.contract.JacksonBasedContract;
import com.scalar.dl.ledger.exception.ContractContextException;
import com.scalar.dl.ledger.statemachine.Ledger;
import javax.annotation.Nullable;

public class WorkloadF extends JacksonBasedContract {

  @Nullable
  @Override
  public JsonNode invoke(
      Ledger<JsonNode> ledger, JsonNode argument, @Nullable JsonNode properties) {

    if (!argument.has(Const.KEY_USER_IDS) || !argument.has(Const.KEY_PAYLOADS)) {
      throw new ContractContextException(
          "Please set " + Const.KEY_USER_IDS + " and " + Const.KEY_PAYLOADS + " in the argument");
    }

    JsonNode userIdsNode = argument.get(Const.KEY_USER_IDS);
    JsonNode payloadsNode = argument.get(Const.KEY_PAYLOADS);
    int opsPerTx = userIdsNode.size();
    if (opsPerTx != payloadsNode.size()) {
      throw new ContractContextException("The number of user IDs and payloads must be same.");
    }

    for (int i = 0; i < opsPerTx; ++i) {
      String userId = userIdsNode.get(i).asText();
      String payload = payloadsNode.get(i).asText();
      ledger.get(userId);
      ledger.put(userId, getObjectMapper().createObjectNode().put(Const.KEY_PAYLOAD, payload));
    }

    return null;
  }
}
