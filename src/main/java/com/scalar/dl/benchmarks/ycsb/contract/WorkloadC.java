package com.scalar.dl.benchmarks.ycsb.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.dl.ledger.contract.JacksonBasedContract;
import com.scalar.dl.ledger.exception.ContractContextException;
import com.scalar.dl.ledger.statemachine.Ledger;
import javax.annotation.Nullable;

public class WorkloadC extends JacksonBasedContract {

  @Nullable
  @Override
  public JsonNode invoke(
      Ledger<JsonNode> ledger, JsonNode argument, @Nullable JsonNode properties) {

    if (!argument.has(Const.KEY_USER_IDS)) {
      throw new ContractContextException("Please set " + Const.KEY_USER_IDS + " in the argument");
    }

    for (JsonNode node : argument.get(Const.KEY_USER_IDS)) {
      String userId = node.asText();
      ledger.get(userId);
    }

    return null;
  }
}
