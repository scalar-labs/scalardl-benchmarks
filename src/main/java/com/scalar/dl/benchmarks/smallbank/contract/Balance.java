package com.scalar.dl.benchmarks.smallbank.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.dl.ledger.contract.JacksonBasedContract;
import com.scalar.dl.ledger.exception.ContractContextException;
import com.scalar.dl.ledger.statemachine.Asset;
import com.scalar.dl.ledger.statemachine.Ledger;
import java.util.Optional;
import javax.annotation.Nullable;

public class Balance extends JacksonBasedContract {

  @Override
  public JsonNode invoke(
      Ledger<JsonNode> ledger, JsonNode argument, @Nullable JsonNode properties) {
    if (!argument.has(Const.KEY_CUSTOMER_ID)) {
      throw new ContractContextException(
          "Please set " + Const.KEY_CUSTOMER_ID + " in the argument");
    }

    String customerId = argument.get(Const.KEY_CUSTOMER_ID).asText();
    Optional<Asset<JsonNode>> asset = ledger.get(customerId);
    if (!asset.isPresent()) {
      throw new ContractContextException(Const.ERR_NOT_FOUND);
    }

    return asset.get().data();
  }
}
