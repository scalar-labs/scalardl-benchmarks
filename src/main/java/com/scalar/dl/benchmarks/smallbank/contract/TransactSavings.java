package com.scalar.dl.benchmarks.smallbank.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.dl.ledger.contract.JacksonBasedContract;
import com.scalar.dl.ledger.exception.ContractContextException;
import com.scalar.dl.ledger.statemachine.Asset;
import com.scalar.dl.ledger.statemachine.Ledger;
import java.util.Optional;
import javax.annotation.Nullable;

public class TransactSavings extends JacksonBasedContract {

  @Override
  public JsonNode invoke(
      Ledger<JsonNode> ledger, JsonNode argument, @Nullable JsonNode properties) {
    if (!argument.has(Const.KEY_CUSTOMER_ID) || !argument.has(Const.KEY_AMOUNT)) {
      throw new ContractContextException(
          "Please set " + Const.KEY_CUSTOMER_ID + " and " + Const.KEY_AMOUNT + " in the argument");
    }

    String customerId = argument.get(Const.KEY_CUSTOMER_ID).asText();
    int amount = argument.get(Const.KEY_AMOUNT).asInt();

    Optional<Asset<JsonNode>> asset = ledger.get(customerId);
    if (!asset.isPresent()) {
      throw new ContractContextException(Const.ERR_NOT_FOUND);
    }

    JsonNode data = asset.get().data();
    int savingsBalance = data.get(Const.KEY_SAVINGS_BALANCE).asInt();
    savingsBalance += amount;

    ObjectNode newData = data.deepCopy();
    newData.put(Const.KEY_SAVINGS_BALANCE, savingsBalance);
    ledger.put(customerId, newData);

    return null;
  }
}
