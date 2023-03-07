package com.scalar.dl.benchmarks.smallbank.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.dl.ledger.contract.JacksonBasedContract;
import com.scalar.dl.ledger.exception.ContractContextException;
import com.scalar.dl.ledger.statemachine.Ledger;
import javax.annotation.Nullable;

public class CreateAccount extends JacksonBasedContract {

  @Override
  public JsonNode invoke(
      Ledger<JsonNode> ledger, JsonNode argument, @Nullable JsonNode properties) {
    if (!argument.has(Const.KEY_CUSTOMER_IDS) || !argument.has(Const.KEY_CUSTOMERS)) {
      throw new ContractContextException(
          "Please set "
              + Const.KEY_CUSTOMER_IDS
              + " and "
              + Const.KEY_CUSTOMERS
              + " in the argument");
    }

    JsonNode customerIdsNode = argument.get(Const.KEY_CUSTOMER_IDS);
    int customersPerTx = customerIdsNode.size();

    JsonNode customersNode = argument.get(Const.KEY_CUSTOMERS);
    if (customersPerTx != customersNode.size()) {
      throw new ContractContextException("The number of customer IDs and customers must be same.");
    }

    for (int i = 0; i < customersPerTx; ++i) {
      String customerId = customerIdsNode.get(i).asText();
      ledger.put(customerId, customersNode.get(i));
    }

    return null;
  }
}
