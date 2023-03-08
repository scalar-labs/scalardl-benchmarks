package com.scalar.dl.benchmarks.smallbank.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.dl.ledger.contract.JacksonBasedContract;
import com.scalar.dl.ledger.exception.ContractContextException;
import com.scalar.dl.ledger.statemachine.Asset;
import com.scalar.dl.ledger.statemachine.Ledger;
import java.util.Optional;
import javax.annotation.Nullable;

public class Amalgamate extends JacksonBasedContract {

  @Override
  public JsonNode invoke(
      Ledger<JsonNode> ledger, JsonNode argument, @Nullable JsonNode properties) {
    if (!argument.has(Const.KEY_SRC_CUSTOMER_ID) || !argument.has(Const.KEY_DST_CUSTOMER_ID)) {
      throw new ContractContextException(
          "Please set "
              + Const.KEY_SRC_CUSTOMER_ID
              + " and "
              + Const.KEY_DST_CUSTOMER_ID
              + " in the argument");
    }

    String srcCustomerId = argument.get(Const.KEY_SRC_CUSTOMER_ID).asText();
    String dstCustomerId = argument.get(Const.KEY_DST_CUSTOMER_ID).asText();

    Optional<Asset<JsonNode>> srcAsset = ledger.get(srcCustomerId);
    if (!srcAsset.isPresent()) {
      throw new ContractContextException(Const.ERR_NOT_FOUND);
    }
    Optional<Asset<JsonNode>> dstAsset = ledger.get(dstCustomerId);
    if (!dstAsset.isPresent()) {
      throw new ContractContextException(Const.ERR_NOT_FOUND);
    }

    JsonNode srcNode = srcAsset.get().data();
    JsonNode dstNode = dstAsset.get().data();
    int srcSavingsBalance = srcNode.get(Const.KEY_SAVINGS_BALANCE).asInt();
    int dstCheckingBalance = dstNode.get(Const.KEY_CHECKING_BALANCE).asInt();
    dstCheckingBalance += srcSavingsBalance;
    srcSavingsBalance = 0;

    ObjectNode newSrcNode = srcNode.deepCopy();
    ObjectNode newDstNode = dstNode.deepCopy();
    newSrcNode.put(Const.KEY_SAVINGS_BALANCE, srcSavingsBalance);
    newDstNode.put(Const.KEY_CHECKING_BALANCE, dstCheckingBalance);
    ledger.put(srcCustomerId, newSrcNode);
    ledger.put(dstCustomerId, newDstNode);

    return null;
  }
}
