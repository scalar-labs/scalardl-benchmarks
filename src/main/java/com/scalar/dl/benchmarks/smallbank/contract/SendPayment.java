package com.scalar.dl.benchmarks.smallbank.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.dl.ledger.contract.JacksonBasedContract;
import com.scalar.dl.ledger.exception.ContractContextException;
import com.scalar.dl.ledger.statemachine.Asset;
import com.scalar.dl.ledger.statemachine.Ledger;
import java.util.Optional;
import javax.annotation.Nullable;

public class SendPayment extends JacksonBasedContract {

  @Override
  public JsonNode invoke(
      Ledger<JsonNode> ledger, JsonNode argument, @Nullable JsonNode properties) {
    if (!argument.has(Const.KEY_SRC_CUSTOMER_ID)
        || !argument.has(Const.KEY_DST_CUSTOMER_ID)
        || !argument.has(Const.KEY_AMOUNT)) {
      throw new ContractContextException(
          "Please set "
              + Const.KEY_SRC_CUSTOMER_ID
              + ", "
              + Const.KEY_DST_CUSTOMER_ID
              + " and "
              + Const.KEY_AMOUNT
              + " in the argument");
    }

    String srcCustomerId = argument.get(Const.KEY_SRC_CUSTOMER_ID).asText();
    String dstCustomerId = argument.get(Const.KEY_DST_CUSTOMER_ID).asText();
    int amount = argument.get(Const.KEY_AMOUNT).asInt();

    Optional<Asset<JsonNode>> srcAsset = ledger.get(srcCustomerId);
    if (!srcAsset.isPresent()) {
      throw new ContractContextException(Const.ERR_NOT_FOUND);
    }
    Optional<Asset<JsonNode>> dstAsset = ledger.get(dstCustomerId);
    if (!dstAsset.isPresent()) {
      throw new ContractContextException(Const.ERR_NOT_FOUND);
    }

    JsonNode srcData = srcAsset.get().data();
    JsonNode dstData = dstAsset.get().data();
    int srcBalance = srcData.get(Const.KEY_CHECKING_BALANCE).asInt();
    int dstBalance = dstData.get(Const.KEY_CHECKING_BALANCE).asInt();
    srcBalance -= amount;
    dstBalance += amount;

    ObjectNode newSrcData = srcData.deepCopy();
    ObjectNode newDstData = dstData.deepCopy();
    newSrcData.put(Const.KEY_CHECKING_BALANCE, srcBalance);
    newDstData.put(Const.KEY_CHECKING_BALANCE, dstBalance);
    ledger.put(srcCustomerId, newSrcData);
    ledger.put(dstCustomerId, newDstData);

    return null;
  }
}
