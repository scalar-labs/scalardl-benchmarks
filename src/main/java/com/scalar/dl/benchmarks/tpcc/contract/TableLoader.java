package com.scalar.dl.benchmarks.tpcc.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.scalar.dl.ledger.contract.JacksonBasedContract;
import com.scalar.dl.ledger.exception.ContractContextException;
import com.scalar.dl.ledger.statemachine.Ledger;
import javax.annotation.Nullable;

public class TableLoader extends JacksonBasedContract {

  @Override
  public JsonNode invoke(
      Ledger<JsonNode> ledger, JsonNode argument, @Nullable JsonNode properties) {
    if (!argument.has(Table.KEY_TABLE_NAME)) {
      throw new ContractContextException(Message.ERR_TABLE_NOT_SPECIFIED);
    }

    String table = argument.get(Table.KEY_TABLE_NAME).asText();
    String key;
    switch (table) {
      case Table.Name.CUSTOMER:
        key = createCustomerKey(argument);
        break;
      case Table.Name.CUSTOMER_SECONDARY:
        key = createCustomerSecondaryKey(argument);
        break;
      case Table.Name.DISTRICT:
        key = createDistrictKey(argument);
        break;
      case Table.Name.HISTORY:
        String nonce = argument.get(Table.QueryParam.KEY_NONCE).asText();
        key = createHistoryKey(nonce);
        break;
      case Table.Name.ITEM:
        key = createItemKey(argument);
        break;
      case Table.Name.NEW_ORDER:
        key = createNewOrderKey(argument);
        break;
      case Table.Name.ORDER:
        key = createOrderKey(argument);
        break;
      case Table.Name.ORDER_LINE:
        key = createOrderLineKey(argument);
        break;
      case Table.Name.STOCK:
        key = createStockKey(argument);
        break;
      case Table.Name.WAREHOUSE:
        key = createWarehouseKey(argument);
        break;
      default:
        throw new ContractContextException(Message.ERR_NO_SUCH_TABLE);
    }

    ledger.put(key, argument);

    return null;
  }

  private String createCustomerKey(JsonNode argument) {
    int warehouseId = argument.get(Table.Customer.KEY_WAREHOUSE_ID).asInt();
    int districtId = argument.get(Table.Customer.KEY_DISTRICT_ID).asInt();
    int customerId = argument.get(Table.Customer.KEY_CUSTOMER_ID).asInt();
    return String.format("%02d", Table.Code.CUSTOMER)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + String.format("%010d", customerId);
  }

  private String createCustomerSecondaryKey(JsonNode argument) {
    int warehouseId = argument.get(Table.Customer.KEY_WAREHOUSE_ID).asInt();
    int districtId = argument.get(Table.Customer.KEY_DISTRICT_ID).asInt();
    String lastName = argument.get(Table.Customer.KEY_LAST).asText();
    return String.format("%02d", Table.Code.CUSTOMER_SECONDARY)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + lastName;
  }

  private String createDistrictKey(JsonNode argument) {
    int warehouseId = argument.get(Table.District.KEY_WAREHOUSE_ID).asInt();
    int districtId = argument.get(Table.District.KEY_DISTRICT_ID).asInt();
    return String.format("%02d", Table.Code.DISTRICT)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId);
  }

  private String createHistoryKey(String nonce) {
    return String.format("%02d", Table.Code.HISTORY) + nonce;
  }

  private String createItemKey(JsonNode argument) {
    int itemId = argument.get(Table.Item.KEY_ITEM_ID).asInt();
    return String.format("%02d", Table.Code.ITEM) + String.format("%06d", itemId);
  }

  private String createNewOrderKey(JsonNode argument) {
    int warehouseId = argument.get(Table.NewOrder.KEY_WAREHOUSE_ID).asInt();
    int districtId = argument.get(Table.NewOrder.KEY_DISTRICT_ID).asInt();
    int orderId = argument.get(Table.NewOrder.KEY_ORDER_ID).asInt();
    return String.format("%02d", Table.Code.NEW_ORDER)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + String.format("%010d", orderId);
  }

  private String createOrderKey(JsonNode argument) {
    int warehouseId = argument.get(Table.Order.KEY_WAREHOUSE_ID).asInt();
    int districtId = argument.get(Table.Order.KEY_DISTRICT_ID).asInt();
    int orderId = argument.get(Table.Order.KEY_ORDER_ID).asInt();
    return String.format("%02d", Table.Code.ORDER)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + String.format("%010d", orderId);
  }

  private String createOrderLineKey(JsonNode argument) {
    int warehouseId = argument.get(Table.OrderLine.KEY_WAREHOUSE_ID).asInt();
    int districtId = argument.get(Table.OrderLine.KEY_DISTRICT_ID).asInt();
    int orderId = argument.get(Table.OrderLine.KEY_ORDER_ID).asInt();
    int orderLineNumber = argument.get(Table.OrderLine.KEY_NUMBER).asInt();
    return String.format("%02d", Table.Code.ORDER_LINE)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + String.format("%010d", orderId)
        + String.format("%03d", orderLineNumber);
  }

  private String createStockKey(JsonNode argument) {
    int warehouseId = argument.get(Table.Stock.KEY_WAREHOUSE_ID).asInt();
    int itemId = argument.get(Table.Stock.KEY_ITEM_ID).asInt();
    return String.format("%02d", Table.Code.STOCK)
        + String.format("%05d", warehouseId)
        + String.format("%06d", itemId);
  }

  private String createWarehouseKey(JsonNode argument) {
    int warehouseId = argument.get(Table.Warehouse.KEY_WAREHOUSE_ID).asInt();
    return String.format("%02d", Table.Code.WAREHOUSE) + String.format("%05d", warehouseId);
  }
}
