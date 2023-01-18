package com.scalar.dl.benchmarks.tpcc.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.dl.ledger.contract.JacksonBasedContract;
import com.scalar.dl.ledger.exception.ContractContextException;
import com.scalar.dl.ledger.statemachine.Asset;
import com.scalar.dl.ledger.statemachine.Ledger;
import java.util.Optional;
import javax.annotation.Nullable;

public class Payment extends JacksonBasedContract {
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public JsonNode invoke(
      Ledger<JsonNode> ledger, JsonNode argument, @Nullable JsonNode properties) {
    String nonce = argument.get(Table.QueryParam.KEY_NONCE).asText();
    int warehouseId = argument.get(Table.Warehouse.KEY_WAREHOUSE_ID).asInt();
    int districtId = argument.get(Table.District.KEY_DISTRICT_ID).asInt();
    int customerWarehouseId = argument.get(Table.Customer.KEY_WAREHOUSE_ID).asInt();
    int customerDistrictId = argument.get(Table.Customer.KEY_DISTRICT_ID).asInt();
    double amount = argument.get(Table.History.KEY_AMOUNT).asDouble();
    boolean byLastName = argument.get(Table.QueryParam.KEY_BY_LAST_NAME).asBoolean();
    String date = argument.get(Table.QueryParam.KEY_DATE).asText();
    int customerId;

    // Get and update warehouse
    String warehouseKey = createWarehouseKey(warehouseId);
    Optional<Asset<JsonNode>> warehouse = ledger.get(warehouseKey);
    if (warehouse.isEmpty()) {
      throw new ContractContextException(Message.ERR_ASSET_NOT_FOUND);
    }
    JsonNode oldWarehouseNode = warehouse.get().data();
    String warehouseName =
        oldWarehouseNode.get(Table.Warehouse.KEY_WAREHOUSE_NAME).asText(); // for history
    ObjectNode newWarehouseNode = oldWarehouseNode.deepCopy();
    newWarehouseNode.put(
        Table.Warehouse.KEY_YTD, oldWarehouseNode.get(Table.Warehouse.KEY_YTD).asDouble() + amount);
    ledger.put(warehouseKey, newWarehouseNode);

    // Get and update district
    String districtKey = createDistrictKey(warehouseId, districtId);
    Optional<Asset<JsonNode>> district = ledger.get(districtKey);
    if (district.isEmpty()) {
      throw new ContractContextException(Message.ERR_ASSET_NOT_FOUND);
    }
    JsonNode oldDistrictNode = district.get().data();
    String districtName = oldDistrictNode.get(Table.District.KEY_NAME).asText(); // for history
    ObjectNode newDistrictNode = oldDistrictNode.deepCopy();
    newDistrictNode.put(
        Table.District.KEY_YTD, oldDistrictNode.get(Table.District.KEY_YTD).asDouble() + amount);
    ledger.put(districtKey, newDistrictNode);

    // Get and update customer
    if (byLastName) {
      String customerLastName = argument.get(Table.Customer.KEY_LAST).asText();
      String key =
          createCustomerSecondaryKey(customerWarehouseId, customerDistrictId, customerLastName);
      Optional<Asset<JsonNode>> customerSecondary = ledger.get(key);
      if (customerSecondary.isEmpty()) {
        throw new ContractContextException(Message.ERR_ASSET_NOT_FOUND);
      }
      JsonNode indexDataNode = customerSecondary.get().data();
      JsonNode array = indexDataNode.get(Table.CustomerSecondary.KEY_CUSTOMER_ID_LIST);
      int offset = (array.size() + 1) / 2 - 1; // locate midpoint customer
      customerId = array.get(offset).get(0).asInt();
    } else {
      customerId = argument.get(Table.Customer.KEY_CUSTOMER_ID).asInt();
    }
    String customerKey = createCustomerKey(customerWarehouseId, customerDistrictId, customerId);
    Optional<Asset<JsonNode>> customer = ledger.get(customerKey);
    if (customer.isEmpty()) {
      throw new ContractContextException(Message.ERR_ASSET_NOT_FOUND);
    }
    JsonNode oldCustomerNode = customer.get().data();
    ObjectNode newCustomerNode = oldCustomerNode.deepCopy();
    newCustomerNode.put(
        Table.Customer.KEY_BALANCE,
        oldCustomerNode.get(Table.Customer.KEY_BALANCE).asDouble() + amount);
    newCustomerNode.put(
        Table.Customer.KEY_YTD_PAYMENT,
        oldCustomerNode.get(Table.Customer.KEY_YTD_PAYMENT).asDouble() + amount);
    newCustomerNode.put(
        Table.Customer.KEY_PAYMENT_CNT,
        oldCustomerNode.get(Table.Customer.KEY_PAYMENT_CNT).asInt() + 1);
    if (oldCustomerNode.get(Table.Customer.KEY_CREDIT).asText().equals("BC")) { // bad credit
      newCustomerNode.put(
          Table.Customer.KEY_DATA,
          generateCustomerData(
              warehouseId,
              districtId,
              customerId,
              customerWarehouseId,
              customerDistrictId,
              amount,
              oldCustomerNode.get(Table.Customer.KEY_DATA).asText()));
    }
    ledger.put(customerKey, newCustomerNode);

    // Insert history
    String historyKey = createHistoryKey(nonce);
    ledger.put(
        historyKey,
        buildHistoryJson(
            warehouseId,
            districtId,
            customerId,
            customerWarehouseId,
            customerDistrictId,
            date,
            amount,
            warehouseName,
            districtName));

    return null;
  }

  private String createCustomerKey(int warehouseId, int districtId, int customerId) {
    return String.format("%02d", Table.Code.CUSTOMER)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + String.format("%010d", customerId);
  }

  private String createCustomerSecondaryKey(int warehouseId, int districtId, String lastName) {
    return String.format("%02d", Table.Code.CUSTOMER_SECONDARY)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + lastName;
  }

  private String createDistrictKey(int warehouseId, int districtId) {
    return String.format("%02d", Table.Code.DISTRICT)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId);
  }

  private String createHistoryKey(String nonce) {
    return String.format("%02d", Table.Code.HISTORY) + nonce;
  }

  private String createWarehouseKey(int warehouseId) {
    return String.format("%02d", Table.Code.WAREHOUSE) + String.format("%05d", warehouseId);
  }

  private String generateCustomerData(
      int warehouseId,
      int districtId,
      int customerId,
      int customerWarehouseId,
      int customerDistrictId,
      double amount,
      String oldData) {
    String data =
        customerId
            + " "
            + customerDistrictId
            + " "
            + customerWarehouseId
            + " "
            + districtId
            + " "
            + warehouseId
            + " "
            + String.format("%7.2f", amount)
            + " | "
            + oldData;
    if (data.length() > 500) {
      data = data.substring(0, 500);
    }
    return data;
  }

  private static JsonNode buildHistoryJson(
      int warehouseId,
      int districtId,
      int customerId,
      int customerWarehouseId,
      int customerDistrictId,
      String date,
      double amount,
      String warehouseName,
      String districtName) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.Name.HISTORY);
    node.put(Table.History.KEY_CUSTOMER_ID, customerId);
    node.put(Table.History.KEY_CUSTOMER_DID, customerDistrictId);
    node.put(Table.History.KEY_CUSTOMER_WID, customerWarehouseId);
    node.put(Table.History.KEY_DISTRICT_ID, districtId);
    node.put(Table.History.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.History.KEY_DATE, date);
    node.put(Table.History.KEY_AMOUNT, amount);
    if (warehouseName.length() > 10) {
      warehouseName = warehouseName.substring(0, 10);
    }
    if (districtName.length() > 10) {
      districtName = districtName.substring(0, 10);
    }
    node.put(Table.History.KEY_DATA, warehouseName + "    " + districtName);
    return node;
  }
}
