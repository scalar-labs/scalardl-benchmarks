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

public class NewOrder extends JacksonBasedContract {
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public JsonNode invoke(
      Ledger<JsonNode> ledger, JsonNode argument, @Nullable JsonNode properties) {
    int warehouseId = argument.get(Table.Warehouse.KEY_WAREHOUSE_ID).asInt();
    int districtId = argument.get(Table.District.KEY_DISTRICT_ID).asInt();
    int customerId = argument.get(Table.Customer.KEY_CUSTOMER_ID).asInt();
    int orderLineCount = argument.get(Table.QueryParam.KEY_ORDER_LINE_COUNT).asInt();
    boolean remote = argument.get(Table.QueryParam.KEY_REMOTE).asBoolean();
    JsonNode items = argument.get(Table.QueryParam.KEY_ITEMS);
    String date = argument.get(Table.QueryParam.KEY_DATE).asText();

    // Get warehouse
    Optional<Asset<JsonNode>> warehouse = ledger.get(createWarehouseKey(warehouseId));
    if (!warehouse.isPresent()) {
      throw new ContractContextException(Message.ERR_ASSET_NOT_FOUND);
    }
    JsonNode warehouseNode = warehouse.get().data();

    // Get and update district
    String districtKey = createDistrictKey(warehouseId, districtId);
    Optional<Asset<JsonNode>> district = ledger.get(districtKey);
    if (!district.isPresent()) {
      throw new ContractContextException(Message.ERR_ASSET_NOT_FOUND);
    }

    JsonNode oldDistrictNode = district.get().data();
    int orderId = oldDistrictNode.get(Table.District.KEY_NEXT_O_ID).asInt();
    ObjectNode newDistrictNode = oldDistrictNode.deepCopy();
    newDistrictNode.put(Table.District.KEY_NEXT_O_ID, orderId + 1);
    ledger.put(districtKey, newDistrictNode);

    // Get customer
    String customerKey = createCustomerKey(warehouseId, districtId, customerId);
    Optional<Asset<JsonNode>> customer = ledger.get(customerKey);
    if (!customer.isPresent()) {
      throw new ContractContextException(Message.ERR_ASSET_NOT_FOUND);
    }
    JsonNode customerNode = customer.get().data();

    // Insert new-order
    String newOrderKey = createNewOrderKey(warehouseId, districtId, orderId);
    ledger.put(newOrderKey, buildNewOrderJson(warehouseId, districtId, orderId));

    // Insert order
    String orderKey = createOrderKey(warehouseId, districtId, orderId);
    ledger.put(
        orderKey,
        buildOrderJson(warehouseId, districtId, orderId, customerId, orderLineCount, remote, date));

    double warehouseTax = warehouseNode.get(Table.Warehouse.KEY_TAX).asDouble();
    // CHECK: can it be from the old district?
    double districtTax = oldDistrictNode.get(Table.District.KEY_TAX).asDouble();
    double discount = customerNode.get(Table.Customer.KEY_DISCOUNT).asDouble();

    // Insert order-line
    for (int orderLineNumber = 1; orderLineNumber <= orderLineCount; orderLineNumber++) {
      JsonNode queryItem = items.get(orderLineNumber - 1);
      int itemId = queryItem.get(Table.OrderLine.KEY_ITEM_ID).asInt();
      int supplyWarehouseId = queryItem.get(Table.OrderLine.KEY_SUPPLY_W_ID).asInt();
      int quantity = queryItem.get(Table.OrderLine.KEY_QUANTITY).asInt();

      // Get item
      String itemKey = createItemKey(itemId);
      Optional<Asset<JsonNode>> item = ledger.get(itemKey);
      if (!item.isPresent()) {
        throw new ContractContextException(Message.ERR_ASSET_NOT_FOUND);
      }
      JsonNode itemNode = item.get().data();

      // Get and update stock
      String stockKey = createStockKey(supplyWarehouseId, itemId);
      Optional<Asset<JsonNode>> stock = ledger.get(stockKey);
      if (!stock.isPresent()) {
        throw new ContractContextException(Message.ERR_ASSET_NOT_FOUND);
      }
      JsonNode oldStockNode = stock.get().data();

      ObjectNode newStockNode = oldStockNode.deepCopy();
      newStockNode.put(
          Table.Stock.KEY_YTD, oldStockNode.get(Table.Stock.KEY_YTD).asInt() + quantity);
      newStockNode.put(
          Table.Stock.KEY_ORDER_CNT, oldStockNode.get(Table.Stock.KEY_ORDER_CNT).asInt() + 1);
      if (remote) {
        newStockNode.put(
            Table.Stock.KEY_REMOTE_CNT, oldStockNode.get(Table.Stock.KEY_REMOTE_CNT).asInt() + 1);
      }
      int stockQuantity = oldStockNode.get(Table.Stock.KEY_QUANTITY).asInt();
      if (stockQuantity > quantity + 10) {
        stockQuantity -= quantity;
      } else {
        stockQuantity = (stockQuantity - quantity) + 91;
      }
      newStockNode.put(Table.Stock.KEY_QUANTITY, stockQuantity);
      ledger.put(stockKey, newStockNode);

      // Insert order-line
      double itemPrice = itemNode.get(Table.Item.KEY_PRICE).asDouble();
      double amount = quantity * itemPrice * (1.0 + warehouseTax + districtTax) * (1.0 - discount);

      String orderLineKey = createOrderLineKey(warehouseId, districtId, orderId, orderLineNumber);
      ledger.put(
          orderLineKey,
          buildOrderLineJson(
              warehouseId,
              districtId,
              orderId,
              orderLineNumber,
              itemId,
              supplyWarehouseId,
              quantity,
              amount,
              oldStockNode));
    }

    return null;
  }

  private String createCustomerKey(int warehouseId, int districtId, int customerId) {
    return String.format("%02d", Table.Code.CUSTOMER)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + String.format("%010d", customerId);
  }

  private String createDistrictKey(int warehouseId, int districtId) {
    return String.format("%02d", Table.Code.DISTRICT)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId);
  }

  private String createItemKey(int itemId) {
    return String.format("%02d", Table.Code.ITEM) + String.format("%06d", itemId);
  }

  private String createNewOrderKey(int warehouseId, int districtId, int orderId) {
    return String.format("%02d", Table.Code.NEW_ORDER)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + String.format("%010d", orderId);
  }

  private String createOrderKey(int warehouseId, int districtId, int orderId) {
    return String.format("%02d", Table.Code.ORDER)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + String.format("%010d", orderId);
  }

  private String createOrderLineKey(
      int warehouseId, int districtId, int orderId, int orderLineNumber) {
    return String.format("%02d", Table.Code.ORDER_LINE)
        + String.format("%05d", warehouseId)
        + String.format("%03d", districtId)
        + String.format("%010d", orderId)
        + String.format("%03d", orderLineNumber);
  }

  private String createStockKey(int warehouseId, int itemId) {
    return String.format("%02d", Table.Code.STOCK)
        + String.format("%05d", warehouseId)
        + String.format("%06d", itemId);
  }

  private String createWarehouseKey(int warehouseId) {
    return String.format("%02d", Table.Code.WAREHOUSE) + String.format("%05d", warehouseId);
  }

  private static JsonNode buildNewOrderJson(int warehouseId, int districtId, int orderId) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.Name.NEW_ORDER);
    node.put(Table.NewOrder.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.NewOrder.KEY_DISTRICT_ID, districtId);
    node.put(Table.NewOrder.KEY_ORDER_ID, orderId);
    return node;
  }

  private static JsonNode buildOrderJson(
      int warehouseId,
      int districtId,
      int orderId,
      int customerId,
      int orderLineCount,
      Boolean remote,
      String date) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.Name.ORDER);
    node.put(Table.Order.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.Order.KEY_DISTRICT_ID, districtId);
    node.put(Table.Order.KEY_ORDER_ID, orderId);
    node.put(Table.Order.KEY_CUSTOMER_ID, customerId);
    node.put(Table.Order.KEY_CARRIER_ID, -1); // JsonObjectBuilder cannot handle null
    node.put(Table.Order.KEY_OL_CNT, orderLineCount);
    node.put(Table.Order.KEY_ALL_LOCAL, !remote);
    node.put(Table.Order.KEY_ENTRY_D, date);
    return node;
  }

  private static JsonNode buildOrderLineJson(
      int warehouseId,
      int districtId,
      int orderId,
      int orderLineNumber,
      int itemId,
      int supplyWarehouseId,
      int quantity,
      double amount,
      JsonNode stock) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.Name.ORDER_LINE);
    node.put(Table.OrderLine.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.OrderLine.KEY_DISTRICT_ID, districtId);
    node.put(Table.OrderLine.KEY_ORDER_ID, orderId);
    node.put(Table.OrderLine.KEY_NUMBER, orderLineNumber);
    node.put(Table.OrderLine.KEY_ITEM_ID, itemId);
    node.put(Table.OrderLine.KEY_SUPPLY_W_ID, supplyWarehouseId);
    node.put(Table.OrderLine.KEY_QUANTITY, quantity);
    node.put(Table.OrderLine.KEY_AMOUNT, amount);
    switch (districtId) {
      case 1:
        node.put(Table.OrderLine.KEY_DIST_INFO, stock.get(Table.Stock.KEY_DISTRICT01).asText());
        break;
      case 2:
        node.put(Table.OrderLine.KEY_DIST_INFO, stock.get(Table.Stock.KEY_DISTRICT02).asText());
        break;
      case 3:
        node.put(Table.OrderLine.KEY_DIST_INFO, stock.get(Table.Stock.KEY_DISTRICT03).asText());
        break;
      case 4:
        node.put(Table.OrderLine.KEY_DIST_INFO, stock.get(Table.Stock.KEY_DISTRICT04).asText());
        break;
      case 5:
        node.put(Table.OrderLine.KEY_DIST_INFO, stock.get(Table.Stock.KEY_DISTRICT05).asText());
        break;
      case 6:
        node.put(Table.OrderLine.KEY_DIST_INFO, stock.get(Table.Stock.KEY_DISTRICT06).asText());
        break;
      case 7:
        node.put(Table.OrderLine.KEY_DIST_INFO, stock.get(Table.Stock.KEY_DISTRICT07).asText());
        break;
      case 8:
        node.put(Table.OrderLine.KEY_DIST_INFO, stock.get(Table.Stock.KEY_DISTRICT08).asText());
        break;
      case 9:
        node.put(Table.OrderLine.KEY_DIST_INFO, stock.get(Table.Stock.KEY_DISTRICT09).asText());
        break;
      case 10:
        node.put(Table.OrderLine.KEY_DIST_INFO, stock.get(Table.Stock.KEY_DISTRICT10).asText());
        break;
      default:
        throw new ContractContextException(Message.ERR_ILLEGAL_ASSET);
    }
    return node;
  }
}
