package com.scalar.dl.benchmarks.tpcc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.dl.benchmarks.tpcc.contract.Table;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

public class TpccQuery {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public enum Type {
    None,
    NewOrder,
    Payment,
  }

  public static class NewOrder {

    public static String generate(int numWarehouse) {
      int warehouseId = TpccCommon.randomInt(1, numWarehouse);
      int districtId = TpccCommon.randomInt(1, Table.Warehouse.DISTRICTS);
      int customerId = TpccCommon.getCustomerId();
      int rollback = TpccCommon.randomInt(1, 100);
      int orderLineCount = TpccCommon.randomInt(5, 15);
      int[] itemIds = new int[orderLineCount];
      int[] supplierWarehouseIds = new int[orderLineCount];
      int[] orderQuantities = new int[orderLineCount];
      Boolean remote = false;
      LocalDateTime date = LocalDateTime.now(ZoneId.systemDefault());

      for (int i = 0; i < orderLineCount; i++) {
        itemIds[i] = TpccCommon.getItemId();
        if (numWarehouse == 1 || TpccCommon.randomInt(1, 100) > 1) {
          supplierWarehouseIds[i] = warehouseId;
        } else {
          do {
            supplierWarehouseIds[i] = TpccCommon.randomInt(1, numWarehouse);
          } while (supplierWarehouseIds[i] == warehouseId);
          remote = true;
        }
        orderQuantities[i] = TpccCommon.randomInt(1, 10);
      }

      if (rollback == 1) {
        // set an unused item number to produce "not-found" for roll back
        itemIds[orderLineCount - 1] += Table.Item.ITEMS;
      }

      ObjectNode node = MAPPER.createObjectNode();
      node.put(Table.Warehouse.KEY_WAREHOUSE_ID, warehouseId);
      node.put(Table.District.KEY_DISTRICT_ID, districtId);
      node.put(Table.Customer.KEY_CUSTOMER_ID, customerId);
      node.put(Table.QueryParam.KEY_ORDER_LINE_COUNT, orderLineCount);
      node.put(Table.QueryParam.KEY_REMOTE, remote);

      ArrayNode array = MAPPER.createArrayNode();
      for (int i = 0; i < orderLineCount; i++) {
        array.add(
            MAPPER
                .createObjectNode()
                .put(Table.OrderLine.KEY_ITEM_ID, itemIds[i])
                .put(Table.OrderLine.KEY_SUPPLY_W_ID, supplierWarehouseIds[i])
                .put(Table.OrderLine.KEY_QUANTITY, orderQuantities[i]));
      }
      node.set(Table.QueryParam.KEY_ITEMS, array);
      node.put(Table.QueryParam.KEY_DATE, date.format(TpccCommon.FORMATTER));
      return node.toString();
    }
  }

  public static class Payment {

    public static String generate(int numWarehouse) {
      int warehouseId = TpccCommon.randomInt(1, numWarehouse);
      int districtId = TpccCommon.randomInt(1, Table.Warehouse.DISTRICTS);
      int customerId = 0;
      int customerWarehouseId;
      int customerDistrictId;
      String customerLastName = "";
      boolean byLastName;
      float paymentAmount = (float) (TpccCommon.randomInt(100, 500000) / 100.0);
      LocalDateTime date = LocalDateTime.now(ZoneId.systemDefault());

      int x = TpccCommon.randomInt(1, 100);
      if (x <= 85) {
        // home warehouse
        customerWarehouseId = warehouseId;
        customerDistrictId = districtId;
      } else {
        // remote warehouse
        if (numWarehouse > 1) {
          do {
            customerWarehouseId = TpccCommon.randomInt(1, numWarehouse);
          } while (customerWarehouseId == warehouseId);
        } else {
          customerWarehouseId = warehouseId;
        }
        customerDistrictId = TpccCommon.randomInt(1, Table.Warehouse.DISTRICTS);
      }

      int y = TpccCommon.randomInt(1, 100);
      if (y <= 60) {
        // by last name
        byLastName = true;
        customerLastName = TpccCommon.getNonUniformRandomLastNameForRun();
      } else {
        // by customer id
        byLastName = false;
        customerId = TpccCommon.getCustomerId();
      }

      ObjectNode node = MAPPER.createObjectNode();
      node.put(Table.Warehouse.KEY_WAREHOUSE_ID, warehouseId);
      node.put(Table.District.KEY_DISTRICT_ID, districtId);
      node.put(Table.Customer.KEY_WAREHOUSE_ID, customerWarehouseId);
      node.put(Table.Customer.KEY_DISTRICT_ID, customerDistrictId);
      node.put(Table.History.KEY_AMOUNT, paymentAmount);
      node.put(Table.QueryParam.KEY_BY_LAST_NAME, byLastName);
      if (byLastName) {
        node.put(Table.Customer.KEY_LAST, customerLastName);
      } else {
        node.put(Table.Customer.KEY_CUSTOMER_ID, customerId);
      }
      node.put(Table.QueryParam.KEY_DATE, date.format(TpccCommon.FORMATTER));
      node.put(Table.QueryParam.KEY_NONCE, UUID.randomUUID().toString());
      return node.toString();
    }
  }
}
