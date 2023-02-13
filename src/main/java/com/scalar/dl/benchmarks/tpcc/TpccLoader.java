package com.scalar.dl.benchmarks.tpcc;

import static com.scalar.dl.benchmarks.Common.getClientConfig;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.CONFIG_NAME;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.NUM_WAREHOUSES;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.getLoaderContractId;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.getLoaderContractName;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.getLoaderContractPath;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.getNewOrderContractId;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.getNewOrderContractName;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.getNewOrderContractPath;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.getPaymentContractId;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.getPaymentContractName;
import static com.scalar.dl.benchmarks.tpcc.TpccCommon.getPaymentContractPath;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.dl.benchmarks.tpcc.contract.Table;
import com.scalar.dl.benchmarks.tpcc.contract.Table.History;
import com.scalar.dl.client.config.ClientConfig;
import com.scalar.dl.client.exception.ClientException;
import com.scalar.dl.client.service.ClientService;
import com.scalar.dl.client.service.ClientServiceFactory;
import com.scalar.dl.ledger.service.StatusCode;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TpccLoader extends PreProcessor {
  private static final String LOAD_CONCURRENCY = "load_concurrency";
  private static final String START_WAREHOUSE = "load_start_warehouse";
  private static final String END_WAREHOUSE = "load_end_warehouse";
  private static final String SKIP_ITEM_LOAD = "skip_item_load";
  private static final long DEFAULT_LOAD_CONCURRENCY = 1;
  private static final long DEFAULT_START_WAREHOUSE = 1;
  private static final boolean DEFAULT_SKIP_ITEM_LOAD = false;
  private static final int QUEUE_SIZE = 10000;
  private final ObjectMapper mapper = new ObjectMapper();
  private final ClientServiceFactory factory;
  private final ClientService service;
  private final String loaderContractId;
  private final String loaderContractName;
  private final String loaderContractPath;
  private final String newOrderContractId;
  private final String newOrderContractName;
  private final String newOrderContractPath;
  private final String paymentContractId;
  private final String paymentContractName;
  private final String paymentContractPath;
  private final int concurrency;
  private final int startWarehouse;
  private final int endWarehouse;
  private final boolean skipItemLoad;

  public TpccLoader(Config config) {
    super(config);
    ClientConfig clientConfig = getClientConfig(config);
    this.factory = new ClientServiceFactory();
    this.service = factory.create(clientConfig);
    this.concurrency =
        (int) config.getUserLong(CONFIG_NAME, LOAD_CONCURRENCY, DEFAULT_LOAD_CONCURRENCY);
    this.skipItemLoad = config.getUserBoolean(CONFIG_NAME, SKIP_ITEM_LOAD, DEFAULT_SKIP_ITEM_LOAD);

    this.loaderContractId = getLoaderContractId(config);
    this.loaderContractName = getLoaderContractName(config);
    this.loaderContractPath = getLoaderContractPath(config);
    this.newOrderContractId = getNewOrderContractId(config);
    this.newOrderContractName = getNewOrderContractName(config);
    this.newOrderContractPath = getNewOrderContractPath(config);
    this.paymentContractId = getPaymentContractId(config);
    this.paymentContractName = getPaymentContractName(config);
    this.paymentContractPath = getPaymentContractPath(config);

    if (config.hasUserValue(CONFIG_NAME, END_WAREHOUSE)
        && config.hasUserValue(CONFIG_NAME, NUM_WAREHOUSES)) {
      throw new RuntimeException(
          END_WAREHOUSE + " and " + NUM_WAREHOUSES + " cannot be specified simultaneously");
    }

    this.startWarehouse =
        (int) config.getUserLong(CONFIG_NAME, START_WAREHOUSE, DEFAULT_START_WAREHOUSE);
    if (!config.hasUserValue(CONFIG_NAME, END_WAREHOUSE)
        && !config.hasUserValue(CONFIG_NAME, NUM_WAREHOUSES)) {
      this.endWarehouse = this.startWarehouse;
    } else if (config.hasUserValue(CONFIG_NAME, NUM_WAREHOUSES)) {
      this.endWarehouse =
          this.startWarehouse + (int) config.getUserLong(CONFIG_NAME, NUM_WAREHOUSES) - 1;
    } else {
      this.endWarehouse = (int) config.getUserLong(CONFIG_NAME, END_WAREHOUSE);
    }
  }

  @Override
  public void execute() {
    registerCertificateAndContracts();
    loadRecords();
  }

  @Override
  public void close() {
    factory.close();
  }

  private void registerCertificateAndContracts() {
    service.registerCertificate();
    service.registerContract(loaderContractId, loaderContractName, loaderContractPath);
    service.registerContract(newOrderContractId, newOrderContractName, newOrderContractPath);
    service.registerContract(paymentContractId, paymentContractName, paymentContractPath);
  }

  private void loadRecords() {
    ExecutorService executor = Executors.newFixedThreadPool(concurrency + 1);
    BlockingQueue<ObjectNode> queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    AtomicBoolean isAllQueued = new AtomicBoolean();
    AtomicInteger queuedCounter = new AtomicInteger();
    AtomicInteger succeededCounter = new AtomicInteger();
    AtomicInteger failedCounter = new AtomicInteger();

    for (int i = 0; i < concurrency; ++i) {
      executor.execute(
          () -> {
            while (true) {
              ObjectNode node = queue.poll();
              if (node == null) {
                if (isAllQueued.get()) {
                  break;
                }
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                continue;
              }
              while (true) {
                try {
                  String nonce = UUID.randomUUID().toString();
                  if (node.get(Table.KEY_TABLE_NAME).asText().equals(History.NAME)) {
                    node.put(Table.QueryParam.KEY_NONCE, nonce);
                  }
                  service.executeContract(nonce, loaderContractId, node.toString());
                  succeededCounter.incrementAndGet();
                  break;
                } catch (ClientException e) {
                  if (e.getStatusCode() != StatusCode.CONFLICT) {
                    e.printStackTrace();
                    failedCounter.incrementAndGet();
                  }
                }
              }
            }
          });
    }

    Future<?> future =
        executor.submit(
            () -> {
              while (!isAllQueued.get()
                  || succeededCounter.get() + failedCounter.get() < queuedCounter.get()) {
                logInfo(succeededCounter.get() + " succeeded, " + failedCounter + " failed");
                Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
              }
            });

    if (!skipItemLoad) {
      for (int itemId = 1; itemId <= Table.Item.ITEMS; itemId++) {
        try {
          queue.put(buildItemJson(itemId));
        } catch (InterruptedException e) {
          logError("queuing failed", e);
          throw new RuntimeException(e);
        }
        queuedCounter.incrementAndGet();
      }
    }
    try {
      queueWarehouses(queue, queuedCounter);
    } catch (InterruptedException e) {
      logError("queuing failed", e);
      throw new RuntimeException(e);
    }
    isAllQueued.set(true);

    try {
      future.get();
      executor.shutdown();
      Uninterruptibles.awaitTerminationUninterruptibly(executor);
    } catch (java.util.concurrent.ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    logInfo("all records have been inserted");
  }

  private void queueWarehouses(BlockingQueue<ObjectNode> queue, AtomicInteger counter)
      throws InterruptedException {
    LocalDateTime date = LocalDateTime.now(ZoneId.systemDefault());
    for (int warehouseId = startWarehouse; warehouseId <= endWarehouse; warehouseId++) {
      queue.put(buildWarehouseJson(warehouseId));
      counter.incrementAndGet();
      for (int itemId = 1; itemId <= Table.Warehouse.ITEMS; itemId++) {
        queue.put(buildStockJson(warehouseId, itemId));
        counter.incrementAndGet();
      }
      queueDistricts(queue, counter, warehouseId, date);
    }
  }

  private void queueDistricts(
      BlockingQueue<ObjectNode> queue, AtomicInteger counter, int warehouseId, LocalDateTime date)
      throws InterruptedException {
    for (int districtId = 1; districtId <= Table.Warehouse.DISTRICTS; districtId++) {
      queue.put(buildDistrictJson(warehouseId, districtId));
      counter.incrementAndGet();
      queueCustomers(queue, counter, warehouseId, districtId, date);
      queueOrders(queue, counter, warehouseId, districtId, date);
    }
  }

  private void queueCustomers(
      BlockingQueue<ObjectNode> queue,
      AtomicInteger counter,
      int warehouseId,
      int districtId,
      LocalDateTime date)
      throws InterruptedException {
    Multimap<String, Entry<Integer, String>> map = HashMultimap.create();
    for (int customerId = 1; customerId <= Table.District.CUSTOMERS; customerId++) {
      ObjectNode customer = buildCustomerJson(warehouseId, districtId, customerId, date);
      String last = customer.get(Table.Customer.KEY_LAST).asText();
      String first = customer.get(Table.Customer.KEY_FIRST).asText();
      map.put(last, Maps.immutableEntry(customerId, first));
      // customer
      queue.put(customer);
      counter.incrementAndGet();
      // history
      queue.put(
          buildHistoryJson(customerId, districtId, warehouseId, districtId, warehouseId, date));
      counter.incrementAndGet();
    }

    // customer_secondary
    map.asMap()
        .forEach(
            (last, entries) -> {
              try {
                queue.put(buildCustomerSecondaryJson(warehouseId, districtId, last, entries));
                counter.incrementAndGet();
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private void queueOrders(
      BlockingQueue<ObjectNode> queue,
      AtomicInteger counter,
      int warehouseId,
      int districtId,
      LocalDateTime date)
      throws InterruptedException {
    List<Integer> customers = new ArrayList<>();
    for (int customerId = 1; customerId <= Table.District.CUSTOMERS; customerId++) {
      customers.add(customerId);
    }
    Collections.shuffle(customers);
    Integer[] permutation = customers.toArray(new Integer[Table.District.CUSTOMERS]);

    for (int orderId = 1; orderId <= Table.District.ORDERS; orderId++) {
      int customerId = permutation[orderId - 1];
      ObjectNode order = buildOrderJson(warehouseId, districtId, orderId, customerId, date);
      // order & order-secondary
      queue.put(order);
      counter.incrementAndGet();
      int orderLineCount = order.get(Table.Order.KEY_OL_CNT).asInt();
      for (int number = 1; number <= orderLineCount; number++) {
        int itemId = TpccCommon.randomInt(1, Table.Item.ITEMS);
        // order-line
        queue.put(
            buildOrderLineJson(
                warehouseId, districtId, orderId, number, warehouseId, itemId, date));
        counter.incrementAndGet();
      }
      if (orderId > 2100) {
        // new-order
        queue.put(buildNewOrderJson(warehouseId, districtId, orderId));
        counter.incrementAndGet();
      }
    }
  }

  private ObjectNode buildCustomerJson(
      int warehouseId, int districtId, int customerId, LocalDateTime date) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.Customer.NAME);
    node.put(Table.Customer.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.Customer.KEY_DISTRICT_ID, districtId);
    node.put(Table.Customer.KEY_CUSTOMER_ID, customerId);
    node.put(
        Table.Customer.KEY_FIRST,
        TpccCommon.randomAlphaString(Table.Customer.MIN_FIRST, Table.Customer.MAX_FIRST));
    node.put(Table.Customer.KEY_MIDDLE, "OE");
    if (customerId <= 1000) {
      node.put(Table.Customer.KEY_LAST, TpccCommon.getLastName(customerId - 1));
    } else {
      node.put(Table.Customer.KEY_LAST, TpccCommon.getNonUniformRandomLastNameForLoad());
    }
    node.put(Table.Customer.KEY_DISCOUNT, TpccCommon.randomDouble(0, 5000, 10000));
    if (TpccCommon.randomInt(0, 99) < 10) {
      node.put(Table.Customer.KEY_CREDIT, "BC");
    } else {
      node.put(Table.Customer.KEY_CREDIT, "GC");
    }
    node.put(Table.Customer.KEY_CREDIT_LIM, 50000.00);
    node.put(Table.Customer.KEY_BALANCE, 10.00);
    node.put(Table.Customer.KEY_YTD_PAYMENT, 10.00);
    node.put(Table.Customer.KEY_PAYMENT_CNT, 1);
    node.put(Table.Customer.KEY_DELIVERY_CNT, 0);
    node.put(Table.Customer.KEY_PHONE, TpccCommon.randomNumberString(Table.Customer.PHONE_SIZE));
    node.put(Table.Customer.KEY_SINCE, date.format(TpccCommon.FORMATTER));
    node.put(
        Table.Customer.KEY_DATA,
        TpccCommon.randomAlphaString(Table.Customer.MIN_DATA, Table.Customer.MAX_DATA));
    putAddress(node, Table.Customer.PREFIX);
    return node;
  }

  private ObjectNode buildCustomerSecondaryJson(
      int warehouseId,
      int districtId,
      String lastName,
      Collection<Entry<Integer, String>> entries) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.CustomerSecondary.NAME);
    node.put(Table.CustomerSecondary.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.CustomerSecondary.KEY_DISTRICT_ID, districtId);
    node.put(Table.CustomerSecondary.KEY_LAST, lastName);
    ArrayNode keyCustomerIdList = mapper.createArrayNode();
    entries.forEach(
        entry ->
            keyCustomerIdList.add(
                mapper.createArrayNode().add(entry.getKey()).add(entry.getValue())));
    node.set(Table.CustomerSecondary.KEY_CUSTOMER_ID_LIST, keyCustomerIdList);
    return node;
  }

  private ObjectNode buildDistrictJson(int warehouseId, int districtId) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.District.NAME);
    node.put(Table.District.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.District.KEY_DISTRICT_ID, districtId);
    node.put(
        Table.District.KEY_NAME,
        TpccCommon.randomAlphaString(Table.District.MIN_NAME, Table.District.MAX_NAME));
    node.put(Table.District.KEY_TAX, TpccCommon.randomDouble(0, 2000, 10000));
    node.put(Table.District.KEY_YTD, 30000.00);
    node.put(Table.District.KEY_NEXT_O_ID, 3001);
    putAddress(node, Table.District.PREFIX);
    return node;
  }

  private ObjectNode buildHistoryJson(
      int customerId,
      int customerDistrictId,
      int customerWarehouseId,
      int districtId,
      int warehouseId,
      LocalDateTime date) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.History.NAME);
    node.put(Table.History.KEY_CUSTOMER_ID, customerId);
    node.put(Table.History.KEY_CUSTOMER_DID, customerDistrictId);
    node.put(Table.History.KEY_CUSTOMER_WID, customerWarehouseId);
    node.put(Table.History.KEY_DISTRICT_ID, districtId);
    node.put(Table.History.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.History.KEY_DATE, date.format(TpccCommon.FORMATTER));
    node.put(Table.History.KEY_AMOUNT, 10.00);
    node.put(
        Table.History.KEY_DATA,
        TpccCommon.randomAlphaString(Table.History.MIN_DATA, Table.History.MAX_DATA));
    return node;
  }

  private ObjectNode buildItemJson(int itemId) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.Item.NAME);
    node.put(Table.Item.KEY_ITEM_ID, itemId);
    node.put(
        Table.Item.KEY_NAME,
        TpccCommon.randomAlphaString(Table.Item.MIN_NAME, Table.Item.MAX_NAME));
    node.put(Table.Item.KEY_PRICE, TpccCommon.randomDouble(100, 1000, 100));
    node.put(
        Table.Item.KEY_DATA,
        TpccCommon.getRandomStringWithOriginal(Table.Item.MIN_DATA, Table.Item.MAX_DATA, 10));
    node.put(Table.Item.KEY_IM_ID, TpccCommon.randomInt(1, 10000));
    return node;
  }

  private ObjectNode buildNewOrderJson(int warehouseId, int districtId, int orderId) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.NewOrder.NAME);
    node.put(Table.NewOrder.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.NewOrder.KEY_DISTRICT_ID, districtId);
    node.put(Table.NewOrder.KEY_ORDER_ID, orderId);
    return node;
  }

  private ObjectNode buildOrderJson(
      int warehouseId, int districtId, int orderId, int customerId, LocalDateTime date) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.Order.NAME);
    node.put(Table.Order.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.Order.KEY_DISTRICT_ID, districtId);
    node.put(Table.Order.KEY_ORDER_ID, orderId);
    node.put(Table.Order.KEY_CUSTOMER_ID, customerId);
    if (orderId < 2101) {
      node.put(Table.Order.KEY_CARRIER_ID, TpccCommon.randomInt(1, 10));
    } else {
      node.put(Table.Order.KEY_CARRIER_ID, 0);
    }
    node.put(
        Table.Order.KEY_OL_CNT,
        TpccCommon.randomInt(Table.OrderLine.MIN_PER_ORDER, Table.OrderLine.MAX_PER_ORDER));
    node.put(Table.Order.KEY_ALL_LOCAL, 1);
    node.put(Table.Order.KEY_ENTRY_D, date.format(TpccCommon.FORMATTER));
    return node;
  }

  private ObjectNode buildOrderLineJson(
      int warehouseId,
      int districtId,
      int orderId,
      int number,
      int supplyWarehouseId,
      int itemId,
      LocalDateTime date) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.OrderLine.NAME);
    node.put(Table.OrderLine.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.OrderLine.KEY_DISTRICT_ID, districtId);
    node.put(Table.OrderLine.KEY_ORDER_ID, orderId);
    node.put(Table.OrderLine.KEY_NUMBER, number);
    node.put(Table.OrderLine.KEY_SUPPLY_W_ID, supplyWarehouseId);
    node.put(Table.OrderLine.KEY_ITEM_ID, itemId);
    if (orderId < 2101) {
      node.put(Table.OrderLine.KEY_DELIVERY_D, date.format(TpccCommon.FORMATTER));
      node.put(Table.OrderLine.KEY_AMOUNT, 0.00);
    } else {
      node.putNull(Table.OrderLine.KEY_DELIVERY_D);
      node.put(Table.OrderLine.KEY_AMOUNT, TpccCommon.randomDouble(1, 999999, 100));
    }
    node.put(Table.OrderLine.KEY_QUANTITY, 5);
    node.put(
        Table.OrderLine.KEY_DIST_INFO,
        TpccCommon.randomAlphaString(Table.OrderLine.DIST_INFO_SIZE));
    return node;
  }

  private ObjectNode buildStockJson(int warehouseId, int itemId) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.Stock.NAME);
    node.put(Table.Stock.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.Stock.KEY_ITEM_ID, itemId);
    node.put(Table.Stock.KEY_QUANTITY, TpccCommon.randomInt(10, 100));
    node.put(Table.Stock.KEY_YTD, 0.00);
    node.put(Table.Stock.KEY_ORDER_CNT, 0);
    node.put(Table.Stock.KEY_REMOTE_CNT, 0);
    for (int i = 0; i < 10; i++) {
      String key = Table.Stock.KEY_DIST_PREFIX + String.format("%02d", i + 1);
      node.put(key, TpccCommon.randomAlphaString(Table.Stock.DIST_SIZE));
    }
    return node;
  }

  private ObjectNode buildWarehouseJson(int warehouseId) {
    ObjectNode node = mapper.createObjectNode();
    node.put(Table.KEY_TABLE_NAME, Table.Warehouse.NAME);
    node.put(Table.Warehouse.KEY_WAREHOUSE_ID, warehouseId);
    node.put(Table.Warehouse.KEY_YTD, 300000.00);
    node.put(Table.Warehouse.KEY_TAX, TpccCommon.randomDouble(0, 2000, 10000));
    node.put(
        Table.Warehouse.KEY_WAREHOUSE_NAME,
        TpccCommon.randomAlphaString(Table.Warehouse.MIN_NAME, Table.Warehouse.MAX_NAME));
    putAddress(node, Table.Warehouse.PREFIX);
    return node;
  }

  private void putAddress(ObjectNode node, String prefix) {
    node.put(
        prefix + Table.Address.KEY_STREET_1,
        TpccCommon.randomAlphaString(Table.Address.MIN_STREET, Table.Address.MAX_STREET));
    node.put(
        prefix + Table.Address.KEY_STREET_2,
        TpccCommon.randomAlphaString(Table.Address.MIN_STREET, Table.Address.MAX_STREET));
    node.put(
        prefix + Table.Address.KEY_CITY,
        TpccCommon.randomAlphaString(Table.Address.MIN_CITY, Table.Address.MAX_CITY));
    node.put(
        prefix + Table.Address.KEY_STATE, TpccCommon.randomAlphaString(Table.Address.STATE_SIZE));
    node.put(
        prefix + Table.Address.KEY_ZIP,
        TpccCommon.randomAlphaString(Table.Address.ZIP_SIZE) + "11111");
  }
}
