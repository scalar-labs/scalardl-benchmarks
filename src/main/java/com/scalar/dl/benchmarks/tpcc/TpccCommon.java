package com.scalar.dl.benchmarks.tpcc;

import com.scalar.dl.benchmarks.tpcc.contract.Table;
import com.scalar.kelpie.config.Config;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

public class TpccCommon {
  static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
  static final String CONFIG_NAME = "tpcc_config";
  static final String CONTRACT_CONFIG_NAME = "contract";
  static final String NUM_WAREHOUSES = "num_warehouses";
  static final long DEFAULT_NUM_WAREHOUSES = 1;
  private static final String TABLE_LOADER_ID = "table_loader_id";
  private static final String TABLE_LOADER_NAME = "table_loader_name";
  private static final String TABLE_LOADER_PATH = "table_loader_path";
  private static final String NEW_ORDER_ID = "new_order_id";
  private static final String NEW_ORDER_NAME = "new_order_name";
  private static final String NEW_ORDER_PATH = "new_order_path";
  private static final String PAYMENT_ID = "payment_id";
  private static final String PAYMENT_NAME = "payment_name";
  private static final String PAYMENT_PATH = "payment_path";
  private static final String DEFAULT_TABLE_LOADER_ID = "table-loader";
  private static final String DEFAULT_TABLE_LOADER_NAME =
      "com.scalar.dl.benchmarks.tpcc.contract.TableLoader";
  private static final String DEFAULT_TABLE_LOADER_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/tpcc/contract/TableLoader.class";
  private static final String DEFAULT_NEW_ORDER_ID = "new-order";
  private static final String DEFAULT_NEW_ORDER_NAME =
      "com.scalar.dl.benchmarks.tpcc.contract.NewOrder";
  private static final String DEFAULT_NEW_ORDER_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/tpcc/contract/NewOrder.class";
  private static final String DEFAULT_PAYMENT_ID = "payment";
  private static final String DEFAULT_PAYMENT_NAME =
      "com.scalar.dl.benchmarks.tpcc.contract.Payment";
  private static final String DEFAULT_PAYMENT_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/tpcc/contract/Payment.class";

  // for customer last name
  static final String[] NAME_TOKENS = {
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"
  };

  // for non-uniform random
  static final int CUSTOMER_LASTNAME_IN_LOAD = 250;
  static final int CUSTOMER_LASTNAME_IN_RUN = 150;
  static final int CUSTOMER_ID = 987;
  static final int ORDER_LINE_ITEM_ID = 5987;

  public static String getLoaderContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, TABLE_LOADER_ID, DEFAULT_TABLE_LOADER_ID);
  }

  public static String getLoaderContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, TABLE_LOADER_NAME, DEFAULT_TABLE_LOADER_NAME);
  }

  public static String getLoaderContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, TABLE_LOADER_PATH, DEFAULT_TABLE_LOADER_PATH);
  }

  public static String getNewOrderContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, NEW_ORDER_ID, DEFAULT_NEW_ORDER_ID);
  }

  public static String getNewOrderContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, NEW_ORDER_NAME, DEFAULT_NEW_ORDER_NAME);
  }

  public static String getNewOrderContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, NEW_ORDER_PATH, DEFAULT_NEW_ORDER_PATH);
  }

  public static String getPaymentContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, PAYMENT_ID, DEFAULT_PAYMENT_ID);
  }

  public static String getPaymentContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, PAYMENT_NAME, DEFAULT_PAYMENT_NAME);
  }

  public static String getPaymentContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, PAYMENT_PATH, DEFAULT_PAYMENT_PATH);
  }

  /**
   * Returns a customer ID for transaction arguments.
   *
   * @return a customer ID for transaction arguments
   */
  public static int getCustomerId() {
    return nonUniformRandom(1023, 1, Table.District.CUSTOMERS);
  }

  /**
   * Returns an item ID for transaction arguments.
   *
   * @return an item ID for transaction arguments
   */
  public static int getItemId() {
    return nonUniformRandom(8191, 1, Table.Item.ITEMS);
  }

  /**
   * Returns a random {@code String} including "ORIGINAL".
   *
   * @param minLength minimum length of generated string
   * @param maxLength maximum length of generated string
   * @param rate probability of including "ORIGINAL"
   * @return a random {@code String} including "ORIGINAL"
   */
  public static String getRandomStringWithOriginal(int minLength, int maxLength, int rate) {
    int length = randomInt(minLength, maxLength);
    if (TpccCommon.randomInt(0, 99) < rate) {
      int startOriginal = TpccCommon.randomInt(2, length - 8);
      return TpccCommon.randomAlphaString(startOriginal - 1)
          + "ORIGINAL"
          + TpccCommon.randomAlphaString(length - startOriginal - 9);
    } else {
      return TpccCommon.randomAlphaString(length);
    }
  }

  /**
   * Returns a customer last name {@code String} for transaction argument.
   *
   * @return a customer last name {@code String} for transaction argument
   */
  public static String getNonUniformRandomLastNameForRun() {
    return getLastName(nonUniformRandom(255, 0, 999, false));
  }

  /**
   * Returns a customer last name {@code String} for load.
   *
   * @return a customer last name {@code String} for load
   */
  public static String getNonUniformRandomLastNameForLoad() {
    return getLastName(nonUniformRandom(255, 0, 999, true));
  }

  /**
   * Returns a customer last name {@code String} for load.
   *
   * @param num a number to select name tokens
   * @return a customer last name {@code String} for load
   */
  public static String getLastName(int num) {
    return NAME_TOKENS[num / 100] + NAME_TOKENS[(num / 10) % 10] + NAME_TOKENS[num % 10];
  }

  private static String randomString(int minLength, int maxLength, boolean isNumberOnly) {
    byte[] characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".getBytes();
    int offset = isNumberOnly ? 9 : (characters.length - 1);
    int length = randomInt(minLength, maxLength);
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; ++i) {
      bytes[i] = characters[randomInt(0, offset)];
    }
    return new String(bytes);
  }

  public static String randomAlphaString(int length) {
    return randomString(length, length, false);
  }

  public static String randomAlphaString(int minLength, int maxLength) {
    return randomString(minLength, maxLength, false);
  }

  public static String randomNumberString(int length) {
    return randomNumberString(length, length);
  }

  public static String randomNumberString(int minLength, int maxLength) {
    return randomString(minLength, maxLength, true);
  }

  public static int randomInt(int min, int max) {
    return (int) (ThreadLocalRandom.current().nextDouble() * (max - min + 1) + min);
  }

  public static double randomDouble(int min, int max, int divider) {
    return randomInt(min, max) / (double) divider;
  }

  public static int nonUniformRandom(int a, int min, int max) {
    return nonUniformRandom(a, min, max, false);
  }

  public static int nonUniformRandom(int a, int min, int max, boolean isLoad) {
    int c = getConstantForNonUniformRandom(a, isLoad);
    return (((randomInt(0, a) | randomInt(min, max)) + c) % (max - min + 1)) + min;
  }

  private static int getConstantForNonUniformRandom(int a, boolean isLoad) {
    switch (a) {
      case 255:
        return isLoad ? CUSTOMER_LASTNAME_IN_LOAD : CUSTOMER_LASTNAME_IN_RUN;
      case 1023:
        return CUSTOMER_ID;
      case 8191:
        return ORDER_LINE_ITEM_ID;
      default:
        return 0; // BUG
    }
  }
}
