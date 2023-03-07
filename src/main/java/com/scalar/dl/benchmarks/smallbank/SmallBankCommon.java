package com.scalar.dl.benchmarks.smallbank;

import com.scalar.kelpie.config.Config;

public class SmallBankCommon {
  static final String CONFIG_NAME = "smallbank_config";
  static final String CONTRACT_CONFIG_NAME = "contract";
  static final String LOAD_CONCURRENCY = "load_concurrency";
  static final String LOAD_BATCH_SIZE = "load_batch_size";
  static final String NUM_ACCOUNTS = "num_accounts";
  private static final String CREATE_ID = "create_account_id";
  private static final String CREATE_NAME = "create_account_name";
  private static final String CREATE_PATH = "create_account_path";
  private static final String BALANCE_ID = "balance_id";
  private static final String BALANCE_NAME = "balance_name";
  private static final String BALANCE_PATH = "balance_path";
  private static final String SAVING_ID = "transact_savings_id";
  private static final String SAVING_NAME = "transact_savings_name";
  private static final String SAVING_PATH = "transact_savings_path";
  private static final String DEPOSIT_ID = "deposit_checking_id";
  private static final String DEPOSIT_NAME = "deposit_checking_name";
  private static final String DEPOSIT_PATH = "deposit_checking_path";
  private static final String PAYMENT_ID = "send_payment_id";
  private static final String PAYMENT_NAME = "send_payment_name";
  private static final String PAYMENT_PATH = "send_payment_path";
  private static final String CHECK_ID = "write_check_id";
  private static final String CHECK_NAME = "write_check_name";
  private static final String CHECK_PATH = "write_check_path";
  private static final String AMALGAMATE_ID = "amalgamate_id";
  private static final String AMALGAMATE_NAME = "amalgamate_name";
  private static final String AMALGAMATE_PATH = "amalgamate_path";

  static final long DEFAULT_LOAD_CONCURRENCY = 1;
  static final long DEFAULT_LOAD_BATCH_SIZE = 1;
  static final long DEFAULT_NUM_ACCOUNTS = 100000;
  private static final String DEFAULT_CREATE_ID = "create_account";
  private static final String DEFAULT_CREATE_NAME =
      "com.scalar.dl.benchmarks.smallbank.contract.CreateAccount";
  private static final String DEFAULT_CREATE_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/smallbank/contract/CreateAccount.class";
  private static final String DEFAULT_BALANCE_ID = "balance";
  private static final String DEFAULT_BALANCE_NAME =
      "com.scalar.dl.benchmarks.smallbank.contract.Balance";
  private static final String DEFAULT_BALANCE_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/smallbank/contract/Balance.class";
  private static final String DEFAULT_SAVING_ID = "transact_savings";
  private static final String DEFAULT_SAVING_NAME =
      "com.scalar.dl.benchmarks.smallbank.contract.TransactSavings";
  private static final String DEFAULT_SAVING_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/smallbank/contract/TransactSavings.class";
  private static final String DEFAULT_DEPOSIT_ID = "deposit_checking";
  private static final String DEFAULT_DEPOSIT_NAME =
      "com.scalar.dl.benchmarks.smallbank.contract.DepositChecking";
  private static final String DEFAULT_DEPOSIT_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/smallbank/contract/DepositChecking.class";
  private static final String DEFAULT_PAYMENT_ID = "send_payment";
  private static final String DEFAULT_PAYMENT_NAME =
      "com.scalar.dl.benchmarks.smallbank.contract.SendPayment";
  private static final String DEFAULT_PAYMENT_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/smallbank/contract/SendPayment.class";
  private static final String DEFAULT_CHECK_ID = "write_check";
  private static final String DEFAULT_CHECK_NAME =
      "com.scalar.dl.benchmarks.smallbank.contract.WriteCheck";
  private static final String DEFAULT_CHECK_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/smallbank/contract/WriteCheck.class";
  private static final String DEFAULT_AMALGAMATE_ID = "amalgamate";
  private static final String DEFAULT_AMALGAMATE_NAME =
      "com.scalar.dl.benchmarks.smallbank.contract.Amalgamate";
  private static final String DEFAULT_AMALGAMATE_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/smallbank/contract/Amalgamate.class";

  public static int getLoadConcurrency(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, LOAD_CONCURRENCY, DEFAULT_LOAD_CONCURRENCY);
  }

  public static int getLoadBatchSize(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, LOAD_BATCH_SIZE, DEFAULT_LOAD_BATCH_SIZE);
  }

  public static int getNumAccounts(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, NUM_ACCOUNTS, DEFAULT_NUM_ACCOUNTS);
  }

  public static String getCreateContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, CREATE_ID, DEFAULT_CREATE_ID);
  }

  public static String getCreateContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, CREATE_NAME, DEFAULT_CREATE_NAME);
  }

  public static String getCreateContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, CREATE_PATH, DEFAULT_CREATE_PATH);
  }

  public static String getBalanceContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, BALANCE_ID, DEFAULT_BALANCE_ID);
  }

  public static String getBalanceContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, BALANCE_NAME, DEFAULT_BALANCE_NAME);
  }

  public static String getBalanceContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, BALANCE_PATH, DEFAULT_BALANCE_PATH);
  }

  public static String getSavingContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, SAVING_ID, DEFAULT_SAVING_ID);
  }

  public static String getSavingContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, SAVING_NAME, DEFAULT_SAVING_NAME);
  }

  public static String getSavingContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, SAVING_PATH, DEFAULT_SAVING_PATH);
  }

  public static String getDepositContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, DEPOSIT_ID, DEFAULT_DEPOSIT_ID);
  }

  public static String getDepositContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, DEPOSIT_NAME, DEFAULT_DEPOSIT_NAME);
  }

  public static String getDepositContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, DEPOSIT_PATH, DEFAULT_DEPOSIT_PATH);
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

  public static String getCheckContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, CHECK_ID, DEFAULT_CHECK_ID);
  }

  public static String getCheckContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, CHECK_NAME, DEFAULT_CHECK_NAME);
  }

  public static String getCheckContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, CHECK_PATH, DEFAULT_CHECK_PATH);
  }

  public static String getAmalgamateContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, AMALGAMATE_ID, DEFAULT_AMALGAMATE_ID);
  }

  public static String getAmalgamateContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, AMALGAMATE_NAME, DEFAULT_AMALGAMATE_NAME);
  }

  public static String getAmalgamateContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, AMALGAMATE_PATH, DEFAULT_AMALGAMATE_PATH);
  }
}
