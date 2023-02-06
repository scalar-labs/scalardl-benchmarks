package com.scalar.dl.benchmarks.ycsb;

import com.scalar.kelpie.config.Config;
import java.util.Random;

public class YcsbCommon {
  static final String CONFIG_NAME = "ycsb_config";
  static final String CONTRACT_CONFIG_NAME = "contract";
  static final String LOAD_CONCURRENCY = "load_concurrency";
  static final String LOAD_BATCH_SIZE = "load_batch_size";
  static final String RECORD_COUNT = "record_count";
  static final String PAYLOAD_SIZE = "payload_size";
  static final String OPS_PER_TX = "ops_per_tx";
  static final String WORKLOAD = "workload";
  static final long DEFAULT_LOAD_CONCURRENCY = 1;
  static final long DEFAULT_LOAD_BATCH_SIZE = 1;
  static final long DEFAULT_RECORD_COUNT = 1000;
  static final long DEFAULT_PAYLOAD_SIZE = 1000;
  static final long DEFAULT_OPS_PER_TX = 2;
  static final String DEFAULT_WORKLOAD = "A";
  private static final String CREATE_ID = "create_id";
  private static final String CREATE_NAME = "create_name";
  private static final String CREATE_PATH = "create_path";
  private static final String WORKLOAD_A_ID = "a_id";
  private static final String WORKLOAD_A_NAME = "a_name";
  private static final String WORKLOAD_A_PATH = "a_path";
  private static final String WORKLOAD_C_ID = "c_id";
  private static final String WORKLOAD_C_NAME = "c_name";
  private static final String WORKLOAD_C_PATH = "c_path";
  private static final String WORKLOAD_F_ID = "f_id";
  private static final String WORKLOAD_F_NAME = "f_name";
  private static final String WORKLOAD_F_PATH = "f_path";
  private static final String DEFAULT_CREATE_ID = "create";
  private static final String DEFAULT_CREATE_NAME = "com.scalar.dl.benchmarks.ycsb.contract.Create";
  private static final String DEFAULT_CREATE_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/ycsb/contract/Create.class";
  private static final String DEFAULT_WORKLOAD_A_ID = "A";
  private static final String DEFAULT_WORKLOAD_A_NAME =
      "com.scalar.dl.benchmarks.ycsb.contract.WorkloadA";
  private static final String DEFAULT_WORKLOAD_A_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/ycsb/contract/WorkloadA.class";
  private static final String DEFAULT_WORKLOAD_C_ID = "C";
  private static final String DEFAULT_WORKLOAD_C_NAME =
      "com.scalar.dl.benchmarks.ycsb.contract.WorkloadC";
  private static final String DEFAULT_WORKLOAD_C_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/ycsb/contract/WorkloadC.class";
  private static final String DEFAULT_WORKLOAD_F_ID = "F";
  private static final String DEFAULT_WORKLOAD_F_NAME =
      "com.scalar.dl.benchmarks.ycsb.contract.WorkloadF";
  private static final String DEFAULT_WORKLOAD_F_PATH =
      "./build/classes/java/main/com/scalar/dl/benchmarks/ycsb/contract/WorkloadF.class";
  private static final int CHAR_START = 32; // [space]
  private static final int CHAR_STOP = 126; // [~]
  private static final char[] CHAR_SYMBOLS = new char[1 + CHAR_STOP - CHAR_START];

  public static int getLoadConcurrency(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, LOAD_CONCURRENCY, DEFAULT_LOAD_CONCURRENCY);
  }

  public static int getLoadBatchSize(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, LOAD_BATCH_SIZE, DEFAULT_LOAD_BATCH_SIZE);
  }

  public static int getRecordCount(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, RECORD_COUNT, DEFAULT_RECORD_COUNT);
  }

  public static String getWorkload(Config config) {
    return config.getUserString(CONFIG_NAME, WORKLOAD, DEFAULT_WORKLOAD);
  }

  public static int getPayloadSize(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, PAYLOAD_SIZE, DEFAULT_PAYLOAD_SIZE);
  }

  public static int getOpsPerTx(Config config) {
    return (int) config.getUserLong(CONFIG_NAME, OPS_PER_TX, DEFAULT_OPS_PER_TX);
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

  public static String getWorkloadAContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, WORKLOAD_A_ID, DEFAULT_WORKLOAD_A_ID);
  }

  public static String getWorkloadAContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, WORKLOAD_A_NAME, DEFAULT_WORKLOAD_A_NAME);
  }

  public static String getWorkloadAContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, WORKLOAD_A_PATH, DEFAULT_WORKLOAD_A_PATH);
  }

  public static String getWorkloadCContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, WORKLOAD_C_ID, DEFAULT_WORKLOAD_C_ID);
  }

  public static String getWorkloadCContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, WORKLOAD_C_NAME, DEFAULT_WORKLOAD_C_NAME);
  }

  public static String getWorkloadCContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, WORKLOAD_C_PATH, DEFAULT_WORKLOAD_C_PATH);
  }

  public static String getWorkloadFContractId(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, WORKLOAD_F_ID, DEFAULT_WORKLOAD_F_ID);
  }

  public static String getWorkloadFContractName(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, WORKLOAD_F_NAME, DEFAULT_WORKLOAD_F_NAME);
  }

  public static String getWorkloadFContractPath(Config config) {
    return config.getUserString(CONTRACT_CONFIG_NAME, WORKLOAD_F_PATH, DEFAULT_WORKLOAD_F_PATH);
  }

  static {
    for (int i = 0; i < CHAR_SYMBOLS.length; i++) {
      CHAR_SYMBOLS[i] = (char) (CHAR_START + i);
    }
  }

  private static final int[] FAST_MASKS = {
    554189328, // 10000
    277094664, // 01000
    138547332, // 00100
    69273666, // 00010
    34636833, // 00001
    346368330, // 01010
    727373493, // 10101
    588826161, // 10001
    935194491, // 11011
    658099827, // 10011
  };

  // This method is taken from BenchBase, which is available under the Apache License, Version 2.0.
  // https://github.com/cmu-db/benchbase/blob/bbe8c1db84ec81c6cdec6fbeca27b24b1b4e6612/src/main/java/com/oltpbenchmark/util/TextGenerator.java#L80
  // Copyright 2021 by OLTPBenchmark Project
  // https://www.apache.org/licenses/LICENSE-2.0
  public static char[] randomFastChars(Random rng, char[] chars) {
    // Ok so now the goal of this is to reduce the number of times that we have to
    // invoke a random number. We'll do this by grabbing a single random int
    // and then taking different bitmasks

    int num_rounds = chars.length / FAST_MASKS.length;
    int i = 0;
    for (int ctr = 0; ctr < num_rounds; ctr++) {
      int rand = rng.nextInt(10000); // CHAR_SYMBOLS.length);
      for (int mask : FAST_MASKS) {
        chars[i++] = CHAR_SYMBOLS[(rand | mask) % CHAR_SYMBOLS.length];
      }
    }
    // Use the old way for the remaining characters
    // I am doing this because I am too lazy to think of something more clever
    for (; i < chars.length; i++) {
      chars[i] = CHAR_SYMBOLS[rng.nextInt(CHAR_SYMBOLS.length)];
    }
    return (chars);
  }
}
