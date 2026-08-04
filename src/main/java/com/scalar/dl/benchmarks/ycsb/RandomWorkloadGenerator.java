package com.scalar.dl.benchmarks.ycsb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.dl.benchmarks.ycsb.contract.Const;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Generates random YCSB contract executions over the key space loaded by {@link YcsbLoader}: every
 * request runs workload C (read-only) or F (read-modify-write), chosen by the read ratio, on {@code
 * opsPerTx} distinct user IDs drawn uniformly from {@code [0, totalAssets)}.
 *
 * <p>Not thread-safe: one generator drives one execution loop.
 */
public final class RandomWorkloadGenerator {

  /** One generated contract execution. */
  public static final class Request {
    private final String contractId;
    private final long[] userIds;
    private final JsonNode argument;

    private Request(String contractId, long[] userIds, JsonNode argument) {
      this.contractId = contractId;
      this.userIds = userIds;
      this.argument = argument;
    }

    public String getContractId() {
      return contractId;
    }

    public long[] getUserIds() {
      return userIds;
    }

    public JsonNode getArgument() {
      return argument;
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String contractIdC;
  private final String contractIdF;
  private final double readRatio;
  private final long totalAssets;
  private final int opsPerTx;
  private final int payloadSize;
  private final Random random;

  /**
   * @param readRatio probability of workload C; 1 runs C only and 0 runs F only
   */
  public RandomWorkloadGenerator(
      String contractIdC,
      String contractIdF,
      double readRatio,
      long totalAssets,
      int opsPerTx,
      int payloadSize,
      Random random) {
    if (contractIdC == null || contractIdC.isEmpty()) {
      throw new IllegalArgumentException("--contract-id-c must not be empty");
    }
    if (contractIdF == null || contractIdF.isEmpty()) {
      throw new IllegalArgumentException("--contract-id-f must not be empty");
    }
    if (!(readRatio >= 0 && readRatio <= 1)) {
      throw new IllegalArgumentException(
          "--read-ratio must be between 0 and 1, but was " + readRatio);
    }
    if (totalAssets < 1) {
      throw new IllegalArgumentException("--total-assets must be >= 1, but was " + totalAssets);
    }
    if (opsPerTx < 1) {
      throw new IllegalArgumentException("--ops-per-tx must be >= 1, but was " + opsPerTx);
    }
    if (opsPerTx > totalAssets) {
      throw new IllegalArgumentException(
          "--ops-per-tx ("
              + opsPerTx
              + ") must not exceed --total-assets ("
              + totalAssets
              + "); one execution cannot touch more distinct assets than exist");
    }
    if (payloadSize < 1) {
      throw new IllegalArgumentException("--payload-size must be >= 1, but was " + payloadSize);
    }
    this.contractIdC = contractIdC;
    this.contractIdF = contractIdF;
    this.readRatio = readRatio;
    this.totalAssets = totalAssets;
    this.opsPerTx = opsPerTx;
    this.payloadSize = payloadSize;
    this.random = random;
  }

  /**
   * Returns the contract IDs this generator can produce, C first. Only reachable ones are listed,
   * so that a single-workload run does not report the other contract at all.
   */
  public List<String> getContractIds() {
    List<String> contractIds = new ArrayList<>(2);
    if (readRatio > 0) {
      contractIds.add(contractIdC);
    }
    if (readRatio < 1 && !contractIds.contains(contractIdF)) {
      contractIds.add(contractIdF);
    }
    return Collections.unmodifiableList(contractIds);
  }

  /** Generates the next execution. */
  public Request next() {
    long[] userIds = nextUserIds();
    // nextDouble() returns [0, 1), so readRatio 1 always reads and 0 never does.
    boolean read = random.nextDouble() < readRatio;
    if (read) {
      return new Request(contractIdC, userIds, buildArgument(userIds, false));
    }
    return new Request(contractIdF, userIds, buildArgument(userIds, true));
  }

  private long[] nextUserIds() {
    long[] userIds = new long[opsPerTx];
    if (opsPerTx == 1) {
      userIds[0] = nextUserId();
      return userIds;
    }
    // Distinct within one execution: the same asset twice in one transaction would be read and
    // written twice by workload F, so the assets touched would not match the operation count.
    Set<Long> picked = new HashSet<>();
    for (int i = 0; i < opsPerTx; i++) {
      long userId;
      do {
        userId = nextUserId();
      } while (!picked.add(userId));
      userIds[i] = userId;
    }
    return userIds;
  }

  private long nextUserId() {
    if (totalAssets <= Integer.MAX_VALUE) {
      return random.nextInt((int) totalAssets);
    }
    return Math.floorMod(random.nextLong(), totalAssets);
  }

  private JsonNode buildArgument(long[] userIds, boolean withPayloads) {
    ObjectNode argument = MAPPER.createObjectNode();
    ArrayNode userIdsNode = MAPPER.createArrayNode();
    for (long userId : userIds) {
      userIdsNode.add(userId);
    }
    argument.set(Const.KEY_USER_IDS, userIdsNode);
    if (withPayloads) {
      ArrayNode payloadsNode = MAPPER.createArrayNode();
      char[] payload = new char[payloadSize];
      for (int i = 0; i < userIds.length; i++) {
        YcsbCommon.randomFastChars(random, payload);
        payloadsNode.add(new String(payload));
      }
      argument.set(Const.KEY_PAYLOADS, payloadsNode);
    }
    return argument;
  }
}
