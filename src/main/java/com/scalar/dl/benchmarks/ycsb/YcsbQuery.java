package com.scalar.dl.benchmarks.ycsb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.dl.benchmarks.ycsb.contract.Const;
import java.util.concurrent.ThreadLocalRandom;

public class YcsbQuery {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static class A {
    public static ObjectNode generate(int recordCount, int opsPerTx, int payloadSize) {
      char[] payload = new char[payloadSize];
      ObjectNode node = MAPPER.createObjectNode();
      ArrayNode userIdsNode = MAPPER.createArrayNode();
      ArrayNode payloadsNode = MAPPER.createArrayNode();
      int i = 0;
      for (; i < opsPerTx / 2; ++i) {
        userIdsNode.add(ThreadLocalRandom.current().nextInt(recordCount));
      }
      for (; i < opsPerTx; ++i) {
        YcsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
        userIdsNode.add(ThreadLocalRandom.current().nextInt(recordCount));
        payloadsNode.add(new String(payload));
      }
      node.set(Const.KEY_USER_IDS, userIdsNode);
      node.set(Const.KEY_PAYLOADS, payloadsNode);
      return node;
    }
  }

  public static class C {
    public static ObjectNode generate(int recordCount, int opsPerTx) {
      ObjectNode node = MAPPER.createObjectNode();
      ArrayNode userIdsNode = MAPPER.createArrayNode();
      for (int i = 0; i < opsPerTx; ++i) {
        userIdsNode.add(ThreadLocalRandom.current().nextInt(recordCount));
      }
      node.set(Const.KEY_USER_IDS, userIdsNode);
      return node;
    }
  }

  public static class F {
    public static ObjectNode generate(int recordCount, int opsPerTx, int payloadSize) {
      char[] payload = new char[payloadSize];
      ObjectNode node = MAPPER.createObjectNode();
      ArrayNode userIdsNode = MAPPER.createArrayNode();
      ArrayNode payloadsNode = MAPPER.createArrayNode();
      for (int i = 0; i < opsPerTx; ++i) {
        YcsbCommon.randomFastChars(ThreadLocalRandom.current(), payload);
        userIdsNode.add(ThreadLocalRandom.current().nextInt(recordCount));
        payloadsNode.add(new String(payload));
      }
      node.set(Const.KEY_USER_IDS, userIdsNode);
      node.set(Const.KEY_PAYLOADS, payloadsNode);
      return node;
    }
  }
}
