package com.scalar.dl.benchmarks.ycsb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The record-ID ranges that a load run failed to insert, persisted as a JSON file so that a
 * follow-up run can load only the missing ranges. The Create contract appends blindly (it is not
 * idempotent), so resuming must never re-load IDs that were already inserted; this file is the
 * source of truth for what is still missing.
 */
public final class YcsbLoadFailedRanges {

  /** A half-open record-ID range [start, end). */
  public static final class Range {
    private final int start;
    private final int end;

    public Range(int start, int end) {
      if (start < 0 || end <= start) {
        throw new IllegalArgumentException(
            "invalid range [" + start + ", " + end + "): start must be >= 0 and < end");
      }
      this.start = start;
      this.end = end;
    }

    public int getStart() {
      return start;
    }

    public int getEnd() {
      return end;
    }

    public int size() {
      return end - start;
    }

    @Override
    public String toString() {
      return "[" + start + ", " + end + ")";
    }
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String KEY_RECORD_COUNT = "record_count";
  private static final String KEY_FAILED_RANGES = "failed_ranges";
  private static final String KEY_START = "start";
  private static final String KEY_END = "end";

  private final int recordCount;
  private final List<Range> ranges;

  public YcsbLoadFailedRanges(int recordCount, List<Range> ranges) {
    this.recordCount = recordCount;
    this.ranges = Collections.unmodifiableList(new ArrayList<>(ranges));
  }

  /** The record_count of the load run that produced this file, kept for sanity checking. */
  public int getRecordCount() {
    return recordCount;
  }

  public List<Range> getRanges() {
    return ranges;
  }

  public long totalRecords() {
    long total = 0;
    for (Range range : ranges) {
      total += range.size();
    }
    return total;
  }

  public void write(File file) {
    ObjectNode root = MAPPER.createObjectNode();
    root.put(KEY_RECORD_COUNT, recordCount);
    ArrayNode array = root.putArray(KEY_FAILED_RANGES);
    for (Range range : ranges) {
      array.addObject().put(KEY_START, range.getStart()).put(KEY_END, range.getEnd());
    }
    try {
      MAPPER.writerWithDefaultPrettyPrinter().writeValue(file, root);
    } catch (IOException e) {
      throw new UncheckedIOException("failed to write the failed-ranges file: " + file, e);
    }
  }

  public static YcsbLoadFailedRanges read(File file) {
    JsonNode root;
    try {
      root = MAPPER.readTree(file);
    } catch (IOException e) {
      throw new UncheckedIOException("failed to read the failed-ranges file: " + file, e);
    }
    if (!root.has(KEY_RECORD_COUNT) || !root.has(KEY_FAILED_RANGES)) {
      throw new IllegalArgumentException(
          "the failed-ranges file must have "
              + KEY_RECORD_COUNT
              + " and "
              + KEY_FAILED_RANGES
              + ": "
              + file);
    }
    List<Range> ranges = new ArrayList<>();
    for (JsonNode node : root.get(KEY_FAILED_RANGES)) {
      if (!node.has(KEY_START) || !node.has(KEY_END)) {
        throw new IllegalArgumentException("each range must have start and end: " + node);
      }
      ranges.add(new Range(node.get(KEY_START).asInt(), node.get(KEY_END).asInt()));
    }
    return new YcsbLoadFailedRanges(root.get(KEY_RECORD_COUNT).asInt(), ranges);
  }
}
