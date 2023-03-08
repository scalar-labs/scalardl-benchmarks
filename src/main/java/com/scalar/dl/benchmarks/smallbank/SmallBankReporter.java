package com.scalar.dl.benchmarks.smallbank;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PostProcessor;
import com.scalar.kelpie.stats.Stats;

public class SmallBankReporter extends PostProcessor {

  public SmallBankReporter(Config config) {
    super(config);
  }

  @Override
  public void execute() {
    Stats stats = getStats();
    if (stats == null) {
      return;
    }
    getSummary();
    logInfo(
        "==== Statistics Details ====\n"
            + "Transaction abort count: "
            + getPreviousState().getString("abort_count")
            + "\n");
  }

  @Override
  public void close() {}
}
