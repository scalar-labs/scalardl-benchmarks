# Random Executor

`random-executor` is a test-only command that keeps a slow, steady trickle of YCSB contract
executions running against uniformly random assets. It exists to check how the
[scalardl-cleanup](https://github.com/scalar-labs/scalardl-tools) tools *behave* while a client keeps
touching the same key space — concurrent lazy recovery, read/write locks taken and released, assets
being appended to — which is exactly what [`recovery-seeder`](RECOVERY_SEEDER.md) deliberately
avoids.

Run it **without a `recovery-seeder` run in place**: the two commands cover different halves of
cleanup testing and must not be combined. See
[Where this fits in cleanup testing](#where-this-fits-in-cleanup-testing).

It is not a benchmark: one execution per interval (100 ms by default) keeps the load low on purpose
so that the traffic does not dominate the target, and no throughput or latency is measured. Each
execution runs workload C (read-only) or F (read-modify-write) on `--ops-per-tx` distinct assets
drawn uniformly from the loaded key space.

Failures are not fatal. Every execution is tallied per outcome (ScalarDL status code, or exception
class) and the run continues, because the traffic itself is the point. The first occurrence of each
outcome is logged with the user IDs involved; the rest are only counted.

## Where this fits in cleanup testing

Cleanup testing splits into two runs, and they must not overlap:

1. **Did the tools recover everything?** — a quiet run, with no traffic at all. Seed with
   [`recovery-seeder`](RECOVERY_SEEDER.md), run the cleanup tools, and verify what they resolved
   against the counts the seeder printed. Nothing else may touch the key space while those counts
   are still unverified, `random-executor` included: reading a seeded asset triggers lazy recovery,
   which resolves that asset's state before the cleanup tools ever see it, and there is then no way
   to tell a tool that missed records from traffic that got there first.
2. **Do the tools behave under concurrent traffic?** — this command, with **no seeding at all**.
   Load the data, start `random-executor`, and run the cleanup tools alongside it: locks should be
   taken and released, transactions should not get stuck, and neither side should report unexpected
   status codes. There are no exact counts to verify in this run, which is precisely why the traffic
   is harmless here.

## Build

```console
./gradlew installDist
```

The launch script is generated at `build/install/scalardl-benchmarks/bin/random-executor`.

## Prerequisites

**Load data first.** Run `YcsbLoader` (via Kelpie) as usual. It registers the contracts (default IDs
`C` and `F`) and loads `record_count` assets; pass that same count as `--total-assets`.

## Usage

```console
./build/install/scalardl-benchmarks/bin/random-executor \
  --properties client.properties \
  --total-assets 1000000
```

| Option | Required | Default | Description |
|---|---|---|---|
| `--properties <file>` | yes | - | ScalarDL client properties (the same one used for the loader) |
| `--total-assets <N>` | yes | - | Number of assets loaded by `YcsbLoader`; keys are drawn uniformly from `[0, N)` |
| `--interval-millis <ms>` | no | `100` | Interval between the start of consecutive executions |
| `--duration-seconds <s>` | no | `0` | Stop after this many seconds; `0` runs until interrupted |
| `--workload <C\|F\|MIXED>` | no | `MIXED` | `MIXED` picks a workload at random per execution |
| `--read-ratio <r>` | no | `0.5` | Fraction of executions that run C. Only valid with `--workload MIXED` |
| `--ops-per-tx <K>` | no | `1` | Assets per contract execution; the K assets are distinct and share one transaction |
| `--payload-size <bytes>` | no | `1000` | Payload size written by workload F |
| `--contract-id-c` / `--contract-id-f` | no | `C` / `F` | Override when the loader registered the contracts under non-default IDs |

Interrupting the command (Ctrl-C) stops it after the in-flight execution and prints the same summary
as reaching `--duration-seconds`. A stop only takes effect between executions, so it waits for the
in-flight one: the command allows it the gRPC deadlines of a whole execution
(`scalar.dl.client.grpc.deadline_duration_millis`, 60 s by default, times three when the auditor is
enabled) before giving up, and reports the budget it is waiting for.

```
1200 executions in 120.0 seconds (10.0/s, interval 100 ms): 1198 succeeded, 2 failed
  contract C: 604 executions, 604 succeeded, 0 failed
  contract F: 596 executions, 594 succeeded, 2 failed (CONFLICT=2)
```

Exit codes: `0` = the run reached `--duration-seconds` (regardless of individual failures); `1` =
every execution failed, which points at the wrong target, unregistered contract IDs, an unwritable
coordinator, or a `--total-assets` that does not match what was loaded; `2` = option/validation
error (nothing was executed); `130` = interrupted with Ctrl-C. The summary is printed before the JVM
exits in every one of those cases except `2`, so read it rather than the exit code — `130` is the
shell's usual SIGINT status and says nothing about how the run went.

## Notes

- A single client thread drives the loop, so an execution slower than `--interval-millis` delays the
  next one instead of overlapping with it. The summary reports how often that happened; the loop
  does not fire the missed executions back to back afterwards, which would turn a slow target into a
  burst of load.
- `CONFLICT` is a normal outcome once the traffic overlaps with the cleanup tools, or with itself on
  a small key space: workload F appends a new asset version, so two executions that draw the same
  key close together conflict. It is counted, not retried.
- Every asset the command touches is left in a committed state, so unlike the seeder it does not
  need to be undone. Workload F does append an asset version per execution, so the loaded assets
  grow while it runs.
