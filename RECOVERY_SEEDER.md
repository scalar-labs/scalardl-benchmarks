# Recovery State Seeder

`recovery-seeder` is a test-only command that seeds "recovery-needed" states for testing the
[scalardl-cleanup](https://github.com/scalar-labs/scalardl-tools) tools at scale. It executes the
YCSB contracts registered by `YcsbLoader` while the ScalarDB Coordinator is intentionally made
unwritable, so that every contract execution fails at the commit-state phase and leaves:

| Workload | Ledger side | Auditor side |
|---|---|---|
| F (read-modify-write) | PREPARED records: 1 per asset on the `asset` table, plus 1 per asset on `asset_metadata` when `scalar.dl.ledger.direct_asset_access.enabled=false` | 1 write lock per asset |
| C (read-only) | nothing (the write set is empty) | 1 read lock per asset |

The seeded counts are exact and deterministic: keys are assigned uniformly over the loaded key
space, every execution is verified to fail with `UNKNOWN_TRANSACTION_STATUS` (501), and any
deviation (an execution that commits, or fails with any other error) aborts the run immediately
(fail-fast) with a full accounting of every planned execution.

## Build

```console
./gradlew installDist
```

The launch script is generated at `build/install/scalardl-benchmarks/bin/recovery-seeder`.

## Prerequisites

1. **Load data first.** Run `YcsbLoader` (via Kelpie) as usual. It registers the contracts
   (default IDs `C` and `F`) and loads `record_count` assets. The seeder reuses both.
2. **Auditor mode (optional).** If the client properties enable the auditor, seeded executions
   also leave asset locks. If the auditor is disabled, only the Ledger-side PREPARED records of
   workload F are left (the seeder warns, and rejects a C-only run since it would leave nothing).
3. **`scalar.db.consensus_commit.one_phase_commit.enabled` must be `false`** (the default) on the
   servers. Also note that ScalarDL forces coordinator writes even for read-only transactions,
   which is what makes workload C fail (and leave its read locks) as intended.

## Breaking the Coordinator

The seeder itself does not break anything; make the Coordinator unwritable before running it.

### Cosmos DB

Delete the stored procedure `mutate.js` from the coordinator `state` container. The ScalarDB
Cosmos adapter performs all writes through this stored procedure, so commit-state writes fail
with 404 while reads, and writes to all other containers (prepare phase, auditor lock writes),
keep working. The container and its data are untouched.

The whole cycle is doable with `az`. Confirm the subscription first — `az` keeps a persistent
default, which is exactly how a deletion lands on the wrong account:

```console
az account show --query "{name:name, id:id}" -o table
```

```console
az cosmosdb sql stored-procedure delete -g <RG> -a <ACCOUNT> \
  -d coordinator -c state -n mutate.js
```

To restore, put the stored procedure body in a local file and pass its path to `--body` (which
accepts `@<file>`). The procedure name must stay `mutate.js`, which is what the adapter looks up.
Take the body from either source:

- the ScalarDB jar the deployment runs, which is version-exact:
  `unzip -p scalardb-<VERSION>.jar cosmosdb_stored_procedure/mutate.js > mutate.js`
- [the file in the scalardb repository](https://github.com/scalar-labs/scalardb/blob/master/core/src/main/resources/cosmosdb_stored_procedure/mutate.js)
  (pick the matching tag if you want to be strict)

In practice the two agree: `mutate.js` has been touched by four commits in total and has not
changed since February 2022, and the copy in a 3.19 jar is byte-identical to the one on master.

```console
az cosmosdb sql stored-procedure create -g <RG> -a <ACCOUNT> \
  -d coordinator -c state -n mutate.js --body @mutate.js
```

```console
az cosmosdb sql stored-procedure list -g <RG> -a <ACCOUNT> -d coordinator -c state -o table
```

Alternatively, `DistributedTransactionAdmin.repairCoordinatorTables()` (e.g., via ScalarDB Schema
Loader's `--repair-all`) re-creates it too: repair recreates the container with `ifNotExists`,
which adds the stored procedure back when it is missing, using the `mutate.js` bundled in the
ScalarDB version running the repair.

The database name follows the configured coordinator namespace, so adjust `-d coordinator` if the
deployment overrides it.

### JDBC databases (MySQL / PostgreSQL) — for rehearsal

Drop the coordinator `state` table **while the servers are running** (so that the cached table
metadata keeps the failure mode identical), after saving its DDL:

```sql
DROP TABLE coordinator.state;
```

Restore it from the saved DDL afterwards. The failure path converges to the same 501 as Cosmos.

## Usage

```console
./build/install/scalardl-benchmarks/bin/recovery-seeder \
  --properties client.properties \
  --num-assets 10000 \
  --total-assets 1000000
```

| Option | Required | Default | Description |
|---|---|---|---|
| `--properties <file>` | yes | - | ScalarDL client properties (the same one used for the loader) |
| `--num-assets <M>` | yes | - | Number of assets to drive into a recovery-needed state **per workload** |
| `--total-assets <N>` | yes | - | Number of assets loaded by `YcsbLoader`; seeded keys are spread uniformly over `[0, N)` |
| `--workload <C\|F\|BOTH>` | no | `BOTH` | `BOTH` seeds M assets with F (even key slots) and another M with C (odd key slots) |
| `--ops-per-tx <K>` | no | `1` | Assets per contract execution; the K assets of one execution share one transaction ID. `M % K == 0` is required |
| `--concurrency <n>` | no | `1` | Client threads |
| `--skip-executions <n>` | no | `0` | Resume an interrupted run by skipping its first n executions; see [Resuming an interrupted run](#resuming-an-interrupted-run) |
| `--contract-id-c` / `--contract-id-f` | no | `C` / `F` | Override when the loader registered the contracts under non-default IDs |

Constraints: `M × (number of seeded workloads) ≤ N`. Keys are computed as
`key(j) = floor(j × N / S)` over `S = M × W` slots, so they are distinct and uniformly
distributed; with `BOTH`, both workloads individually cover the whole key space.

Exit codes: `0` = every execution failed as expected; `1` = fail-fast or interruption (the
summary shows `expected-failure / unexpected-success / unexpected-error / not-started` per
workload); `2` = option/validation error (nothing was executed).

## Resuming an interrupted run

An interrupted run leaves assets seeded that must not be seeded twice, so re-running from the
start is not an option. `--skip-executions` resumes instead: it drops the first n executions of the
planned sequence and leaves every remaining key exactly where it was, so the resumed run seeds only
what the interrupted one did not reach.

This is exact **only if the interrupted run used `--concurrency 1`**. Executions are submitted in
order to a single-threaded pool, so the ones it started are a contiguous prefix. With more threads
they are not, and there is no n that describes what was touched.

1. Stop the run with **Ctrl-C, never `kill -9`** — the summary is the only record of how far it got.
2. Read n off that summary: `planned` minus `not-started`, summed over the workloads. That is the
   number of executions it *started*, which includes the one that failed if it fail-fasted, since
   that one reached the target too. A workload the summary reports as `not run` contributes 0:
   with `BOTH` the workloads run F first and then C, so a run interrupted during F never started C.
3. Re-run with **every other option identical** and `--skip-executions n`. Changing `--num-assets`,
   `--total-assets`, `--ops-per-tx` or `--workload` shifts the whole key set and the resumed run
   would seed assets the first one already seeded.

For example, a run interrupted during workload F that reported
`10000 executions planned (K=1): expected-failure=3271, unexpected-success=0, unexpected-error=0,
not-started=6729` resumes with `--skip-executions 3271`, and the summary of the resumed run says so:

```
resumed run: the first 3271 executions of the plan were skipped, so everything below counts this
run only; add it to what the interrupted run seeded
```

The seeded totals are then the sum across the runs; the seeder cannot add them up for you.

Raising `--concurrency` on the resumed run is fine and is the usual reason to resume, but note that
it gives up the property above: if the resumed run is itself interrupted, it cannot be resumed
again.

## After seeding

1. **Restore the Coordinator** (see above). The cleanup tools themselves commit transactions, so
   they need a working Coordinator.
2. Wait 15+ seconds so the PREPARED records pass the recovery-expiration window.
3. Run the cleanup tools and verify against the counts printed in the seeder summary. Keep the key
   space idle while doing so — no other client, benchmark or
   [`random-executor`](RANDOM_EXECUTOR.md) run (see the first caveat).

## Caveats

- **Do not read the seeded assets** (contracts, validation, etc.) before running the cleanup
  tools: lazy recovery would resolve them and break the counts. Verify the counts on an otherwise
  idle key space, with no other client running — including
  [`random-executor`](RANDOM_EXECUTOR.md), which draws keys uniformly from the whole space and so
  eventually reads the seeded assets. Exercising the cleanup tools under concurrent traffic is a
  separate run: `random-executor` with no seeding at all, where there are no exact counts to lose.
- **Do not re-run the seeder after a partial failure** with the same parameters and no
  `--skip-executions`: keys are deterministic, so the run would hit already-seeded assets and
  corrupt the counts (lazy-recovery interference, shared read-lock counts, write-lock conflicts).
  Either resume with [`--skip-executions`](#resuming-an-interrupted-run), or resolve all seeded
  states with the cleanup tools first.
- A genuine coordinator outage during seeding is indistinguishable from the injected fault (both
  are 501). Rehearse the fault-injection procedure in a small environment first; the seeder
  fail-fasts on an unexpected *success*, which is the signature of forgetting to break the
  Coordinator.
- **Check which deployment the client properties point at before running.** If the Coordinator is
  in fact writable, workload F commits. The cleanup tools only resolve non-terminal (PREPARED /
  DELETED) records, so a committed asset version is out of their scope, and the ledger is
  append-only — undoing it means deleting the tip rows directly from `asset` (and
  `asset_metadata`, and the Auditor's copies) in the underlying database. Fail-fast detects this,
  but up to `--concurrency` executions may already have committed by then.
- If the run is interrupted (Ctrl-C) or fail-fasts, read the summary: it reports
  `expected-failure / unexpected-success / unexpected-error / not-started` per workload, and warns
  when the four do not add up to the planned count. Only `expected-failure × K` assets were
  actually seeded, and with `--concurrency > 1` the touched subset is not a contiguous prefix.
