# TPC-H worker placement and live validation

The checked-in manual Q3 worker configurations enforce one physical owner per
TPC-H table. `deployment.json` supplies the corresponding query-independent
Calcite/FQP destination and placement metadata:

- `df-customer` registers only `customer`;
- `df-orders` registers only `orders`;
- `df-lineitem` registers only `lineitem`.

For a manual deployment, place Parquet files under `tpch/data` and start each
worker from the `fqp-datafusion-worker` directory. The automated test below
generates its own data and worker configurations under the Java build directory.
Cross-owner inputs use explicit exchanges; the workers do not replicate tables.

## Live three-worker Q3/Q5/Q7

From the repository root, in Windows PowerShell:

```powershell
.\gradlew.bat :fqp:tpchIntegrationTest
```

On macOS/Linux:

```bash
./gradlew :fqp:tpchIntegrationTest
```

The test generates each query's tables from Calcite's existing
`TpchSchema` at scale factor 0.01. It exports those rows through Arrow and the
Rust `arrow_to_parquet` example, measures statistics, and starts three workers
on dynamically assigned HTTP/Flight ports. Each table has one physical owner.
For Q5/Q7, supplier shares the lineitem
worker and nation shares the customer worker; Q5 region shares the orders worker.
Q7 scans the same owned nation table through two aliases.
It loads the generated JSON deployment, prepares each query without worker RPCs,
executes that exact plan, and compares its ordered result with Calcite JDBC
execution of the same SQL and generated data.

Q3 uses FQP's `BUILDING` fixture with orders before and shipments after
March 15, 1995. Q5 uses Calcite's `EUROPE` variant; Q7 uses its
`EGYPT`/`UNITED STATES` variant. Q5/Q7 retain the checked-in SQL's commented-out
date filters. The exact SQL is included in each result report. This does not
validate variants with inclusive date ranges or interval arithmetic.

Keys, dates, years, and ordering are compared exactly; revenue allows
floating-point rounding within `max(1e-8, abs(expected) * 1e-10)`.
Each query checks strict ownership, completed task/exchange counts, and zero
optimizer-time RPCs. Routes and task counts are selected by costs and recorded
in the result file.

Artifacts remain under `fqp/build/live-tpch-q3/`, `live-tpch-q5/`, and
`live-tpch-q7/`: Parquet and Arrow data,
measured deployment statistics, worker configurations/logs, task Substrait
payloads, and `result.json` with both result sets and execution events. The
workers exit after the test, so the recorded dynamic endpoints are diagnostic.
Change the scale with `-PfqpTpchScale=0.02`. Use the generated worker
configurations to restart the workers manually with the retained Parquet files.

## File-based discovery and execution

Load `deployment.json` using `FqpDeploymentConfigLoader.loadDeployment(path)`,
or construct `FqpQueryExecutor.fromDeployment(path)` and call
`execute(logicalRoot)` to obtain `Enumerable<Object[]>`. The logical root must
come from Calcite's schema and SQL planning. `prepare(logicalRoot)` optimizes
and lowers the query without contacting workers.

The `statistics` object is keyed by destination ID. Each snapshot contains a
`version`, an ISO-8601 `loadedAt` timestamp, and `tables`. Each table has its
Calcite `logicalName`, `rowCount`, and `averageRowWidth` in bytes. An optional
`columns` map supplies `distinctCount` and `nullCount` per column. These are
statistics, not SQL schema definitions.

The `costParameters` object is also keyed by destination ID. It supports
`scanCpuPerRow`, `expressionCpuPerRow`, `joinCpuPerRow`, `aggregateCpuPerRow`,
`sortCpuPerComparison`, and `ioPerByte`. Omitted coefficients use the local
cost model's defaults. Counts and coefficients must be finite and non-negative.

The included statistics are illustrative estimates for scale factor 0.01,
not measurements of supplied Parquet files. Replace them with values for the
data used in the test. Runtime worker discovery is not required.

The integrated path applies logical rewrites, then uses Calcite's Volcano
planner to select execution locations from local execution and Arrow movement
costs. Exchange routes can change with statistics and coefficients; Q3 need
not follow the original customer-to-orders-to-lineitem chain. Exchange names
are unique per prepared plan; workers retain uploaded tables until restart.

Run `.\gradlew.bat :fqp:flightIntegrationTest` in Windows PowerShell or
`./gradlew :fqp:flightIntegrationTest` on macOS/Linux from the repository root to verify
Java-to-Rust uploads and Substrait scans using a temporary worker, without
TPC-H files. The separate `tpchIntegrationTest` generates and checks the Q3/Q5/Q7 paths.
