# FQP Architecture

## Purpose and current scope

The FQP module is a prototype federated query-planning layer for Apache Calcite. It describes where tables live, lets Calcite consider executing a relational fragment at one of several destinations, obtains destination-specific costs, serializes the selected fragment to a portable plan, and converts the remote result back into Calcite's enumerable row model.

The integrated path runs FQP's Q3 with BUILDING and the March 15, 1995 date predicates across three DataFusion workers with strict table ownership, local cost-based placement, and Arrow Flight exchanges. The live test generates Parquet data from Calcite's TPC-H schema and compares the distributed output with local Calcite execution. The same harness also validates Calcite's checked-in Q5/Q7 variants (with their date predicates omitted).

The legacy two-worker demo also remains available: a two-table inner join is costed at both workers, executed at the cheaper worker, and returned as Arrow rows. Both demo CSVs are registered at both workers.

## End-to-end flow

1. Calcite parses and validates SQL and creates a logical `RelNode` tree.
2. `FqpPlanningConfig` describes destinations, endpoints, table ownership/visibility, timeouts, and movement-cost policy.
3. `FqpQueryExecutor.prepare` calls `FqpOptimizer.optimizeForDistribution`: Hep applies logical rewrites, then `FqpPlacementOptimizer` registers operators and transfers in an isolated Volcano memo. Each destination is a convention. Calcite selects placements using local operator costs and estimated Arrow movement costs. `FqpPlacement` retains the logical tree and selected destinations; `FqpDistributedPlanLowerer` preserves them when building tasks and exchanges.
4. `DataFusionCostModel` estimates those alternatives locally from immutable statistics snapshots loaded before optimization. Planner rules perform no worker calls.
5. `SubstraitFragmentSerializer` converts a candidate subtree into a binary Substrait `Plan`.
6. Base scans stay at their owners. Join alternatives include moving either input to the other owner or both to another worker with HTTP execution, binary Substrait, and Flight capabilities. Unary operators can also move. The standalone legacy demo still probes `/v1/cost` directly and is not representative of either planner path.
7. `FqpDistributedPlanExecutor` runs tasks in dependency order through `DataFusionTaskClient` and stages intermediate results through `ArrowFlightExchangeTransport` before running consumers.
8. The worker translates Substrait into a DataFusion logical plan, executes it, and returns an Apache Arrow IPC stream.
9. `ArrowRows` converts the root stream into materialized `Object[]` rows exposed through Calcite linq4j `Enumerable`, including Java strings and Calcite's integer date representation.

## File-configured execution

```java
FqpQueryExecutor executor = FqpQueryExecutor.fromDeployment(deploymentPath);
Enumerable<Object[]> rows = executor.execute(logicalRoot);
```

`logicalRoot` comes from Calcite parsing and validation. `prepare(logicalRoot)`
also exposes the task DAG for inspection without RPCs. The executor accepts
injected task and exchange transports for embedding and tests. Its default
transports use the deployment's HTTP and Flight endpoints. Result rows and
intermediate streams are currently materialized in memory.

`FqpDeploymentConfigLoader.loadDeployment` returns an immutable `FqpDeployment`
containing planning configuration, per-worker statistics snapshots, and cost
coefficients. Legacy `load` overloads still return just `FqpPlanningConfig`.
Malformed JSON, unknown fields, invalid values, inconsistent ownership, and
file errors use `FqpDeploymentException`, preserving the cause and including
the path for file loads. Discovery is file-based; there are no startup worker
discovery calls. See `fqp-datafusion-worker/tpch/deployment.json` for the format.

Statistics and coefficient maps are optional for configuration-only use, but
execution planning requires statistics for every referenced table. SQL schemas
are supplied by Calcite, never duplicated in the deployment document.

There is also a coordinator path: `CoordinatorFragmentExecutor` and `HttpFqpCoordinatorClient` can send a fragment plus exchange metadata to an external coordinator. The coordinator service itself is not implemented in this repository.

## Main components

The placement memo shares subplans by expression and destination instead of
enumerating complete combinations. `DataFusionCostModel.estimateOperator`
charges only the current operator's CPU and I/O. Transfers charge intermediate
rows times row width times `movementCostFactor`; root response movement back
to Calcite is also included. `FqpPlacement.cost()` exposes the total estimate.
Estimates remain heuristic, although selection is cost-based. The search
chooses placements for the rewritten join tree; it does not enumerate all join
orders. Compound join conditions and OR predicates still use heuristic fallback
selectivities; these tests establish result correctness, not calibrated placement
costs or optimal routes. `allowRemoteRoot=false` requires a capable configured coordinator
destination. Impossible placements fail locally, without RPCs.

From the repository root, use `.\gradlew.bat` in Windows PowerShell or
`./gradlew` on macOS/Linux for the commands below.

Run `./gradlew :fqp:flightIntegrationTest` to build and launch a Rust worker on
OS-assigned ports, upload Java Arrow batches, and scan them through Substrait.
This task is separate from ordinary Java tests; use
`-PfqpWorkerBinary=<absolute path>` for a prebuilt worker. Coverage includes
multiple batches, UTF-8, decimals, dates, booleans, floating-point values, nulls,
empty results, invalid descriptors, missing endpoints, schema mismatches, and
failed transfers. Tests always reap their worker processes.

The sender validates exchange types before uploading and applies a Flight
deadline (30 seconds by default). The receiver accepts one identifier in the
descriptor path. Workers retain temporary tables until restart; automatic
temporary-table cleanup remains separate work.

`./gradlew :fqp:tpchIntegrationTest` generates scale-factor 0.01 Parquet data
from Calcite's TPC-H generator, measures startup statistics, and runs FQP's
Q3 plus Calcite's checked-in Q5/Q7 variants on three workers per query. It
checks ordered output against local Calcite JDBC execution, strict ownership,
completed task/exchange counts, and zero optimizer RPCs. The live test exercises DataFusion's
`Utf8View` strings as well as joins, aggregation, sorting, and fetch. The exact
prepared plan is executed through `FqpQueryExecutor.execute(FqpDistributedPlan)`.
Data, logs, plans, and comparisons are retained in `fqp/build/live-tpch-q3`,
`live-tpch-q5`, and `live-tpch-q7`.
See the [TPC-H guide](../fqp-datafusion-worker/tpch/README.md) for commands and comparison tolerance.

| Area | Responsibility |
| --- | --- |
| Core model | Immutable destinations, capabilities, placements, fragments, tasks, typed exchanges, validated distributed plans, execution requests/results, and cost values. |
| Planning | `FqpConvention`, `FqpPlanningContext`, and rules introduce and cost FQP relational alternatives. |
| Serialization | Converts the supported Calcite relational/type/expression subset into binary Substrait protobuf. |
| Costing | Estimates DataFusion work locally from startup statistics and calibrated coefficients, including transfer/movement cost. Remote costing remains available only for the legacy demo and offline calibration. |
| Execution | Sends a fragment to DataFusion directly or through a coordinator and adapts Arrow output to Calcite rows. |
| Demo | Builds a Calcite plan, probes two workers, selects the lower cost, executes, and prints the result. |
| Rust worker | The sibling `fqp-datafusion-worker` crate hosts HTTP health, costing, and execution endpoints backed by DataFusion. |

## Technologies and how they are used

- **Apache Calcite (Java):** SQL parsing/validation, relational algebra, planner conventions and conversion rules, cost integration, and the final enumerable execution boundary.
- **Substrait:** Portable binary representation sent from Calcite to a worker. The Java serializer uses generated protobuf types from `io.substrait:core`; the Rust worker uses `datafusion-substrait` to consume the plan.
- **Apache DataFusion (Rust):** Registers the configured CSV and Parquet tables, converts Substrait plans, estimates plans, and executes the selected query.
- **Apache Arrow:** The worker returns query results as an Arrow IPC stream; Arrow Java decodes that stream into JDBC-style `Object[]` rows.
- **Arrow Flight:** `FqpDistributedPlanExecutor` walks the validated task DAG and `ArrowFlightExchangeTransport` uploads each producer's Arrow IPC stream to the consumer's temporary-input Flight descriptor. Each configured Rust worker accepts `DoPut`, decodes the stream, and registers it as a DataFusion in-memory table for the downstream Substrait task.
- **HTTP:** Binary Substrait is posted for execution. The legacy demo also consumes JSON from `/v1/cost`; normal planner rules do not.
- **Jackson:** Loads deployment files and parses legacy `/v1/cost` responses.
- **Gradle/Kotlin DSL:** Builds and tests the Java `fqp` module and provides the `runDataFusionDemo` task.
- **Cargo/Tokio/Axum:** Builds the Rust worker, runs its async server, and exposes `/health`, `/v1/cost`, and `/v1/execute`.

## Supported portable subset

The serializer currently supports named table scans, projections, filters, inner joins, grouped `SUM` aggregates, sorting, and fetch/limit. Expressions include input references, literals, casts, equality, strict less-than and greater-than comparisons, conjunction, disjunction, year extraction from dates, multiplication, and subtraction. Supported scalar types include booleans, integer and floating-point numbers, decimal, date, char, and varchar. This covers the tested Q3/Q5/Q7 operator trees without embedding query SQL or column names in production serialization code. Year extraction uses the DataFusion `date_part` extension (`urn:datafusion:functions:datetime`) and an explicit cast to Calcite's result type; it is not a claim of support for arbitrary datetime functions or all Substrait consumers. Other operators, expressions, join types, and data types fail explicitly instead of being silently translated.

## Important design boundaries

- A destination represents an execution location and its supported payload formats/endpoints; a table placement separately records ownership and the table name visible at each destination.
- `FqpDeploymentConfigLoader` loads destinations, capabilities, endpoints, policies, and table placements from query-independent JSON. Calcite plans remain the source of query structure and schemas.
- `FqpDistributedPlan` validates destination-local table claims and exchange producer/consumer schemas, rejects cycles and orphan tasks, and exposes producer-before-consumer task order.
- Payloads are currently binary Substrait only, although the capability model is designed to negotiate formats.
- Legacy remote-costing failures are values (`RemoteCostResult.failure`); local planner estimation instead requires a valid startup statistics snapshot.
- Fragment execution failures are exceptions because they occur after a plan has been selected.
- The generic lowering and execution path keeps base scans at the owners defined by `FqpTablePlacement` and derives task boundaries from selected execution destinations. It transfers intermediate Arrow streams and resolves temporary inputs without query-specific lowering rules.
