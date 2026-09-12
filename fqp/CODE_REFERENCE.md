# FQP Code Reference

## Integrated file-based execution

- `FqpDeploymentConfigLoader.loadDeployment` loads and validates infrastructure,
  ownership, local statistics, and cost parameters from JSON.
- `FqpDeployment.planningContext` creates a context with the loaded cost model.
- `FqpOptimizer.optimizeForDistribution` applies logical rewrites and selects
  placements through `FqpPlacementOptimizer`'s isolated Volcano memo.
- `FqpPlacement` retains the selected logical tree, destinations, and total cost.
- `FqpDistributedPlanLowerer.lower(FqpPlacement)` preserves selected placements.
- `FqpQueryExecutor.fromDeployment` constructs the production HTTP/Flight path.
- `FqpQueryExecutor.prepare` optimizes and lowers without RPCs.
- `FqpQueryExecutor.execute` executes the task DAG and decodes root Arrow output
  as `Enumerable<Object[]>`.
- `FqpDeploymentException` reports configuration failures with their causes.
- `ArrowExchangeSchema` validates uploaded field types against the exchange schema.
- `FlightWorkerIntegrationTest` uploads Java Flight streams to a real Rust worker
  and executes downstream Substrait scans (`:fqp:flightIntegrationTest`).
- `TpchIntegrationTest` runs three Parquet-backed workers per query and compares Q3/Q5/Q7 with
  local Calcite execution (`:fqp:tpchIntegrationTest`).
- `TpchTestData` exports Calcite-generated rows and measures statistics; the Rust
  `arrow_to_parquet` example converts the exported Arrow stream to Parquet.
- `DataFusionTestWorker` manages worker processes for both integration suites.

This is a compact class-and-method map of production code in `fqp`; overloads and routine constructors/accessors are grouped where they have the same role.

## Core model and planning

- `FqpDestination` — Identifies a source by ID, SQL dialect, and advertised capabilities; `sourceId`, `dialect`, and `capabilities` expose those values, while `requireNonBlank` validates identifiers.
- `FqpDestinationCapabilities` — Describes payload formats and optional execution, cost, and Flight endpoints; `substraitOnly`/`of` construct it and `supports` checks format compatibility.
- `FqpTablePlacement` — Maps a Calcite logical table name to its owning source and destination-visible names; `visibleName` resolves the physical name for a destination.
- `FqpPlanningConfig` — Immutable registry of destinations and placements plus coordinator, movement, remote-root, and timeout policy; `of` builds it and lookup methods resolve entries.
- `FqpDeploymentConfigLoader.load` — Loads query-independent destinations, capabilities, endpoints, policies, and table placements from JSON.
- `FqpPlanningContext` — Bundles configuration, the local DataFusion cost model, fragment executor, and metrics for planner rules.
- `FqpPlanningMetrics` — Thread-safe counters; `recordCostRequest`, `recordCacheHit`, and `recordRemoteCall` update metrics and the remaining methods return snapshots.
- `FqpConvention` — Calcite calling convention for one destination; `register` installs that destination's FQP rules.
- `FqpOptimizer.optimize` — Registers locally costed remote-root alternatives and asks Calcite's Volcano planner to choose the lowest-cost executable Enumerable plan.
- `FqpFragmentPayload` / `Format` — Defensive immutable wrapper for serialized bytes; `binary` creates a payload and `format`/`bytes` read it.
- `FqpFragment` — Immutable executable unit containing destination, payload, row type, contributing source IDs, and required exchanges.
- `FqpExchangeRequirement` — Describes a future source-to-destination transfer and its estimated byte count.
- `FqpTaskId` / `FqpExchangeId` — Stable value identifiers for tasks and exchanges.
- `FqpTask` — Destination-specific fragment plus the base tables that must be local at that destination.
- `FqpExchange` — Typed producer-to-consumer edge with temporary input name and row/byte estimates.
- `FqpDistributedPlan` — Validates task destinations, strict table ownership, exchange endpoints and schemas, cycles, and root reachability; exposes topological task order.
- `FqpDistributedPlanLowerer.lower` — Partitions a supported Calcite tree at selected execution-destination boundaries and serializes maximal destination-local tasks with temporary Substrait inputs. Base scans remain at their owners; downstream operators can execute at another worker, including a third worker receiving both join inputs.
- `FqpRuntime.execute` — Runtime bridge that delegates a fragment to an `FqpFragmentExecutor`.
- `FqpExecutionException` — Signals a selected fragment could not be executed.

## Costing

- `DataFusionStatisticsSnapshot` — Versioned immutable table-statistics snapshot loaded for one worker before optimization.
- `DataFusionTableStatistics` / `DataFusionColumnStatistics` — Row counts, row widths, and column statistics used by local estimation.
- `DataFusionCostParameters` — Calibratable scan, expression, join, aggregate, sort, and I/O coefficients.
- `DataFusionCostModel.estimate` — Recursively estimates a relational subtree locally, including cross-owner movement, without worker communication.
- `DataFusionCostEstimate` — Holds locally estimated rows, row width, CPU, I/O, network, and total cost.
- `FqpCostModel.toRelOptCost` — Converts local or legacy remote estimates into Calcite cost dimensions.

### Legacy remote costing and calibration

- `RemoteCostRequest` — Carries caller/destination IDs, serialized payload, and timeout to a cost service.
- `RemoteCostEstimate` — Holds startup cost, total cost, row count, and row width; `cpu`, `transferCost`, and `movementCost` derive Calcite-facing cost components.
- `RemoteCostResult` — Success-or-failure wrapper; factory methods create results and optional accessors expose the estimate or message.
- `RemoteCostClient.explain` — Contract for obtaining a normalized estimate for a serialized fragment.
- `DataFusionCostClient.explain` — Validates destination capabilities, posts Substrait to `/v1/cost`, and parses its JSON estimate.
- `CachingRemoteCostClient.explain` — Caches cost results by request identity and records request, hit, call, and timing metrics; `size` and `metrics` expose cache state.

## Relational nodes

- `FqpRel` — Marker interface for relational nodes using an FQP convention.
- `FqpTableScan` — Destination-local scan; `copy` preserves it under new traits and `computeSelfCost` uses its remote estimate.
- `FqpJoin` — Destination-local join; `copy` rebuilds the join and `computeSelfCost` uses its remote estimate.
- `FqpRemoteFragmentRel` — Leaf representing a serialized remote subtree; `fragment`/`estimate` expose metadata, `explainTerms` prints it, and cost/copy methods support planning.
- `FqpToEnumerableConverter` — Calcite execution boundary; `computeSelfCost` discounts the lightweight adapter boundary, `implement` generates a call to `FqpRuntime.execute`, and `copy` recreates the converter.

## Planner rules

- `FqpRules.rules` — Returns scan, filter, project, aggregate, sort, join, and enumerable-conversion rules for a destination.
- `FqpScanRule.create` / `convert` — Creates the rule and turns an eligible placed table scan into a locally costed remote fragment.
- `FqpPushdownRule.create` / `convert` — Creates a whole-subtree rule, locally estimates it, and serializes a supported filter, project, aggregate, or sort root as one remote fragment.
- `FqpJoinRule.create` / `convert` — Creates the join rule and forms a locally costed remote inner join when both inputs can run at the destination.
- `FqpToEnumerableConverterRule.create` / `convert` — Adds the adapter that brings an FQP plan back to Calcite enumerable convention.

## Serialization

- `FqpFragmentSerializer.serialize` — Format-neutral facade that currently delegates to the Substrait serializer.
- `SubstraitFragmentSerializer.serialize` — Resolves a destination, validates capabilities, translates a subtree, declares scalar and aggregate function extensions, and returns an `FqpFragment`.
- `relation` — Recursively translates supported scans, projects, filters, inner joins, grouped SUM aggregates, sorts, and fetches to Substrait relations.
- `read` — Resolves table placement/name and produces a Substrait named-table read with schema.
- `expression` / `fieldReference` — Build field selections, literals, casts, equality, strict comparisons, conjunction/disjunction, year extraction from dates, and arithmetic scalar expressions.
- `struct` / `type` — Convert Calcite row and scalar types including boolean, numeric, decimal, date, char, and varchar.
- `unsupportedExpression` — Creates a context-rich translation error.
- `FqpSerializationException` / `SubstraitSerializationException` — Report unsupported generic or Substrait-specific serialization cases.

## Execution and coordination

- `FqpFragmentExecutor.supportedPayloadFormats` / `execute` — Contract describing accepted formats and executing a selected fragment.
- `DataFusionFragmentExecutor.execute` — Validates a no-exchange Substrait fragment, posts it to `/v1/execute`, decodes Arrow, and returns an `Enumerable`.
- `FqpDistributedPlanExecutor.execute` — Executes tasks in topological order, stages every producer output for its consumers, and returns the root Arrow IPC stream.
- `DataFusionTaskClient.execute` — Posts one distributed task's Substrait payload to its DataFusion execution endpoint and preserves the Arrow IPC response for downstream exchange.
- `ArrowFlightExchangeTransport.transfer` — Sends a producer Arrow IPC stream to the consumer's configured Flight endpoint using `DoPut` and the exchange temporary-input name as its descriptor path.
- `ArrowRows.read` — Reads Arrow IPC stream bytes and materializes supported vector values as `Object[]` rows.
- `FqpCoordinatorClient.execute` — Contract for coordinator-managed execution.
- `FqpExecutionRequest` / `FqpExecutionResult` — Coordinator request and defensively copied materialized row response.
- `CoordinatorFragmentExecutor.execute` — Wraps a fragment in a coordinator request and exposes the returned rows as an enumerable.
- `HttpFqpCoordinatorClient.execute` — Sends Substrait and exchange headers to a coordinator and reads the Arrow response.

## Demo

- `FqpDataFusionDemo.main` — Builds the two-worker configuration and logical join, costs both destinations, executes the cheaper fragment, and prints rows.
- `logicalPlan` — Parses and validates the fixed demo SQL into a Calcite relational tree.
- `destination` — Builds worker capabilities from a base URI.
- `placements` / `placement` — Define logical ownership and worker-visible demo table names.
- `DemoTable.getRowType` — Supplies the demo schema (`id BIGINT`, `value VARCHAR`) to Calcite.
