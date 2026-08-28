# FQP Code Reference

This is a compact class-and-method map of production code in `fqp`; overloads and routine constructors/accessors are grouped where they have the same role.

## Core model and planning

- `FqpDestination` — Identifies a source by ID, SQL dialect, and advertised capabilities; `sourceId`, `dialect`, and `capabilities` expose those values, while `requireNonBlank` validates identifiers.
- `FqpDestinationCapabilities` — Describes payload formats and optional execution, cost, and Flight endpoints; `substraitOnly`/`of` construct it and `supports` checks format compatibility.
- `FqpTablePlacement` — Maps a Calcite logical table name to its owning source and destination-visible names; `visibleName` resolves the physical name for a destination.
- `FqpPlanningConfig` — Immutable registry of destinations and placements plus coordinator, movement, remote-root, and timeout policy; `of` builds it and lookup methods resolve entries.
- `FqpPlanningContext` — Bundles configuration, remote cost client, fragment executor, and metrics for planner rules.
- `FqpPlanningMetrics` — Thread-safe counters; `recordCostRequest`, `recordCacheHit`, and `recordRemoteCall` update metrics and the remaining methods return snapshots.
- `FqpConvention` — Calcite calling convention for one destination; `register` installs that destination's FQP rules.
- `FqpFragmentPayload` / `Format` — Defensive immutable wrapper for serialized bytes; `binary` creates a payload and `format`/`bytes` read it.
- `FqpFragment` — Immutable executable unit containing destination, payload, row type, contributing source IDs, and required exchanges.
- `FqpExchangeRequirement` — Describes a future source-to-destination transfer and its estimated byte count.
- `FqpRuntime.execute` — Runtime bridge that delegates a fragment to an `FqpFragmentExecutor`.
- `FqpExecutionException` — Signals a selected fragment could not be executed.

## Remote costing

- `RemoteCostRequest` — Carries caller/destination IDs, serialized payload, and timeout to a cost service.
- `RemoteCostEstimate` — Holds startup cost, total cost, row count, and row width; `cpu`, `transferCost`, and `movementCost` derive Calcite-facing cost components.
- `RemoteCostResult` — Success-or-failure wrapper; factory methods create results and optional accessors expose the estimate or message.
- `RemoteCostClient.explain` — Contract for obtaining a normalized estimate for a serialized fragment.
- `DataFusionCostClient.explain` — Validates destination capabilities, posts Substrait to `/v1/cost`, and parses its JSON estimate.
- `CachingRemoteCostClient.explain` — Caches cost results by request identity and records request, hit, call, and timing metrics; `size` and `metrics` expose cache state.
- `FqpCostModel.toRelOptCost` — Converts a remote estimate and movement factor into a Calcite `RelOptCost`.

## Relational nodes

- `FqpRel` — Marker interface for relational nodes using an FQP convention.
- `FqpTableScan` — Destination-local scan; `copy` preserves it under new traits and `computeSelfCost` uses its remote estimate.
- `FqpJoin` — Destination-local join; `copy` rebuilds the join and `computeSelfCost` uses its remote estimate.
- `FqpRemoteFragmentRel` — Leaf representing a serialized remote subtree; `fragment`/`estimate` expose metadata, `explainTerms` prints it, and cost/copy methods support planning.
- `FqpToEnumerableConverter` — Calcite execution boundary; `implement` generates a call to `FqpRuntime.execute` and `copy` recreates the converter.

## Planner rules

- `FqpRules.rules` — Returns the scan, filter/project pushdown, join, and enumerable-conversion rules for a destination.
- `FqpScanRule.create` / `convert` — Creates the rule and turns an eligible placed table scan into a costed remote fragment.
- `FqpPushdownRule.create` / `convert` — Creates a filter/project rule and serializes a supported operator over an FQP input as one remote fragment.
- `FqpJoinRule.create` / `convert` — Creates the join rule and forms a costed remote inner join when both inputs can run at the destination.
- `FqpToEnumerableConverterRule.create` / `convert` — Adds the adapter that brings an FQP plan back to Calcite enumerable convention.

## Serialization

- `FqpFragmentSerializer.serialize` — Format-neutral facade that currently delegates to the Substrait serializer.
- `SubstraitFragmentSerializer.serialize` — Resolves a destination, validates capabilities, translates a subtree, declares join extensions, and returns an `FqpFragment`.
- `relation` — Recursively translates supported scans, projects, literal filters, and inner joins to Substrait relations.
- `read` — Resolves table placement/name and produces a Substrait named-table read with schema.
- `fieldReference` / `joinCondition` — Build Substrait field selections and equality join expressions.
- `containsJoin` — Detects whether the plan needs the Substrait comparison-function extension declaration.
- `struct` / `type` — Convert Calcite row and scalar types to the supported Substrait type subset.
- `unsupportedExpression` — Creates a context-rich translation error.
- `FqpSerializationException` / `SubstraitSerializationException` — Report unsupported generic or Substrait-specific serialization cases.

## Execution and coordination

- `FqpFragmentExecutor.supportedPayloadFormats` / `execute` — Contract describing accepted formats and executing a selected fragment.
- `DataFusionFragmentExecutor.execute` — Validates a no-exchange Substrait fragment, posts it to `/v1/execute`, decodes Arrow, and returns an `Enumerable`.
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

