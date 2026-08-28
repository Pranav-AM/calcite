# FQP Architecture

## Purpose and current scope

The FQP module is a prototype federated query-planning layer for Apache Calcite. It describes where tables live, lets Calcite consider executing a relational fragment at one of several destinations, obtains destination-specific costs, serializes the selected fragment to a portable plan, and converts the remote result back into Calcite's enumerable row model.

The checked-in demo exercises a deliberately small slice of that design: a two-table inner join is planned by Calcite, costed at two DataFusion workers, executed at the cheaper worker, and returned as Arrow rows. Both demo CSVs are registered at both workers, so it demonstrates destination selection but not physical data movement between workers.

## End-to-end flow

1. Calcite parses and validates SQL and creates a logical `RelNode` tree.
2. `FqpPlanningConfig` describes destinations, endpoints, table ownership/visibility, timeouts, and movement-cost policy.
3. The FQP convention and planner rules create remote alternatives for scans, filters, projects, and eligible inner joins.
4. `SubstraitFragmentSerializer` converts a candidate subtree into a binary Substrait `Plan`.
5. `DataFusionCostClient` posts that plan to each candidate worker's `/v1/cost` endpoint and receives a normalized JSON estimate.
6. Calcite can compare the remote cost plus estimated movement cost with other planner alternatives. The standalone demo performs the same comparison directly.
7. `DataFusionFragmentExecutor` posts the selected Substrait plan to `/v1/execute`.
8. The worker translates Substrait into a DataFusion logical plan, executes it, and returns an Apache Arrow IPC stream.
9. `ArrowRows` converts Arrow vectors into `Object[]` rows exposed through Calcite linq4j `Enumerable`.

There is also a coordinator path: `CoordinatorFragmentExecutor` and `HttpFqpCoordinatorClient` can send a fragment plus exchange metadata to an external coordinator. The coordinator service itself is not implemented in this repository.

## Main components

| Area | Responsibility |
| --- | --- |
| Core model | Immutable destinations, capabilities, placements, fragments, payloads, exchanges, execution requests/results, and remote-cost values. |
| Planning | `FqpConvention`, `FqpPlanningContext`, and rules introduce and cost FQP relational alternatives. |
| Serialization | Converts the supported Calcite relational/type/expression subset into binary Substrait protobuf. |
| Costing | Calls destination-local cost endpoints, normalizes costs, adds transfer/movement cost, caches results, and records metrics. |
| Execution | Sends a fragment to DataFusion directly or through a coordinator and adapts Arrow output to Calcite rows. |
| Demo | Builds a Calcite plan, probes two workers, selects the lower cost, executes, and prints the result. |
| Rust worker | The sibling `fqp-datafusion-worker` crate hosts HTTP health, costing, and execution endpoints backed by DataFusion. |

## Technologies and how they are used

- **Apache Calcite (Java):** SQL parsing/validation, relational algebra, planner conventions and conversion rules, cost integration, and the final enumerable execution boundary.
- **Substrait:** Portable binary representation sent from Calcite to a worker. The Java serializer uses generated protobuf types from `io.substrait:core`; the Rust worker uses `datafusion-substrait` to consume the plan.
- **Apache DataFusion (Rust):** Registers the configured CSV tables, converts Substrait plans, estimates plans, and executes the selected query.
- **Apache Arrow:** The worker returns query results as an Arrow IPC stream; Arrow Java decodes that stream into JDBC-style `Object[]` rows.
- **Arrow Flight:** Modeled as an optional destination `flightEndpoint` and as exchange requirements, but worker-to-worker Flight exchange is not implemented yet. Current costing and execution use ordinary HTTP.
- **HTTP:** Binary Substrait is posted with `application/vnd.substrait.plan`; cost responses are JSON and execution responses are `application/vnd.apache.arrow.stream`.
- **Jackson:** Parses the normalized JSON returned by `/v1/cost`.
- **Gradle/Kotlin DSL:** Builds and tests the Java `fqp` module and provides the `runDataFusionDemo` task.
- **Cargo/Tokio/Axum:** Builds the Rust worker, runs its async server, and exposes `/health`, `/v1/cost`, and `/v1/execute`.

## Supported portable subset

The serializer currently supports named table scans; projections containing input references; filters whose conditions are boolean literals; equality-based inner joins whose operands are input references; and `INTEGER`, `BIGINT`, `CHAR`, and `VARCHAR` fields. Other operators, expressions, join types, and data types fail explicitly instead of being silently translated.

## Important design boundaries

- A destination represents an execution location and its supported payload formats/endpoints; a table placement separately records ownership and the table name visible at each destination.
- Payloads are currently binary Substrait only, although the capability model is designed to negotiate formats.
- Remote costing failures are values (`RemoteCostResult.failure`) so an unavailable destination need not crash planning.
- Fragment execution failures are exceptions because they occur after a plan has been selected.
- The demo is not yet a true distributed join: replicated table registration avoids exchange, while the exchange/Flight abstractions reserve the future extension point.

