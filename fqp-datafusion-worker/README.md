# FQP DataFusion worker

Gradle commands below run from the Calcite repository root. Use `.\gradlew.bat`
in Windows PowerShell or `./gradlew` on macOS/Linux.

## Java/Rust Flight integration

From the repository root, run `./gradlew :fqp:flightIntegrationTest`. The task
builds this crate, launches an isolated worker with no base tables on dynamic
HTTP/Flight ports, uploads Java Arrow batches, and executes Substrait scans.
Use `-PfqpWorkerBinary=<absolute path>` to test an existing executable.

The worker prints a `FQP_READY` JSON line with bound addresses. Flight paths
must contain exactly one ASCII identifier (letters/digits/underscores,
starting with a letter or underscore). Schema-only uploads and queries
returning no batches produce valid empty Arrow streams. Uploaded tables stay
registered until the worker exits.

The worker accepts binary Substrait plans over HTTP and returns either a
normalized JSON estimate (`/v1/cost`) or an Arrow IPC stream (`/v1/execute`).
When `flight_bind` is configured, it also accepts Arrow Flight `DoPut`
streams and registers descriptor paths as temporary DataFusion tables for
downstream distributed tasks.

## Live three-worker TPC-H Q3/Q5/Q7

Run `./gradlew :fqp:tpchIntegrationTest` (PowerShell:
`.\gradlew.bat :fqp:tpchIntegrationTest`) to generate Parquet data from Calcite's
TPC-H schema and execute Q3/Q5/Q7 across three workers per query with strict table ownership.
The test uses local placement costs and real Flight exchanges, compares results
with local Calcite execution, and stops its workers afterward. See the
[TPC-H guide](tpch/README.md) for configuration, scale, and retained artifacts.

## Legacy two-worker demo

The initial demo deliberately registers both CSV files at both workers. It
tests two-destination costing and selected-destination execution, but does not
use the separate placement-driven distributed execution path.

### Run

From this directory, build the worker:

```bash
cargo build --locked
```

Start the two workers in separate terminals, with this directory as the
working directory so the relative CSV paths resolve:

```bash
cargo run --locked -- demo/df1.toml
```

```bash
cargo run --locked -- demo/df2.toml
```

Check them:

```bash
curl http://127.0.0.1:8081/health
curl http://127.0.0.1:8082/health
```

From the Calcite repository root, run the Java driver:

```bash
./gradlew :fqp:runDataFusionDemo \
  --args="http://127.0.0.1:8081 http://127.0.0.1:8082"
```

The driver parses a simple SQL inner join with Calcite, serializes it as
Substrait for each worker, calls both cost endpoints, executes at the cheaper
worker, and prints the Arrow result. `df1.toml` has the lower demo cost factor,
so it should be selected and produce:

```text
[2, remote-two]
```
