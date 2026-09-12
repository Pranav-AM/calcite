# FQP Build, Run, and Test Guide

Commands below assume the Calcite repository root as the starting directory. On Windows PowerShell use `.\gradlew.bat`; on macOS/Linux use `./gradlew`.

## Prerequisites

- A JDK supported by this Calcite checkout, with `JAVA_HOME` configured if required.
- Rust and Cargo capable of building the locked `fqp-datafusion-worker` dependency set.
- `curl` for the optional health checks.
- Ports `8081` and `8082` available on localhost for the legacy demo. Integration tests use OS-assigned ports.

The first Gradle and Cargo builds may download dependencies.

## 1. Build and test the Java module

From the repository root:

```powershell
.\gradlew.bat :fqp:build
```

Run only its tests when iterating:

```powershell
.\gradlew.bat :fqp:test
```

macOS/Linux equivalents:

```bash
./gradlew :fqp:build
./gradlew :fqp:test
```

The module configures the required `--add-opens=java.base/java.nio=ALL-UNNAMED` JVM argument for Arrow tests and the demo task.

### Live integration tests

Ordinary `:fqp:test` and `:fqp:build` do not run the Flight or TPC-H integration
suites. Run them explicitly from the repository root:

```powershell
.\gradlew.bat :fqp:flightIntegrationTest :fqp:tpchIntegrationTest
```

macOS/Linux:

```bash
./gradlew :fqp:flightIntegrationTest :fqp:tpchIntegrationTest
```

The Flight suite builds and launches a Rust worker and checks Java Arrow uploads
followed by Substrait scans. The TPC-H suite also builds the Arrow-to-Parquet
converter, generates scale-factor 0.01 data from Calcite, and compares Q3/Q5/Q7 across
three workers with local Calcite results. The FQP Q3 fixture uses `BUILDING`,
`o_orderdate < DATE '1995-03-15'`, and `l_shipdate > DATE '1995-03-15'` to match
the external plan. This differs from the original query in `plus`; only the
FQP fixture is changed. The result report includes the exact SQL used.
Both suites manage their own worker
processes; no manual startup is required.

Use `-PfqpWorkerBinary=<absolute path>` to supply a prebuilt worker. The TPC-H
suite still builds its converter with Cargo. Use `-PfqpTpchScale=0.02` to change
the TPC-H scale. Artifacts remain in `fqp/build/live-tpch-q3`, `live-tpch-q5`, and
`live-tpch-q7`, including the exact SQL for each query. Q5/Q7 use Calcite's
checked-in variants with their date predicates omitted.
See the [TPC-H guide](../fqp-datafusion-worker/tpch/README.md) for the query
variant, comparison tolerance, and retained artifacts.

The remaining steps describe building the worker manually and running the
legacy two-worker CSV demo.

## 2. Build and test the DataFusion worker

```powershell
Set-Location fqp-datafusion-worker
cargo build --locked
cargo test --locked
Set-Location ..
```

The `--locked` flag uses the checked-in `Cargo.lock`, keeping the worker dependency graph reproducible.

## 3. Start both workers

Open two terminals. Run each command from `fqp-datafusion-worker` so the relative CSV paths in the TOML files resolve.

Terminal 1:

```powershell
Set-Location fqp-datafusion-worker
cargo run --locked -- demo/df1.toml
```

Terminal 2:

```powershell
Set-Location fqp-datafusion-worker
cargo run --locked -- demo/df2.toml
```

The configurations register the same two CSVs on each worker. Worker `df1` listens on `127.0.0.1:8081` with cost factor `1.0`; `df2` listens on `127.0.0.1:8082` with cost factor `2.0`.

## 4. Check worker health

From another terminal:

```powershell
curl.exe http://127.0.0.1:8081/health
curl.exe http://127.0.0.1:8082/health
```

Both requests should succeed before running the Java driver.

## 5. Run the end-to-end demo

From the Calcite repository root:

```powershell
.\gradlew.bat :fqp:runDataFusionDemo --args="http://127.0.0.1:8081 http://127.0.0.1:8082"
```

macOS/Linux:

```bash
./gradlew :fqp:runDataFusionDemo \
  --args="http://127.0.0.1:8081 http://127.0.0.1:8082"
```

The driver prints the Calcite logical plan and both total costs. With the checked-in configuration it should select `df1` and end with:

```text
Selected destination: df1
[2, remote-two]
```

Exact numeric costs and surrounding Gradle logging may vary; the selected destination and result row are the useful assertions.

## 6. Stop the demo

Press `Ctrl+C` in each worker terminal. The demo does not create persistent database state; its inputs are the checked-in CSV files under `fqp-datafusion-worker/demo`.

## Troubleshooting

- **Connection refused:** confirm both workers are running and the URLs/ports passed to Gradle match their TOML `bind` values.
- **CSV path error:** start each worker with `fqp-datafusion-worker` as its working directory.
- **Port already in use:** stop the conflicting process or update both TOML `bind` values and the two URLs passed to the demo.
- **Arrow reflective-access error:** invoke the Gradle tasks above; `fqp/build.gradle.kts` supplies the required JVM `--add-opens` option.
- **Cargo dependency drift or incompatible toolchain:** retain `--locked` and update/install Rust rather than regenerating `Cargo.lock` as part of a normal demo run.
- **Unsupported Substrait operator/type:** the current serializer intentionally supports only the subset documented in `ARCHITECTURE.md`.
