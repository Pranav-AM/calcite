# Minimal two-worker FQP demo

The worker accepts binary Substrait plans over HTTP and returns either a
normalized JSON estimate (`/v1/cost`) or an Arrow IPC stream (`/v1/execute`).

The initial demo deliberately registers both CSV files at both workers. It
tests two-destination costing and selected-destination execution, but does not
yet implement a physical worker-to-worker exchange.

## Run

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
