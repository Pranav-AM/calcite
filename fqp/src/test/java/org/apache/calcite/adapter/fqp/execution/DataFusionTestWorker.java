/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpDestinationCapabilities;
import org.apache.calcite.adapter.fqp.FqpFragmentPayload;

import org.apache.calcite.sql.dialect.DuckDBSqlDialect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Starts a real worker on OS-assigned ports and retains logs for diagnosis. */
final class DataFusionTestWorker implements AutoCloseable {
  final FqpDestination destination;
  final int flightPort;
  final Path log;
  private final Process process;

  DataFusionTestWorker(Path directory) throws Exception {
    this(directory, "worker", Collections.emptyMap());
  }

  DataFusionTestWorker(Path directory, String source, Map<String, Path> tables) throws Exception {
    Files.createDirectories(directory);
    final Path config = directory.resolve("worker.toml");
    log = directory.resolve("worker.log");
    final ObjectMapper mapper = new ObjectMapper();
    final List<String> lines = new ArrayList<>();
    lines.add("source_id = " + mapper.writeValueAsString(source));
    lines.add("bind = \"127.0.0.1:0\"");
    lines.add("flight_bind = \"127.0.0.1:0\"");
    for (Map.Entry<String, Path> table : tables.entrySet()) {
      lines.add("[[tables]]");
      lines.add("name = " + mapper.writeValueAsString(table.getKey()));
      lines.add("path = "
          + mapper.writeValueAsString(table.getValue().toAbsolutePath().toString()));
      lines.add("format = \"parquet\"");
    }
    Files.write(config, lines, StandardCharsets.UTF_8);
    process = new ProcessBuilder(System.getProperty("fqp.worker.binary"), config.toString())
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (System.nanoTime() < deadline && process.isAlive()) {
        for (String line : Files.readAllLines(log, StandardCharsets.UTF_8)) {
          if (line.startsWith("FQP_READY ")) {
            final JsonNode ready = mapper.readTree(line.substring(10));
            final URI flight = URI.create("grpc://" + ready.path("flight").asText());
            flightPort = flight.getPort();
            destination = new FqpDestination(source, DuckDBSqlDialect.DEFAULT,
                FqpDestinationCapabilities.of(Collections.singleton(
                    FqpFragmentPayload.Format.SUBSTRAIT_BINARY), URI.create("http://"
                    + ready.path("http").asText() + "/v1/execute"), null, flight));
            return;
          }
        }
        Thread.sleep(50);
      }
      throw new AssertionError("worker did not start: "
          + String.join("\n", Files.readAllLines(log, StandardCharsets.UTF_8)));
    } catch (Throwable failure) {
      close();
      throw failure;
    }
  }

  @Override public void close() throws Exception {
    process.destroy();
    if (!process.waitFor(5, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      assertTrue(process.waitFor(5, TimeUnit.SECONDS), "worker process did not exit");
    }
  }
}
