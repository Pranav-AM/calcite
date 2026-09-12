/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpDeploymentConfigLoader;
import org.apache.calcite.adapter.fqp.FqpDistributedPlan;
import org.apache.calcite.adapter.fqp.FqpPlanningContext;
import org.apache.calcite.adapter.fqp.FqpTask;
import org.apache.calcite.adapter.fqp.cost.TpchQueryFixture;
import org.apache.calcite.adapter.tpch.TpchSchema;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelNode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Compares distributed TPC-H queries with local Calcite on the same generated data. */
@Tag("tpchIntegration")
class TpchIntegrationTest {
  @ParameterizedTest(name = "Q{0}")
  @ValueSource(ints = {3, 5, 7})
  void distributedQueryMatchesLocalCalcite(int query) throws Exception {
    final double scale = Double.parseDouble(System.getProperty("fqp.tpch.scale", "0.01"));
    assertTrue(Double.isFinite(scale) && scale > 0, "scale must be finite and positive");
    final Path directory = Paths.get(System.getProperty("fqp.tpch.output"))
        .toAbsolutePath().resolveSibling("live-tpch-q" + query);
    final Path data = directory.resolve("data");
    Files.createDirectories(data);
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode document;
    try (InputStream input = getClass().getResourceAsStream(
        "/org/apache/calcite/adapter/fqp/tpch-three-owner.json")) {
      document = (ObjectNode) mapper.readTree(input);
    }
    if (query != 3) {
      addPlacement(document, "supplier", "df-lineitem");
      addPlacement(document, "nation", "df-customer");
      if (query == 5) {
        addPlacement(document, "region", "df-orders");
      }
    }
    final Map<String, Map<String, Path>> tables = new LinkedHashMap<>();
    try (CalciteConnection connection = DriverManager.getConnection("jdbc:calcite:")
        .unwrap(CalciteConnection.class)) {
      connection.getRootSchema().add("TPCH", new TpchSchema(scale, 1, 1, false));
      final ObjectNode statistics = document.putObject("statistics");
      for (JsonNode placement : document.path("placements")) {
        final String table = placement.path("logicalName").get(1).asText();
        final String source = placement.path("sourceId").asText();
        ObjectNode snapshot = (ObjectNode) statistics.get(source);
        if (snapshot == null) {
          snapshot = statistics.putObject(source);
          snapshot.put("version", "generated-sf-" + scale);
          snapshot.put("loadedAt", Instant.now().toString());
          snapshot.putArray("tables");
        }
        ((ArrayNode) snapshot.get("tables")).add(TpchTestData.export(connection, table, data));
        final String name = table.toLowerCase(Locale.ROOT);
        tables.computeIfAbsent(source, key -> new LinkedHashMap<>())
            .put(name, data.resolve(name + ".parquet"));
      }
      final List<Object[]> expected = reference(connection, query);
      assertFalse(expected.isEmpty(), "the generated data must produce a nonempty result");
      try (DataFusionTestWorker customer = worker(directory, tables, "df-customer");
           DataFusionTestWorker orders = worker(directory, tables, "df-orders");
           DataFusionTestWorker lineitem = worker(directory, tables, "df-lineitem");
           ArrowFlightExchangeTransport flight = new ArrowFlightExchangeTransport()) {
        final List<DataFusionTestWorker> workers = Arrays.asList(customer, orders, lineitem);
        for (JsonNode entry : document.path("destinations")) {
          final ObjectNode destination = (ObjectNode) entry;
          for (DataFusionTestWorker worker : workers) {
            if (worker.destination.sourceId().equals(destination.path("sourceId").asText())) {
              destination.put("executionEndpoint", worker.destination.capabilities()
                  .executionEndpoint().get().toString());
              destination.put("flightEndpoint", worker.destination.capabilities()
                  .flightEndpoint().get().toString());
            }
          }
        }
        final Path deployment = directory.resolve("deployment.json");
        mapper.writerWithDefaultPrettyPrinter().writeValue(deployment.toFile(), document);
        final FqpPlanningContext context = FqpDeploymentConfigLoader.loadDeployment(deployment)
            .planningContext();
        final DataFusionTaskClient tasks = new DataFusionTaskClient(context.config());
        final ArrayNode events = mapper.createArrayNode();
        final FqpQueryExecutor executor = new FqpQueryExecutor(context, task -> {
          final byte[] output = tasks.execute(task);
          events.addObject().put("type", "execute").put("source", task.destination().sourceId())
              .put("bytes", output.length);
          return output;
        }, (exchange, destination, stream) -> {
          flight.transfer(exchange, destination, stream);
          events.addObject().put("type", "flight").put("source", exchange.sourceId())
              .put("destination", destination.sourceId()).put("bytes", stream.length)
              .put("temporaryInput", String.join(".", exchange.temporaryInputName()));
        });
        final RelNode logical = TpchQueryFixture.logicalPlan(query, scale);
        final FqpDistributedPlan plan = executor.prepare(logical);
        assertEquals(0, events.size(), "prepare must not execute or transfer data");
        assertEquals(0L, context.metrics().remoteCalls());
        assertNoRequests(workers, true);
        assertTrue(plan.tasks().size() >= 3);
        assertTrue(plan.exchanges().size() >= 2);
        for (FqpTask task : plan.tasks()) {
          Files.write(directory.resolve(task.id() + ".substrait"),
              task.fragment().payload().bytes());
          for (List<String> table : task.localTables()) {
            assertEquals(context.config().placement(table).get().sourceId(),
                task.destination().sourceId());
          }
        }
        final List<Object[]> actual = executor.execute(plan).toList();
        final double maximumDifference = compare(expected, actual, query == 7 ? 3 : 1);
        assertEquals(plan.tasks().size() + plan.exchanges().size(), events.size());
        assertEquals((long) plan.exchanges().size(),
            java.util.stream.StreamSupport.stream(events.spliterator(), false)
            .filter(event -> event.path("type").asText().equals("flight")).count());
        assertNoRequests(workers, false);
        assertEquals(0L, context.metrics().remoteCalls());
        final ObjectNode report = mapper.createObjectNode();
        report.put("query", "TPC-H Q" + query);
        report.put("sql", TpchQueryFixture.sql(query));
        report.put("scaleFactor", scale);
        report.put("tasks", plan.tasks().size());
        report.put("exchanges", plan.exchanges().size());
        report.put("optimizerRemoteCalls", context.metrics().remoteCalls());
        report.put("maximumRevenueDifference", maximumDifference);
        report.set("statistics", statistics);
        report.set("events", events);
        report.set("expectedRows", mapper.valueToTree(expected));
        report.set("actualRows", mapper.valueToTree(actual));
        mapper.writerWithDefaultPrettyPrinter()
            .writeValue(directory.resolve("result.json").toFile(), report);
        System.out.println("Live Q" + query + " matched " + actual.size() + " local Calcite rows; "
            + plan.tasks().size() + " tasks, " + plan.exchanges().size()
            + " Flight exchanges, 0 optimizer RPCs. Report: "
            + directory.resolve("result.json"));
      }
    }
  }

  private static void addPlacement(ObjectNode document, String table, String source) {
    final ObjectNode placement = ((ArrayNode) document.get("placements")).addObject();
    placement.putArray("logicalName").add("TPCH").add(table.toUpperCase(Locale.ROOT));
    placement.put("sourceId", source);
    placement.putObject("visibleNames").putArray(source).add(table);
  }

  private static DataFusionTestWorker worker(Path directory,
      Map<String, Map<String, Path>> tables, String source) throws Exception {
    return new DataFusionTestWorker(directory.resolve(source), source, tables.get(source));
  }

  private static List<Object[]> reference(CalciteConnection connection, int query)
      throws Exception {
    final List<Object[]> result = new ArrayList<>();
    try (Statement statement = connection.createStatement();
         ResultSet rows = statement.executeQuery(TpchQueryFixture.sql(query))) {
      while (rows.next()) {
        final Object[] row = new Object[rows.getMetaData().getColumnCount()];
        for (int i = 0; i < row.length; i++) {
          final Object value = rows.getObject(i + 1);
          row[i] = value instanceof Date
              ? Math.toIntExact(((Date) value).toLocalDate().toEpochDay()) : value;
        }
        result.add(row);
      }
    }
    return result;
  }

  private static double compare(List<Object[]> expected, List<Object[]> actual,
      int revenueColumn) {
    assertEquals(expected.size(), actual.size());
    double maximumDifference = 0;
    for (int i = 0; i < expected.size(); i++) {
      final Object[] reference = expected.get(i);
      final Object[] distributed = actual.get(i);
      assertEquals(reference.length, distributed.length);
      for (int column = 0; column < reference.length; column++) {
        if (column == revenueColumn) {
          final double revenue = ((Number) reference[column]).doubleValue();
          final double actualRevenue = ((Number) distributed[column]).doubleValue();
          assertEquals(revenue, actualRevenue, Math.max(1e-8D, Math.abs(revenue) * 1e-10D));
          maximumDifference = Math.max(maximumDifference, Math.abs(revenue - actualRevenue));
        } else {
          assertEquals(reference[column], distributed[column], "row " + i + ", column " + column);
        }
      }
    }
    return maximumDifference;
  }

  private static void assertNoRequests(List<DataFusionTestWorker> workers, boolean planning)
      throws Exception {
    for (DataFusionTestWorker worker : workers) {
      final String log = String.join("\n", Files.readAllLines(worker.log, StandardCharsets.UTF_8))
          .toLowerCase(Locale.ROOT);
      assertFalse(log.contains(": costing "), "no worker costing calls are allowed");
      if (planning) {
        assertFalse(log.contains(": executing "), "planning must not execute a task");
      }
    }
  }
}
