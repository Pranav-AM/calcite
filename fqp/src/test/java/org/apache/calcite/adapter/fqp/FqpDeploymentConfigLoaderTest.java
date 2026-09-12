/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.adapter.fqp.cost.TpchQ3Fixture;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests query-independent deployment loading. */
class FqpDeploymentConfigLoaderTest {
  @Test void loadsDestinationsPolicyAndStrictPlacements() {
    final FqpPlanningConfig config = FqpTestDeployments.threeOwnerTpch();

    assertEquals(Duration.ofSeconds(30), config.remoteCostTimeout());
    assertEquals(0.01D, config.movementCostFactor());
    assertTrue(config.allowRemoteRoot());
    assertEquals("grpc://127.0.0.1:32012",
        config.destination("df-lineitem").get().capabilities()
            .flightEndpoint().get().toString());
    final FqpTablePlacement customer =
        config.placement(Arrays.asList("TPCH", "CUSTOMER")).get();
    assertEquals("df-customer", customer.sourceId());
    assertEquals(Arrays.asList("customer"),
        customer.visibleName("df-customer").get());
    assertFalse(customer.visibleName("df-orders").isPresent());
  }

  @Test void loadsStatisticsAndUsesConfiguredCostsWithoutWorkerCalls() throws Exception {
    final ObjectNode json = document();
    final ObjectNode costs = (ObjectNode) json.path("costParameters").path("df-lineitem");
    costs.put("scanCpuPerRow", 0);
    costs.put("expressionCpuPerRow", 0);
    costs.put("joinCpuPerRow", 0);
    costs.put("aggregateCpuPerRow", 0);
    costs.put("sortCpuPerComparison", 0);
    costs.put("ioPerByte", 0);
    final ObjectNode table = (ObjectNode) json.path("statistics").path("df-customer")
        .path("tables").get(0);
    table.putObject("columns").putObject("c_custkey")
        .put("distinctCount", 1500).put("nullCount", 0);

    final FqpDeployment deployment = load(json.toString());
    assertEquals(Instant.EPOCH, deployment.statistics().get("df-customer").loadedAt());
    assertEquals(1500D, deployment.statistics().get("df-customer")
        .table(Arrays.asList("TPCH", "CUSTOMER")).get().column("C_CUSTKEY").get()
        .distinctCount());
    final FqpPlanningContext context = deployment.planningContext();
    assertEquals(0D, context.dataFusionCostModel()
        .estimate(TpchQ3Fixture.logicalPlan(), "df-lineitem").cpu());
    assertEquals(0L, context.metrics().remoteCalls());
  }

  @Test void preservesDefaultsForOptionalFields() throws Exception {
    final ObjectNode json = document();
    json.remove(Arrays.asList("movementCostFactor", "allowRemoteRoot",
        "remoteCostTimeoutMillis", "statistics", "costParameters"));
    final FqpDeployment deployment = load(json.toString());
    assertEquals(0D, deployment.planningConfig().movementCostFactor());
    assertTrue(deployment.planningConfig().allowRemoteRoot());
    assertEquals(Duration.ofSeconds(30), deployment.planningConfig().remoteCostTimeout());
    assertTrue(deployment.statistics().isEmpty());
  }

  @Test void rejectsUnknownMissingAndNullProperties() throws Exception {
    final ObjectNode json = document();
    json.put("movmentCostFactor", 1);
    assertInvalid(json, "movmentCostFactor");
    json.remove("movmentCostFactor");
    json.remove("destinations");
    assertInvalid(json, "destinations");
    final ObjectNode nullPolicy = document();
    nullPolicy.putNull("allowRemoteRoot");
    assertInvalid(nullPolicy, "allowRemoteRoot");
  }

  @Test void rejectsInvalidEndpointsAndPolicy() throws Exception {
    final ObjectNode json = document();
    final ObjectNode destination = (ObjectNode) json.path("destinations").get(0);
    destination.put("flightEndpoint", "grpc://localhost");
    assertInvalid(json, "Flight endpoint");
    destination.put("flightEndpoint", "grpc://localhost:32010");
    destination.put("executionEndpoint", "file:///tmp/worker");
    assertInvalid(json, "HTTP endpoint");
    destination.put("executionEndpoint", "http://localhost:8081/v1/execute");
    json.put("movementCostFactor", -1);
    assertInvalid(json, "movementCostFactor");
    json.put("movementCostFactor", 0);
    json.put("remoteCostTimeoutMillis", 0);
    assertInvalid(json, "remoteCostTimeoutMillis");
  }

  @Test void rejectsUnknownOwnersAndInvalidStatistics() throws Exception {
    final ObjectNode json = document();
    final ObjectNode placement = (ObjectNode) json.path("placements").get(0);
    ((ObjectNode) placement.path("visibleNames")).putArray("absent").add("customer");
    assertInvalid(json, "unknown destination absent");
    ((ObjectNode) placement.path("visibleNames")).remove("absent");
    final ObjectNode snapshot = (ObjectNode) json.path("statistics").path("df-customer");
    snapshot.put("loadedAt", "not-a-date");
    assertInvalid(json, "not-a-date");
    snapshot.put("loadedAt", "1970-01-01T00:00:00Z");
    final ObjectNode table = (ObjectNode) snapshot.path("tables").get(0);
    table.put("rowCount", -1);
    assertInvalid(json, "rowCount");
    table.put("rowCount", 1);
    table.putArray("logicalName").add("TPCH").add("ORDERS");
    assertInvalid(json, "must belong to df-orders");
  }

  @Test void wrapsSyntaxDuplicatesAndFileErrors() {
    assertThrows(FqpDeploymentException.class, () -> load("{"));
    assertThrows(FqpDeploymentException.class,
        () -> load("{\"destinations\":[],\"destinations\":[]}"));
    final FqpDeploymentException error = assertThrows(FqpDeploymentException.class,
        () -> FqpDeploymentConfigLoader.loadDeployment(Paths.get("missing-fqp-deployment.json")));
    assertTrue(error.getMessage().contains("missing-fqp-deployment.json"));
    assertNotNull(error.getCause());
  }

  private static ObjectNode document() throws Exception {
    try (InputStream input = FqpDeploymentConfigLoaderTest.class.getResourceAsStream(
        "/org/apache/calcite/adapter/fqp/tpch-three-owner.json")) {
      return (ObjectNode) new ObjectMapper().readTree(input);
    }
  }

  private static FqpDeployment load(String json) {
    return FqpDeploymentConfigLoader.loadDeployment(new ByteArrayInputStream(
        json.getBytes(StandardCharsets.UTF_8)));
  }

  private static void assertInvalid(JsonNode document, String message) {
    final FqpDeploymentException error = assertThrows(FqpDeploymentException.class,
        () -> load(document.toString()));
    assertTrue(error.getMessage().contains(message), error.getMessage());
    assertNotNull(error.getCause());
  }
}
