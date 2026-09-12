/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.adapter.fqp.cost.DataFusionCostModel;
import org.apache.calcite.adapter.fqp.cost.DataFusionStatisticsSnapshot;
import org.apache.calcite.adapter.fqp.cost.DataFusionTableStatistics;
import org.apache.calcite.adapter.fqp.cost.TpchQ3Fixture;
import org.apache.calcite.adapter.tpch.TpchSchema;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests generic placement-driven lowering of Calcite's Q3 tree. */
class FqpDistributedPlanLowererTest {
  @Test void repeatedBoundaryUsesOneProducerAndExchange() {
    final FqpPlanningConfig config = FqpTestDeployments.threeOwnerTpch();
    final SchemaPlus schema = Frameworks.createRootSchema(true);
    schema.add("TPCH", new TpchSchema(0.01D, 1, 1, false));
    final RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder()
        .defaultSchema(schema).build());
    final RelNode scan = builder.scan("TPCH", "CUSTOMER").build();
    builder.push(scan).push(scan);
    final RelNode join = builder.join(JoinRelType.INNER,
        builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0))).build();
    final Map<RelNode, String> destinations = new IdentityHashMap<>();
    destinations.put(scan, "df-customer");
    destinations.put(join, "df-lineitem");
    final FqpPlanningContext context = new FqpPlanningContext(config,
        new DataFusionCostModel(config, snapshots(), Collections.emptyMap()));
    final FqpDistributedPlan plan = new FqpDistributedPlanLowerer(context)
        .lower(new FqpPlacement(join, destinations, 0D));
    assertEquals(2, plan.tasks().size());
    assertEquals(1, plan.exchanges().size());
    assertEquals("df-lineitem", plan.rootTask().destination().sourceId());
  }

  @Test void lowersQ3IntoThreeOwnerLocalTasks() throws Exception {
    final FqpPlanningConfig config = FqpTestDeployments.threeOwnerTpch();
    final DataFusionCostModel costs = new DataFusionCostModel(config, snapshots(),
        Collections.emptyMap());

    final FqpDistributedPlan plan = new FqpDistributedPlanLowerer(
        new FqpPlanningContext(config, costs)).lower(TpchQ3Fixture.logicalPlan());

    assertEquals(3, plan.tasks().size());
    assertEquals(2, plan.exchanges().size());
    assertEquals(Arrays.asList("df-customer", "df-orders", "df-lineitem"),
        plan.topologicalTasks().stream().map(task -> task.destination().sourceId())
            .collect(Collectors.toList()));
    assertEquals("df-lineitem", plan.rootTask().destination().sourceId());
    assertEquals(0L, plan.rootTask().fragment().sourceIds().stream()
        .filter(source -> !source.equals("df-lineitem")).count());
  }

  private static Map<String, DataFusionStatisticsSnapshot> snapshots() {
    final Map<String, DataFusionStatisticsSnapshot> result = new LinkedHashMap<>();
    result.put("df-customer", snapshot("df-customer", "CUSTOMER", 1_500D, 180));
    result.put("df-orders", snapshot("df-orders", "ORDERS", 15_000D, 120));
    result.put("df-lineitem", snapshot("df-lineitem", "LINEITEM", 60_000D, 140));
    return result;
  }

  private static DataFusionStatisticsSnapshot snapshot(String source, String table,
      double rows, int width) {
    final Map<List<String>, DataFusionTableStatistics> tables = new LinkedHashMap<>();
    tables.put(Arrays.asList("TPCH", table), DataFusionTableStatistics.of(rows, width));
    return new DataFusionStatisticsSnapshot(source, "test", Instant.EPOCH, tables);
  }
}
