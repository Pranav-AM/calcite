/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.adapter.fqp.cost.DataFusionCostModel;
import org.apache.calcite.adapter.fqp.cost.DataFusionColumnStatistics;
import org.apache.calcite.adapter.fqp.cost.DataFusionCostParameters;
import org.apache.calcite.adapter.fqp.cost.DataFusionStatisticsSnapshot;
import org.apache.calcite.adapter.fqp.cost.DataFusionTableStatistics;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies Volcano selects placement using execution and intermediate transfer costs. */
class FqpPlacementOptimizerTest {
  @Test void movesSmallLeftToRightOwner() {
    final FqpPlanningContext context = context(1, 100, 1, 1, 1, 100, true, true);
    final FqpPlacement selected = new FqpPlacementOptimizer(context).optimize(join());
    final FqpDistributedPlan plan = new FqpDistributedPlanLowerer(context).lower(selected);
    assertEquals("right", plan.rootTask().destination().sourceId());
    assertEquals(1, plan.exchanges().size());
    assertEquals("left", plan.exchanges().iterator().next().sourceId());
    assertEquals(8D, plan.exchanges().iterator().next().estimatedBytes());
    assertEquals(0L, context.metrics().remoteCalls());
  }

  @Test void movesSmallRightToLeftOwner() {
    final FqpPlanningContext context = context(100, 1, 1, 1, 1, 100, true, true);
    final FqpDistributedPlan plan = new FqpDistributedPlanLowerer(context).lower(join());
    assertEquals("left", plan.rootTask().destination().sourceId());
    assertEquals("right", plan.exchanges().iterator().next().sourceId());
  }

  @Test void movesBothInputsToCheaperThirdWorker() {
    final FqpPlanningContext context = context(100, 100, 0.01, 100, 100, 0, true, true);
    final FqpPlacement selected = new FqpPlacementOptimizer(context).optimize(join());
    final FqpDistributedPlan plan = new FqpDistributedPlanLowerer(context).lower(selected);
    assertEquals("third", plan.rootTask().destination().sourceId());
    assertEquals(2, plan.exchanges().size());
    assertTrue(plan.rootTask().localTables().isEmpty());
    assertEquals(2, plan.exchanges().stream().map(FqpExchange::temporaryInputName)
        .distinct().count());
    assertEquals(232D, selected.cost(), 0.00001D);
    assertEquals(0L, context.metrics().remoteCalls());
  }

  @Test void movementCostCanOutweighCheaperExecution() {
    final FqpPlanningContext cheapTransfers = context(100, 100, 0.01, 1, 1, 0, true, true);
    final FqpPlanningContext expensiveTransfers = context(100, 100, 100, 1, 1, 0, true, true);
    assertEquals("third", new FqpDistributedPlanLowerer(cheapTransfers).lower(join())
        .rootTask().destination().sourceId());
    assertFalse("third".equals(new FqpDistributedPlanLowerer(expensiveTransfers).lower(join())
        .rootTask().destination().sourceId()));
  }

  @Test void excludesWorkerWithoutFlightAndEnforcesRootPolicy() {
    final FqpPlanningContext noFlight = context(100, 100, 0.01, 100, 100, 0, false, true);
    assertFalse("third".equals(new FqpDistributedPlanLowerer(noFlight).lower(join())
        .rootTask().destination().sourceId()));
    final FqpPlanningContext noRemoteRoot = context(100, 100, 0.01, 1, 1, 0, true, false);
    assertThrows(IllegalArgumentException.class,
        () -> new FqpPlacementOptimizer(noRemoteRoot).optimize(join()));
  }

  @Test void costsFilteredIntermediateRatherThanFullBaseTable() {
    final FqpPlanningContext context = context(1000, 10, 1, 1, 1, 100, true, true);
    final FqpDistributedPlan plan = new FqpDistributedPlanLowerer(context).lower(join(true));
    assertEquals("right", plan.rootTask().destination().sourceId());
    assertEquals(8D, plan.exchanges().iterator().next().estimatedBytes());
  }

  private static FqpPlanningContext context(double leftRows, double rightRows,
      double movement, double leftJoin, double rightJoin, double thirdJoin,
      boolean thirdFlight, boolean allowRemoteRoot) {
    final FqpPlanningConfig config = FqpPlanningConfig.of("calcite", Arrays.asList(
        destination("left", true), destination("right", true),
        destination("third", thirdFlight)), Arrays.asList(placement("A", "left"),
            placement("B", "right")), movement, allowRemoteRoot);
    final Map<String, DataFusionStatisticsSnapshot> stats = new LinkedHashMap<>();
    stats.put("left", snapshot("left", "A", leftRows));
    stats.put("right", snapshot("right", "B", rightRows));
    final Map<String, DataFusionCostParameters> costs = new LinkedHashMap<>();
    costs.put("left", coefficients(leftJoin));
    costs.put("right", coefficients(rightJoin));
    costs.put("third", coefficients(thirdJoin));
    return new FqpPlanningContext(config, new DataFusionCostModel(config, stats, costs));
  }

  private static FqpDestination destination(String source, boolean flight) {
    return new FqpDestination(source, DuckDBSqlDialect.DEFAULT,
        FqpDestinationCapabilities.of(Collections.singleton(
            FqpFragmentPayload.Format.SUBSTRAIT_BINARY), URI.create("http://127.0.0.1:1"),
            null, flight ? URI.create("grpc://127.0.0.1:1") : null));
  }

  private static FqpTablePlacement placement(String table, String source) {
    final List<String> name = Collections.singletonList(table);
    return new FqpTablePlacement(name, source, Collections.singletonMap(source, name));
  }

  private static DataFusionStatisticsSnapshot snapshot(String source, String table, double rows) {
    return new DataFusionStatisticsSnapshot(source, "test", Instant.EPOCH,
        Collections.singletonMap(Collections.singletonList(table),
            new DataFusionTableStatistics(rows, 8, Collections.singletonMap("id",
                DataFusionColumnStatistics.of(rows, 0)))));
  }

  private static DataFusionCostParameters coefficients(double join) {
    return new DataFusionCostParameters(1, 0, join, 0, 0, 0);
  }

  private static RelNode join() {
    return join(false);
  }

  private static RelNode join(boolean filterLeft) {
    final SchemaPlus schema = Frameworks.createRootSchema(true);
    for (String name : Arrays.asList("A", "B")) {
      schema.add(name, new AbstractTable() {
        @Override public RelDataType getRowType(RelDataTypeFactory factory) {
          return factory.builder().add("id", SqlTypeName.BIGINT).build();
        }
      });
    }
    final RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder()
        .defaultSchema(schema).build());
    builder.scan("A");
    if (filterLeft) {
      builder.filter(builder.equals(builder.field(0), builder.literal(1L)));
    }
    builder.scan("B");
    return builder.join(JoinRelType.INNER,
        builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0))).build();
  }
}
