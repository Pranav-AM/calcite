/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.adapter.fqp.cost.DataFusionCostModel;
import org.apache.calcite.adapter.fqp.cost.DataFusionCostParameters;
import org.apache.calcite.adapter.fqp.cost.DataFusionStatisticsSnapshot;
import org.apache.calcite.adapter.fqp.cost.DataFusionTableStatistics;
import org.apache.calcite.adapter.fqp.cost.TpchQ3Fixture;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests Calcite-driven selection of an FQP plan without manual cost loops. */
class FqpOptimizerTest {
  @Test void calciteSelectsLocallyCostedQ3RemoteRoot() throws Exception {
    final RelNode logical = TpchQ3Fixture.logicalPlan();
    final FqpDestination destination = new FqpDestination("df1", DuckDBSqlDialect.DEFAULT);
    final FqpPlanningConfig config = FqpPlanningConfig.of("calcite",
        Collections.singletonList(destination), Arrays.asList(placement("CUSTOMER"),
            placement("ORDERS"), placement("LINEITEM")), 0.01D, true);
    final DataFusionCostModel model = new DataFusionCostModel(config,
        Collections.singletonMap("df1", snapshot()),
        Collections.singletonMap("df1", remoteParameters()));
    final FqpPlanningContext context = new FqpPlanningContext(config, model);

    final RelNode selected = new FqpOptimizer(context,
        Collections.singletonList(destination)).optimize(logical);
    final String plan = RelOptUtil.toString(selected);

    assertTrue(plan.contains("FqpToEnumerableConverter"), plan);
    assertTrue(plan.contains("FqpRemoteFragmentRel"), plan);
    assertTrue(plan.contains("destination=[df1]"), plan);
    assertEquals(0L, context.metrics().remoteCalls());
  }

  private static FqpTablePlacement placement(String table) {
    final List<String> logicalName = Arrays.asList("TPCH", table);
    return new FqpTablePlacement(logicalName, "df1", Collections.singletonMap("df1",
        Collections.singletonList(table.toLowerCase(Locale.ROOT))));
  }

  private static DataFusionStatisticsSnapshot snapshot() {
    final Map<List<String>, DataFusionTableStatistics> tables = new LinkedHashMap<>();
    tables.put(Arrays.asList("TPCH", "CUSTOMER"),
        DataFusionTableStatistics.of(1_500D, 180));
    tables.put(Arrays.asList("TPCH", "ORDERS"),
        DataFusionTableStatistics.of(15_000D, 120));
    tables.put(Arrays.asList("TPCH", "LINEITEM"),
        DataFusionTableStatistics.of(60_000D, 140));
    return new DataFusionStatisticsSnapshot("df1", "test-v1", Instant.EPOCH, tables);
  }

  private static DataFusionCostParameters remoteParameters() {
    // Make the fixture's remote engine deliberately cheaper than Calcite's
    // local Enumerable implementation. The planner must still discover and
    // compare both alternatives; this is not a forced trait conversion.
    return new DataFusionCostParameters(0D, 0D, 0D, 0D, 0D, 0D);
  }
}
