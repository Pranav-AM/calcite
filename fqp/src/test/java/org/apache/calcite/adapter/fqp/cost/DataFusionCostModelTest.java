/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.cost;

import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpPlanningConfig;
import org.apache.calcite.adapter.fqp.FqpPlanningContext;
import org.apache.calcite.adapter.fqp.FqpTablePlacement;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the network-free DataFusion cost model using Calcite's TPC-H Q3. */
class DataFusionCostModelTest {
  @Test void estimatesCalciteQ3FromStartupSnapshots() throws Exception {
    final RelNode q3 = TpchQ3Fixture.logicalPlan();
    final FqpPlanningConfig config = config();
    final Map<String, DataFusionStatisticsSnapshot> snapshots = snapshots();
    final DataFusionCostModel model = new DataFusionCostModel(config, snapshots,
        Collections.emptyMap());

    final DataFusionCostEstimate estimate = model.estimate(q3, "df-lineitem");

    assertTrue(estimate.rowCount() > 0D);
    assertTrue(estimate.rowWidth() > 0);
    assertTrue(estimate.cpu() > 0D);
    assertTrue(estimate.io() > 0D);
    assertTrue(estimate.network() > 0D,
        "customer and orders must move when the root runs at lineitem");
    assertEquals(estimate.cpu() + estimate.io() + estimate.network(),
        estimate.totalCost());
  }

  @Test void planningContextCarriesLocalModel() {
    final FqpPlanningConfig config = config();
    final DataFusionCostModel model = new DataFusionCostModel(config, snapshots(),
        Collections.emptyMap());
    final FqpPlanningContext context = new FqpPlanningContext(config, model);

    assertSame(model, context.dataFusionCostModel());
    assertEquals(3, context.dataFusionCostModel().snapshots().size());
  }

  private static FqpPlanningConfig config() {
    return FqpPlanningConfig.of("calcite", Arrays.asList(
        new FqpDestination("df-customer", DuckDBSqlDialect.DEFAULT),
        new FqpDestination("df-orders", DuckDBSqlDialect.DEFAULT),
        new FqpDestination("df-lineitem", DuckDBSqlDialect.DEFAULT)),
        Arrays.asList(placement("CUSTOMER", "df-customer"),
            placement("ORDERS", "df-orders"), placement("LINEITEM", "df-lineitem")),
        0.0001D, true);
  }

  private static FqpTablePlacement placement(String table, String source) {
    final List<String> logicalName = Arrays.asList("TPCH", table);
    return new FqpTablePlacement(logicalName, source,
        Collections.singletonMap(source, logicalName));
  }

  private static Map<String, DataFusionStatisticsSnapshot> snapshots() {
    final Map<String, DataFusionStatisticsSnapshot> result = new LinkedHashMap<>();
    result.put("df-customer", snapshot("df-customer", "CUSTOMER", 150_000D, 180,
        column("C_CUSTKEY", 150_000D), column("C_MKTSEGMENT", 5D)));
    result.put("df-orders", snapshot("df-orders", "ORDERS", 1_500_000D, 120,
        column("O_ORDERKEY", 1_500_000D), column("O_CUSTKEY", 150_000D)));
    result.put("df-lineitem", snapshot("df-lineitem", "LINEITEM", 6_001_215D, 140,
        column("L_ORDERKEY", 1_500_000D)));
    return result;
  }

  @SafeVarargs private static DataFusionStatisticsSnapshot snapshot(String source,
      String table, double rows, int width,
      Map.Entry<String, DataFusionColumnStatistics>... columns) {
    final Map<String, DataFusionColumnStatistics> columnMap = new LinkedHashMap<>();
    for (Map.Entry<String, DataFusionColumnStatistics> column : columns) {
      columnMap.put(column.getKey(), column.getValue());
    }
    final DataFusionTableStatistics tableStatistics =
        new DataFusionTableStatistics(rows, width, columnMap);
    return new DataFusionStatisticsSnapshot(source, "test-v1", Instant.EPOCH,
        Collections.singletonMap(Arrays.asList("TPCH", table), tableStatistics));
  }

  private static Map.Entry<String, DataFusionColumnStatistics> column(String name,
      double distinctCount) {
    return new java.util.AbstractMap.SimpleImmutableEntry<>(name,
        DataFusionColumnStatistics.of(distinctCount, 0D));
  }
}
