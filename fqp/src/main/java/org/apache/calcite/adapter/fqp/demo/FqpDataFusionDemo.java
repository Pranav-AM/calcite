/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.demo;

import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpDestinationCapabilities;
import org.apache.calcite.adapter.fqp.FqpFragment;
import org.apache.calcite.adapter.fqp.FqpFragmentPayload;
import org.apache.calcite.adapter.fqp.FqpPlanningConfig;
import org.apache.calcite.adapter.fqp.FqpTablePlacement;
import org.apache.calcite.adapter.fqp.RemoteCostRequest;
import org.apache.calcite.adapter.fqp.RemoteCostResult;
import org.apache.calcite.adapter.fqp.cost.DataFusionCostClient;
import org.apache.calcite.adapter.fqp.execution.DataFusionFragmentExecutor;
import org.apache.calcite.adapter.fqp.serialization.SubstraitFragmentSerializer;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Minimal two-worker Substrait/DataFusion demonstration. */
public final class FqpDataFusionDemo {
  private static final String SQL = "SELECT l.\"id\", r.\"value\" "
      + "FROM local_rows l JOIN remote_rows r ON l.\"id\" = r.\"id\"";

  private FqpDataFusionDemo() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "usage: FqpDataFusionDemo <df1-base-uri> <df2-base-uri>");
    }
    final FqpDestination df1 = destination("df1", args[0]);
    final FqpDestination df2 = destination("df2", args[1]);
    final FqpPlanningConfig planningConfig = FqpPlanningConfig.of("calcite",
        Arrays.asList(df1, df2), placements(), 0.01D, true, Duration.ofSeconds(10));
    final RelNode logicalPlan = logicalPlan();
    final DataFusionCostClient costClient = new DataFusionCostClient(planningConfig);
    FqpFragment selected = null;
    double selectedCost = Double.POSITIVE_INFINITY;

    System.out.println("Calcite logical plan:\n" + logicalPlan.explain());
    for (FqpDestination destination : Arrays.asList(df1, df2)) {
      final FqpFragment fragment =
          new SubstraitFragmentSerializer(planningConfig).serialize(logicalPlan, destination);
      final RemoteCostResult result = costClient.explain(new RemoteCostRequest("calcite",
          destination.sourceId(), fragment.payload(), Duration.ofSeconds(10)));
      if (!result.isSuccess()) {
        throw new IllegalStateException("costing failed at " + destination.sourceId()
            + ": " + result.failureMessage().orElse("unknown failure"));
      }
      final double cost = result.estimate().get().getTotalCost();
      System.out.println(destination.sourceId() + " total cost: " + cost);
      if (cost < selectedCost) {
        selected = fragment;
        selectedCost = cost;
      }
    }

    if (selected == null) {
      throw new IllegalStateException("no DataFusion destination was selected");
    }
    System.out.println("Selected destination: " + selected.destination().sourceId());
    for (Object[] row : new DataFusionFragmentExecutor(planningConfig).execute(selected)) {
      System.out.println(Arrays.toString(row));
    }
  }

  private static RelNode logicalPlan() throws Exception {
    final SchemaPlus schema = Frameworks.createRootSchema(true);
    schema.add("LOCAL_ROWS", new DemoTable());
    schema.add("REMOTE_ROWS", new DemoTable());
    final FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(schema).build();
    final Planner planner = Frameworks.getPlanner(config);
    final SqlNode parsed = planner.parse(SQL);
    return planner.rel(planner.validate(parsed)).rel;
  }

  private static FqpDestination destination(String sourceId, String baseUri) {
    final String base = baseUri.endsWith("/")
        ? baseUri.substring(0, baseUri.length() - 1) : baseUri;
    final FqpDestinationCapabilities capabilities = FqpDestinationCapabilities.of(
        EnumSet.of(FqpFragmentPayload.Format.SUBSTRAIT_BINARY),
        URI.create(base + "/v1/execute"), URI.create(base + "/v1/cost"), null);
    return new FqpDestination(sourceId, SqlDialect.DatabaseProduct.CALCITE.getDialect(),
        capabilities);
  }

  private static List<FqpTablePlacement> placements() {
    final List<FqpTablePlacement> placements = new ArrayList<>();
    placements.add(placement("LOCAL_ROWS", "df1", "local_rows"));
    placements.add(placement("REMOTE_ROWS", "df2", "remote_rows"));
    return placements;
  }

  private static FqpTablePlacement placement(String logicalName, String owner,
      String workerName) {
    final Map<String, List<String>> visibleNames = new LinkedHashMap<>();
    visibleNames.put("df1", Collections.singletonList(workerName));
    visibleNames.put("df2", Collections.singletonList(workerName));
    return new FqpTablePlacement(Collections.singletonList(logicalName), owner, visibleNames);
  }

  /** Schema shared by both demo tables. */
  private static final class DemoTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder().add("id", SqlTypeName.BIGINT)
          .add("value", SqlTypeName.VARCHAR).build();
    }
  }
}
