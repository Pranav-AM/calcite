/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.rules;

import org.apache.calcite.adapter.fqp.FqpConvention;
import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpPlanningConfig;
import org.apache.calcite.adapter.fqp.FqpPlanningContext;
import org.apache.calcite.adapter.fqp.FqpTablePlacement;
import org.apache.calcite.adapter.fqp.cost.DataFusionCostModel;
import org.apache.calcite.adapter.fqp.cost.DataFusionStatisticsSnapshot;
import org.apache.calcite.adapter.fqp.cost.DataFusionTableStatistics;
import org.apache.calcite.adapter.fqp.rel.FqpRemoteFragmentRel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Verifies FQP rules cost candidates locally without a remote client. */
class FqpLocalCostRulesTest {
  @Test void scanRuleUsesStartupSnapshotOnly() {
    final SchemaPlus root = Frameworks.createRootSchema(true);
    root.add("EMP", new TestTable());
    final RelBuilder builder = RelBuilder.create(Frameworks.newConfigBuilder()
        .defaultSchema(root).build());
    final RelNode scan = builder.scan("EMP").build();
    final FqpDestination destination = new FqpDestination("df1", DuckDBSqlDialect.DEFAULT);
    final FqpTablePlacement placement = new FqpTablePlacement(
        Collections.singletonList("EMP"), "df1",
        Collections.singletonMap("df1", Collections.singletonList("emp")));
    final FqpPlanningConfig config = FqpPlanningConfig.of("calcite",
        Collections.singletonList(destination), Collections.singletonList(placement),
        0.01D, true);
    final DataFusionStatisticsSnapshot snapshot = new DataFusionStatisticsSnapshot(
        "df1", "test-v1", Instant.EPOCH,
        Collections.singletonMap(Collections.singletonList("EMP"),
            DataFusionTableStatistics.of(100D, 12)));
    final DataFusionCostModel model = new DataFusionCostModel(config,
        Collections.singletonMap("df1", snapshot), Collections.emptyMap());
    final FqpPlanningContext context = new FqpPlanningContext(config, model);

    final RelNode converted = FqpScanRule.create(new FqpConvention(destination, context))
        .convert(scan);

    assertNotNull(converted);
    final FqpRemoteFragmentRel fragment =
        assertInstanceOf(FqpRemoteFragmentRel.class, converted);
    assertEquals(100D, fragment.estimate().rowCount());
    assertEquals(0L, context.metrics().remoteCalls());
    assertEquals(0L, context.metrics().costRequests());
  }

  /** Minimal table used to exercise the scan rule. */
  private static final class TestTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder().add("ID", SqlTypeName.BIGINT)
          .add("NAME", SqlTypeName.VARCHAR).build();
    }
  }
}
