/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.calcite.adapter.fqp.serialization;

import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpDestinationCapabilities;
import org.apache.calcite.adapter.fqp.FqpFragment;
import org.apache.calcite.adapter.fqp.FqpFragmentPayload;
import org.apache.calcite.adapter.fqp.FqpPlanningConfig;
import org.apache.calcite.adapter.fqp.FqpTablePlacement;
import org.apache.calcite.adapter.fqp.cost.TpchQ3Fixture;
import org.apache.calcite.adapter.fqp.cost.TpchQueryFixture;

import io.substrait.proto.Plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link SubstraitFragmentSerializer}. */
class SubstraitFragmentSerializerTest {
  @Test void serializesDestinationVisibleScanAndFieldProject() throws Exception {
    final RelBuilder builder = builder();
    final RelNode rel = builder.scan("EMP").project(builder.field("EMPNO")).build();

    final FqpFragment fragment =
        new SubstraitFragmentSerializer(config()).serialize(rel, "df1");
    final Plan plan = Plan.parseFrom(fragment.payload().bytes());

    assertEquals(FqpFragmentPayload.Format.SUBSTRAIT_BINARY, fragment.payload().format());
    assertEquals(Collections.singleton("pg1"), fragment.sourceIds());
    assertTrue(plan.getRelations(0).getRoot().getInput().hasProject());
    assertEquals("remote", plan.getRelations(0).getRoot().getInput().getProject()
        .getInput().getRead().getNamedTable().getNames(0));
    assertEquals("pg1_emp", plan.getRelations(0).getRoot().getInput().getProject()
        .getInput().getRead().getNamedTable().getNames(1));
    assertEquals("EMPNO", plan.getRelations(0).getRoot().getNames(0));
  }

  @Test void serializesBooleanLiteralFilter() throws Exception {
    final RelBuilder builder = builder();
    final RelNode scan = builder.scan("EMP").build();
    final FqpFragment fragment = new SubstraitFragmentSerializer(config()).serialize(
        LogicalFilter.create(scan, builder.literal(true)), "df1");
    final Plan plan = Plan.parseFrom(fragment.payload().bytes());

    assertTrue(plan.getRelations(0).getRoot().getInput().hasFilter());
    assertTrue(plan.getRelations(0).getRoot().getInput().getFilter().getCondition()
        .getLiteral().getBoolean());
  }

  @Test void serializesInnerEqualityJoin() throws Exception {
    final RelBuilder builder = builder();
    builder.scan("EMP").scan("EMP");
    final RelNode join = builder.join(JoinRelType.INNER,
        builder.equals(builder.field(2, 0, "EMPNO"),
            builder.field(2, 1, "EMPNO"))).build();

    final Plan plan = Plan.parseFrom(
        new SubstraitFragmentSerializer(config()).serialize(join, "df1").payload().bytes());

    assertTrue(plan.getRelations(0).getRoot().getInput().hasJoin());
    assertEquals("equal:any_any",
        plan.getExtensions(0).getExtensionFunction().getName());
    assertEquals(2, plan.getRelations(0).getRoot().getInput().getJoin()
        .getExpression().getScalarFunction().getArgumentsCount());
  }

  @Test void serializesEqualityFilter() throws Exception {
    final RelBuilder builder = builder();
    final Plan plan = Plan.parseFrom(new SubstraitFragmentSerializer(config()).serialize(
        builder.scan("EMP").filter(builder.equals(builder.field("EMPNO"), builder.literal(1)))
            .build(), "df1").payload().bytes());
    assertTrue(plan.getRelations(0).getRoot().getInput().hasFilter());
  }

  @Test void serializesCalciteQ3OperatorTree() throws Exception {
    final FqpFragment fragment = new SubstraitFragmentSerializer(q3Config())
        .serialize(TpchQ3Fixture.logicalPlan(), "df1");
    final Plan plan = Plan.parseFrom(fragment.payload().bytes());

    assertTrue(plan.getRelations(0).getRoot().getInput().hasFetch());
    assertTrue(plan.getRelations(0).getRoot().getInput().getFetch().getInput().hasSort());
    assertTrue(plan.getRelations(0).getRoot().getInput().getFetch().getInput().getSort()
        .getInput().hasProject());
    assertEquals(9, plan.getExtensionsCount());
    assertTrue(plan.getExtensionsList().stream().anyMatch(extension ->
        extension.getExtensionFunction().getName().startsWith("sum:")));
  }

  @Test void serializesQ5AndQ7OperatorTrees() throws Exception {
    for (int query : new int[] {5, 7}) {
      final Plan plan = Plan.parseFrom(new SubstraitFragmentSerializer(q3Config())
          .serialize(TpchQueryFixture.logicalPlan(query, 0.01D), "df1").payload().bytes());
      assertTrue(plan.getRelations(0).getRoot().getInput().hasSort());
      if (query == 7) {
        final String encoded = plan.getRelations(0).toString();
        assertTrue(encoded.contains("function_reference: 8"), "Q7 must encode OR");
        assertTrue(encoded.contains("function_reference: 9"), "Q7 must encode year extraction");
        assertTrue(encoded.contains("string: \"year\""));
      }
    }
  }

  private static RelBuilder builder() {
    final SchemaPlus root = Frameworks.createRootSchema(true);
    root.add("EMP", new TestTable());
    return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(root).build());
  }

  private static FqpPlanningConfig config() {
    final FqpDestinationCapabilities capabilities = FqpDestinationCapabilities.of(
        EnumSet.of(FqpFragmentPayload.Format.SUBSTRAIT_BINARY), null, null, null);
    final Map<String, java.util.List<String>> visibleNames = new LinkedHashMap<>();
    visibleNames.put("pg1", Arrays.asList("public", "emp"));
    visibleNames.put("df1", Arrays.asList("remote", "pg1_emp"));
    return FqpPlanningConfig.of("coordinator", Arrays.asList(
        new FqpDestination("pg1", DuckDBSqlDialect.DEFAULT),
        new FqpDestination("df1", DuckDBSqlDialect.DEFAULT, capabilities)),
        Collections.singletonList(new FqpTablePlacement(Collections.singletonList("EMP"), "pg1",
            visibleNames)), 0.01D, false);
  }

  private static FqpPlanningConfig q3Config() {
    final FqpDestinationCapabilities capabilities = FqpDestinationCapabilities.of(
        EnumSet.of(FqpFragmentPayload.Format.SUBSTRAIT_BINARY), null, null, null);
    final FqpDestination destination = new FqpDestination("df1", DuckDBSqlDialect.DEFAULT,
        capabilities);
    return FqpPlanningConfig.of("calcite", Collections.singletonList(destination),
        Arrays.asList(q3Placement("CUSTOMER"), q3Placement("ORDERS"),
            q3Placement("LINEITEM"), q3Placement("SUPPLIER"),
            q3Placement("NATION"), q3Placement("REGION")), 0.01D, true);
  }

  private static FqpTablePlacement q3Placement(String table) {
    final java.util.List<String> logicalName = Arrays.asList("TPCH", table);
    return new FqpTablePlacement(logicalName, "df1",
        Collections.singletonMap("df1",
            Collections.singletonList(table.toLowerCase(java.util.Locale.ROOT))));
  }

  /** Table with the row type used by serializer tests. */
  private static final class TestTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder().add("EMPNO", SqlTypeName.INTEGER)
          .add("ENAME", SqlTypeName.VARCHAR).build();
    }
  }
}
