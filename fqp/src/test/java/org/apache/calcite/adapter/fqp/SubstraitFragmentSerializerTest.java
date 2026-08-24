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
package org.apache.calcite.adapter.fqp;

import io.substrait.proto.Plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubstraitFragmentSerializerTest {
  @Test void serializesDestinationVisibleScanAndFieldProject() throws Exception {
    final RelBuilder builder = builder();
    final RelNode rel = builder.scan("EMP").project(builder.field("EMPNO")).build();

    final FqpFragment fragment = new SubstraitFragmentSerializer(config(true)).serialize(rel, "df1");
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
    final FqpFragment fragment = new SubstraitFragmentSerializer(config(true)).serialize(
        LogicalFilter.create(scan, builder.literal(true)), "df1");
    final Plan plan = Plan.parseFrom(fragment.payload().bytes());

    assertTrue(plan.getRelations(0).getRoot().getInput().hasFilter());
    assertTrue(plan.getRelations(0).getRoot().getInput().getFilter().getCondition()
        .getLiteral().getBoolean());
  }

  @Test void rejectsExpressionFilterAndUnsupportedDestination() {
    final RelBuilder builder = builder();
    assertThrows(SubstraitSerializationException.class, () ->
        new SubstraitFragmentSerializer(config(true)).serialize(
            builder.scan("EMP").filter(builder.equals(builder.field("EMPNO"), builder.literal(1)))
                .build(), "df1"));
    assertThrows(SubstraitSerializationException.class, () ->
        new SubstraitFragmentSerializer(config(false)).serialize(builder().scan("EMP").build(), "df1"));
  }

  private static RelBuilder builder() {
    final SchemaPlus root = Frameworks.createRootSchema(true);
    root.add("EMP", new TestTable());
    return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(root).build());
  }

  private static FqpPlanningConfig config(boolean supportsSubstrait) {
    final FqpDestinationCapabilities capabilities = supportsSubstrait
        ? FqpDestinationCapabilities.of(EnumSet.of(FqpFragmentPayload.Format.SQL,
            FqpFragmentPayload.Format.SUBSTRAIT_BINARY), null, null, null)
        : FqpDestinationCapabilities.sqlOnly();
    final Map<String, java.util.List<String>> visibleNames = new LinkedHashMap<>();
    visibleNames.put("pg1", Arrays.asList("public", "emp"));
    visibleNames.put("df1", Arrays.asList("remote", "pg1_emp"));
    return FqpPlanningConfig.of("coordinator", Arrays.asList(
        new FqpDestination("pg1", DuckDBSqlDialect.DEFAULT),
        new FqpDestination("df1", DuckDBSqlDialect.DEFAULT, capabilities)),
        Collections.singletonList(new FqpTablePlacement(Collections.singletonList("EMP"), "pg1",
            visibleNames)), 0.01D, false);
  }

  private static final class TestTable extends AbstractTable {
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder().add("EMPNO", SqlTypeName.INTEGER)
          .add("ENAME", SqlTypeName.VARCHAR).build();
    }
  }
}
