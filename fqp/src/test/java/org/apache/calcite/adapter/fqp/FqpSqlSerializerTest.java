/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.fqp;

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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FqpSqlSerializerTest {
  @Test void serializesTableUsingDestinationVisibleName() {
    final FqpFragment fragment = new FqpSqlSerializer(config())
        .serialize(builder().scan("EMP").build(), "pg2");

    assertTrue(fragment.sql().contains("\"remote\".\"pg1_emp\""));
    assertEquals(Collections.singleton("pg1"), fragment.sourceIds());
    assertEquals("pg2", fragment.destination().sourceId());
  }

  @Test void serializesInnerEquiJoin() {
    final RelBuilder builder = builder();
    final RelNode join = builder.scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.INNER, "DEPTNO")
        .build();

    final String sql = new FqpSqlSerializer(config()).serialize(join, "pg2").sql();
    assertTrue(sql.contains("INNER JOIN"));
    assertTrue(sql.contains("\"remote\".\"pg1_emp\""));
    assertTrue(sql.contains("\"public\".\"dept\""));
  }

  @Test void rejectsOuterJoin() {
    final RelBuilder builder = builder();
    final RelNode join = builder.scan("EMP")
        .scan("DEPT")
        .join(JoinRelType.LEFT, "DEPTNO")
        .build();

    assertThrows(FqpSerializationException.class,
        () -> new FqpSqlSerializer(config()).serialize(join, "pg2"));
  }

  @Test void placementIsImmutableAndRejectsDuplicates() {
    assertThrows(IllegalArgumentException.class,
        () -> FqpPlanningConfig.of("coordinator",
            Arrays.asList(destination("pg1"), destination("pg1")),
            Collections.emptyList(), 0.01D, false));
  }

  private static RelBuilder builder() {
    final SchemaPlus root = Frameworks.createRootSchema(true);
    root.add("EMP", new TestTable("EMPNO", "DEPTNO"));
    root.add("DEPT", new TestTable("DEPTNO", "DNAME"));
    return RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(root).build());
  }

  private static FqpPlanningConfig config() {
    return FqpPlanningConfig.of("coordinator",
        Arrays.asList(destination("pg1"), destination("pg2")),
        Arrays.asList(placement("EMP", "pg1", "main", "emp", "remote", "pg1_emp"),
            placement("DEPT", "pg2", "public", "dept", "public", "dept")),
        0.01D, false);
  }

  private static FqpDestination destination(String sourceId) {
    return new FqpDestination(sourceId, DuckDBSqlDialect.DEFAULT);
  }

  private static FqpTablePlacement placement(String table, String sourceId,
      String ownSchema, String ownTable, String remoteSchema, String remoteTable) {
    final Map<String, java.util.List<String>> visibleNames = new LinkedHashMap<>();
    visibleNames.put(sourceId, Arrays.asList(ownSchema, ownTable));
    visibleNames.put(sourceId.equals("pg1") ? "pg2" : "pg1",
        Arrays.asList(remoteSchema, remoteTable));
    return new FqpTablePlacement(Collections.singletonList(table), sourceId, visibleNames);
  }

  private static final class TestTable extends AbstractTable {
    private final String firstField;
    private final String secondField;

    TestTable(String firstField, String secondField) {
      this.firstField = firstField;
      this.secondField = secondField;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add(firstField, SqlTypeName.INTEGER)
          .add(secondField, SqlTypeName.INTEGER)
          .build();
    }
  }
}
