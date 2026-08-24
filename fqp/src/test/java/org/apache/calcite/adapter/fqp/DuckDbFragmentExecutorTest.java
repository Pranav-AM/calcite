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

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DuckDbFragmentExecutorTest {
  @Test void executesExchangeFreeFragment() {
    final DataSource dataSource = JdbcSchema.dataSource("jdbc:duckdb:",
        "org.duckdb.DuckDBDriver", null, null);
    final FqpDestination destination = new FqpDestination("duck1", DuckDBSqlDialect.DEFAULT);
    final RelDataType rowType = new JavaTypeFactoryImpl().builder()
        .add("answer", org.apache.calcite.sql.type.SqlTypeName.INTEGER)
        .build();
    final FqpFragment fragment = new FqpFragment(destination, "SELECT 42 AS answer",
        rowType, Collections.singleton("duck1"), Collections.emptyList());
    final DuckDbFragmentExecutor executor = new DuckDbFragmentExecutor(
        Collections.singletonMap("duck1", dataSource));

    try (Enumerator<Object[]> rows = executor.execute(fragment).enumerator()) {
      assertTrue(rows.moveNext());
      assertEquals(42, ((Number) rows.current()[0]).intValue());
      assertTrue(!rows.moveNext());
    }
  }
}
