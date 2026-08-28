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
package org.apache.calcite.adapter.fqp.coordinator;

import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpExchangeRequirement;
import org.apache.calcite.adapter.fqp.FqpExecutionResult;
import org.apache.calcite.adapter.fqp.FqpFragment;
import org.apache.calcite.adapter.fqp.FqpFragmentPayload;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link CoordinatorFragmentExecutor}. */
class CoordinatorFragmentExecutorTest {
  @Test void delegatesFragmentAndExchangeMetadataToCoordinator() {
    final FqpDestination destination = new FqpDestination("pg2", DuckDBSqlDialect.DEFAULT);
    final RelDataType rowType = new JavaTypeFactoryImpl().builder()
        .add("id", SqlTypeName.INTEGER)
        .build();
    final FqpFragment fragment = new FqpFragment(destination,
        FqpFragmentPayload.binary(FqpFragmentPayload.Format.SUBSTRAIT_BINARY,
            new byte[] {1, 2, 3}), rowType,
        Collections.singleton("pg2"), Collections.singletonList(
            new FqpExchangeRequirement("pg2", "coordinator", 8D)));
    final CoordinatorFragmentExecutor executor = new CoordinatorFragmentExecutor("coordinator",
        request -> {
          assertEquals("coordinator", request.coordinatorSourceId());
          assertEquals(fragment, request.fragment());
          assertEquals(1, request.fragment().exchanges().size());
          return new FqpExecutionResult(Collections.singletonList(new Object[] {1}));
        });

    try (Enumerator<Object[]> rows = executor.execute(fragment).enumerator()) {
      assertTrue(rows.moveNext());
      assertEquals(1, rows.current()[0]);
    }
  }
}
