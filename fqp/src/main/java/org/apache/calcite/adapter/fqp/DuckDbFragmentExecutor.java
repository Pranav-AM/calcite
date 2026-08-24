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

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;

/** Executes exchange-free DuckDB fragments through JDBC. */
public final class DuckDbFragmentExecutor implements FqpFragmentExecutor {
  private final Map<String, DataSource> dataSources;

  public DuckDbFragmentExecutor(Map<String, DataSource> dataSources) {
    this.dataSources = Objects.requireNonNull(dataSources, "dataSources");
  }

  @Override public Enumerable<Object[]> execute(FqpFragment fragment) {
    if (!supportedPayloadFormats().contains(fragment.payload().format())) {
      throw new FqpExecutionException("DuckDB direct execution does not support "
          + fragment.payload().format());
    }
    if (!fragment.exchanges().isEmpty()) {
      throw new FqpExecutionException(
          "DuckDB direct execution does not implement FQP exchanges");
    }
    final String destination = fragment.destination().sourceId();
    final DataSource dataSource = dataSources.get(destination);
    if (dataSource == null) {
      throw new FqpExecutionException("no DuckDB data source for " + destination);
    }
    try (Connection connection = dataSource.getConnection();
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(fragment.sql())) {
      final ResultSetMetaData metadata = resultSet.getMetaData();
      final int fieldCount = metadata.getColumnCount();
      final List<Object[]> rows = new ArrayList<>();
      while (resultSet.next()) {
        final Object[] row = new Object[fieldCount];
        for (int field = 0; field < fieldCount; field++) {
          row[field] = resultSet.getObject(field + 1);
        }
        rows.add(row);
      }
      return Linq4j.asEnumerable(rows);
    } catch (SQLException e) {
      throw new FqpExecutionException("DuckDB fragment execution failed", e);
    }
  }
}
