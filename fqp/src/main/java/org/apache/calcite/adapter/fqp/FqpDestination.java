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

import org.apache.calcite.sql.SqlDialect;

import java.util.Objects;

/** An FQP location at which a fragment may be executed. */
public final class FqpDestination {
  private final String sourceId;
  private final SqlDialect dialect;
  private final FqpDestinationCapabilities capabilities;

  public FqpDestination(String sourceId, SqlDialect dialect) {
    this(sourceId, dialect, FqpDestinationCapabilities.substraitOnly());
  }

  public FqpDestination(String sourceId, SqlDialect dialect,
      FqpDestinationCapabilities capabilities) {
    this.sourceId = requireNonBlank(sourceId, "sourceId");
    this.dialect = Objects.requireNonNull(dialect, "dialect");
    this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
  }

  public String sourceId() {
    return sourceId;
  }

  public SqlDialect dialect() {
    return dialect;
  }

  public FqpDestinationCapabilities capabilities() {
    return capabilities;
  }

  public static String requireNonBlank(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.trim().isEmpty()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
