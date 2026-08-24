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

import org.apache.calcite.rel.RelNode;

import java.util.Objects;

/** Selects a destination-supported fragment encoding, with SQL as fallback. */
final class FqpFragmentSerializer {
  private final FqpSqlSerializer sql;
  private final SubstraitFragmentSerializer substrait;

  FqpFragmentSerializer(FqpPlanningConfig config) {
    this.sql = new FqpSqlSerializer(Objects.requireNonNull(config, "config"));
    this.substrait = new SubstraitFragmentSerializer(config);
  }

  FqpFragment serialize(RelNode root, FqpDestination destination) {
    if (destination.capabilities().supports(FqpFragmentPayload.Format.SUBSTRAIT_BINARY)) {
      try {
        return substrait.serialize(root, destination);
      } catch (SubstraitSerializationException ignored) {
        // SQL retains compatibility for richer PostgreSQL/DuckDB fragments.
      }
    }
    return sql.serialize(root, destination);
  }
}
