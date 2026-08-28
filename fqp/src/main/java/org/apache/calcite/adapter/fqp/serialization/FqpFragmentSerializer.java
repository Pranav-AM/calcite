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
import org.apache.calcite.adapter.fqp.FqpFragment;
import org.apache.calcite.adapter.fqp.FqpPlanningConfig;

import org.apache.calcite.rel.RelNode;

import java.util.Objects;

/** Serializes an FQP fragment as a binary Substrait plan. */
public final class FqpFragmentSerializer {
  private final SubstraitFragmentSerializer substrait;

  public FqpFragmentSerializer(FqpPlanningConfig config) {
    this.substrait = new SubstraitFragmentSerializer(
        Objects.requireNonNull(config, "config"));
  }

  public FqpFragment serialize(RelNode root, FqpDestination destination) {
    return substrait.serialize(root, destination);
  }
}
