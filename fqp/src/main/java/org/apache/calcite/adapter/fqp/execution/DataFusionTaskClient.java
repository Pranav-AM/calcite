/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpExecutionException;
import org.apache.calcite.adapter.fqp.FqpFragmentPayload;
import org.apache.calcite.adapter.fqp.FqpPlanningConfig;
import org.apache.calcite.adapter.fqp.FqpTask;

import java.net.URI;
import java.util.Objects;

/** Executes distributed tasks at their DataFusion HTTP endpoints. */
public final class DataFusionTaskClient implements FqpTaskClient {
  private final FqpPlanningConfig config;

  public DataFusionTaskClient(FqpPlanningConfig config) {
    this.config = Objects.requireNonNull(config, "config");
  }

  @Override public byte[] execute(FqpTask task) {
    Objects.requireNonNull(task, "task");
    if (task.fragment().payload().format()
        != FqpFragmentPayload.Format.SUBSTRAIT_BINARY) {
      throw new FqpExecutionException("DataFusion task requires binary Substrait");
    }
    final FqpDestination destination = config.destination(
        task.destination().sourceId()).orElseThrow(() -> new FqpExecutionException(
            "unknown destination " + task.destination().sourceId()));
    final URI endpoint = destination.capabilities().executionEndpoint()
        .orElseThrow(() -> new FqpExecutionException(
            "no DataFusion execution endpoint for " + destination.sourceId()));
    return DataFusionFragmentExecutor.executeArrow(endpoint,
        task.fragment().payload().bytes());
  }
}
