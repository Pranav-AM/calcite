/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpDeployment;
import org.apache.calcite.adapter.fqp.FqpDeploymentConfigLoader;
import org.apache.calcite.adapter.fqp.FqpDistributedPlan;
import org.apache.calcite.adapter.fqp.FqpDistributedPlanLowerer;
import org.apache.calcite.adapter.fqp.FqpExecutionException;
import org.apache.calcite.adapter.fqp.FqpOptimizer;
import org.apache.calcite.adapter.fqp.FqpPlacement;
import org.apache.calcite.adapter.fqp.FqpPlanningContext;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;

import java.nio.file.Path;
import java.util.Objects;

/** File-configured distributed execution boundary for Calcite logical plans. */
public final class FqpQueryExecutor {
  private final FqpPlanningContext context;
  private final FqpTaskClient taskClient;
  private final FqpExchangeTransport exchangeTransport;

  /** Uses HTTP for tasks and a scoped Arrow Flight transport for each execution. */
  public FqpQueryExecutor(FqpDeployment deployment) {
    this(deployment.planningContext(), new DataFusionTaskClient(deployment.planningConfig()),
        (exchange, destination, stream) -> {
          try (ArrowFlightExchangeTransport transport = new ArrowFlightExchangeTransport()) {
            transport.transfer(exchange, destination, stream);
          }
        });
  }

  /** Allows embedding applications to supply task and exchange transports. */
  public FqpQueryExecutor(FqpPlanningContext context, FqpTaskClient taskClient,
      FqpExchangeTransport exchangeTransport) {
    this.context = Objects.requireNonNull(context, "context");
    this.taskClient = Objects.requireNonNull(taskClient, "taskClient");
    this.exchangeTransport = Objects.requireNonNull(exchangeTransport, "exchangeTransport");
  }

  public static FqpQueryExecutor fromDeployment(Path path) {
    return new FqpQueryExecutor(FqpDeploymentConfigLoader.loadDeployment(path));
  }

  /** Optimizes and lowers without contacting any worker. */
  public FqpDistributedPlan prepare(RelNode logicalRoot) {
    final FqpPlacement selected = new FqpOptimizer(context, context.config().destinations())
        .optimizeForDistribution(logicalRoot);
    final FqpDistributedPlan plan = new FqpDistributedPlanLowerer(context).lower(selected);
    final String rootSource = plan.rootTask().destination().sourceId();
    if (!context.config().allowRemoteRoot()
        && !rootSource.equals(context.config().coordinatorSourceId())) {
      throw new FqpExecutionException("deployment disallows a remote root at " + rootSource);
    }
    // Validate statistics for the whole selected tree, including a single-owner plan.
    context.dataFusionCostModel().estimate(selected.root(), rootSource);
    return plan;
  }

  /** Executes the selected task DAG and returns materialized Calcite array rows. */
  public Enumerable<Object[]> execute(RelNode logicalRoot) {
    return execute(prepare(logicalRoot));
  }

  /** Executes a plan returned by {@link #prepare} without optimizing it again. */
  public Enumerable<Object[]> execute(FqpDistributedPlan plan) {
    Objects.requireNonNull(plan, "plan");
    final byte[] stream = new FqpDistributedPlanExecutor(taskClient, exchangeTransport)
        .execute(plan);
    return Linq4j.asEnumerable(ArrowRows.read(stream, plan.rootTask().fragment().rowType()));
  }
}
