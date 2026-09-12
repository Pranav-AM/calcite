/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpDistributedPlan;
import org.apache.calcite.adapter.fqp.FqpExchange;
import org.apache.calcite.adapter.fqp.FqpExecutionException;
import org.apache.calcite.adapter.fqp.FqpTask;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Executes a validated distributed plan in dependency order. */
public final class FqpDistributedPlanExecutor {
  private final FqpTaskClient taskClient;
  private final FqpExchangeTransport exchangeTransport;

  public FqpDistributedPlanExecutor(FqpTaskClient taskClient,
      FqpExchangeTransport exchangeTransport) {
    this.taskClient = Objects.requireNonNull(taskClient, "taskClient");
    this.exchangeTransport = Objects.requireNonNull(exchangeTransport,
        "exchangeTransport");
  }

  /** Returns the root task's Arrow IPC stream. */
  public byte[] execute(FqpDistributedPlan plan) {
    Objects.requireNonNull(plan, "plan");
    final Map<org.apache.calcite.adapter.fqp.FqpTaskId, byte[]> outputs =
        new LinkedHashMap<>();
    for (FqpTask task : plan.topologicalTasks()) {
      final byte[] output = requireOutput(task, taskClient.execute(task));
      outputs.put(task.id(), output);
      for (FqpExchange exchange : plan.outgoingExchanges(task.id())) {
        final FqpTask consumer = plan.task(exchange.consumerTaskId());
        exchangeTransport.transfer(exchange, consumer.destination(), output);
      }
    }
    final byte[] root = outputs.get(plan.rootTask().id());
    return Arrays.copyOf(root, root.length);
  }

  private static byte[] requireOutput(FqpTask task, byte[] output) {
    if (output == null || output.length == 0) {
      throw new FqpExecutionException("task returned an empty Arrow stream: " + task.id());
    }
    return Arrays.copyOf(output, output.length);
  }
}
