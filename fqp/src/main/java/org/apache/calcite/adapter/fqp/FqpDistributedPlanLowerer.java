/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.adapter.fqp.cost.DataFusionCostEstimate;
import org.apache.calcite.adapter.fqp.serialization.SubstraitFragmentSerializer;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Lowers a logical tree into maximal destination-local tasks and typed exchanges. */
public final class FqpDistributedPlanLowerer {
  private final FqpPlanningContext context;
  private final SubstraitFragmentSerializer serializer;
  private final Map<RelNode, String> destinations = new IdentityHashMap<>();
  private final List<FqpTask> tasks = new ArrayList<>();
  private final List<FqpExchange> exchanges = new ArrayList<>();
  private int nextTask;
  private int nextExchange;
  private int nextInput;
  private String inputPrefix = "";

  public FqpDistributedPlanLowerer(FqpPlanningContext context) {
    this.context = Objects.requireNonNull(context, "context");
    this.serializer = new SubstraitFragmentSerializer(context.config());
  }

  /** Creates a distributed plan without query- or table-specific rules. */
  public FqpDistributedPlan lower(RelNode root) {
    return lower(new FqpPlacementOptimizer(context).optimize(root));
  }

  /** Preserves the placements selected by Calcite; performs no placement search. */
  public FqpDistributedPlan lower(FqpPlacement placement) {
    Objects.requireNonNull(placement, "placement");
    destinations.clear();
    destinations.putAll(placement.destinations());
    tasks.clear();
    exchanges.clear();
    nextTask = 0;
    nextExchange = 0;
    nextInput = 0;
    inputPrefix = "fqp_" + UUID.randomUUID().toString().replace("-", "") + "_";
    final FqpTask rootTask = buildTask(placement.root());
    return new FqpDistributedPlan(context.config(), tasks, exchanges, rootTask.id());
  }

  private FqpTask buildTask(RelNode root) {
    final String destinationId = destinations.get(root);
    final Map<RelNode, List<String>> temporaryInputs = new IdentityHashMap<>();
    final Map<RelNode, FqpTask> producers = new IdentityHashMap<>();
    findBoundaries(root, destinationId, temporaryInputs, producers);
    final FqpDestination destination = context.config().destination(destinationId)
        .orElseThrow(() -> new IllegalArgumentException(
            "unknown destination " + destinationId));
    final FqpFragment fragment = serializer.serialize(root, destination, temporaryInputs);
    final FqpTask task = new FqpTask(new FqpTaskId("task-" + nextTask++), fragment,
        localTables(root, temporaryInputs));
    tasks.add(task);
    for (Map.Entry<RelNode, FqpTask> entry : producers.entrySet()) {
      final RelNode boundary = entry.getKey();
      final FqpTask producer = entry.getValue();
      final DataFusionCostEstimate estimate = context.dataFusionCostModel()
          .estimate(boundary, producer.destination().sourceId());
      exchanges.add(new FqpExchange(new FqpExchangeId("exchange-" + nextExchange++),
          producer.id(), task.id(), producer.destination().sourceId(), destinationId,
          boundary.getRowType(), temporaryInputs.get(boundary), estimate.rowCount(),
          estimate.rowCount() * estimate.rowWidth()));
    }
    return task;
  }

  private void findBoundaries(RelNode node, String destinationId,
      Map<RelNode, List<String>> temporaryInputs, Map<RelNode, FqpTask> producers) {
    for (RelNode input : node.getInputs()) {
      if (!destinationId.equals(destinations.get(input))) {
        if (temporaryInputs.containsKey(input)) {
          continue;
        }
        final FqpTask producer = buildTask(input);
        final List<String> name = Collections.singletonList(
            inputPrefix + nextInput++);
        temporaryInputs.put(input, name);
        producers.put(input, producer);
      } else {
        findBoundaries(input, destinationId, temporaryInputs, producers);
      }
    }
  }

  private Set<List<String>> localTables(RelNode root,
      Map<RelNode, List<String>> temporaryInputs) {
    final Set<List<String>> result = new LinkedHashSet<>();
    collectTables(root, temporaryInputs, result);
    return result;
  }

  private void collectTables(RelNode node, Map<RelNode, List<String>> temporaryInputs,
      Set<List<String>> result) {
    if (temporaryInputs.containsKey(node)) {
      return;
    }
    if (node instanceof TableScan) {
      result.add(((TableScan) node).getTable().getQualifiedName());
    }
    for (RelNode input : node.getInputs()) {
      collectTables(input, temporaryInputs, result);
    }
  }

}
