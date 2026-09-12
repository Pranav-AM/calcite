/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Validated task DAG for one selected distributed FQP plan. */
public final class FqpDistributedPlan {
  private final FqpPlanningConfig config;
  private final Map<FqpTaskId, FqpTask> tasks;
  private final Map<FqpExchangeId, FqpExchange> exchanges;
  private final FqpTaskId rootTaskId;
  private final List<FqpTask> topologicalTasks;

  public FqpDistributedPlan(FqpPlanningConfig config, Collection<FqpTask> tasks,
      Collection<FqpExchange> exchanges, FqpTaskId rootTaskId) {
    this.config = Objects.requireNonNull(config, "config");
    this.tasks = indexTasks(tasks);
    this.exchanges = indexExchanges(exchanges);
    this.rootTaskId = Objects.requireNonNull(rootTaskId, "rootTaskId");
    if (!this.tasks.containsKey(rootTaskId)) {
      throw new IllegalArgumentException("unknown root task: " + rootTaskId);
    }
    validateTasks();
    validateExchanges();
    this.topologicalTasks = Collections.unmodifiableList(validateAndSort());
  }

  public FqpTask rootTask() {
    return tasks.get(rootTaskId);
  }

  public FqpTask task(FqpTaskId taskId) {
    return requireTask(taskId);
  }

  public Collection<FqpTask> tasks() {
    return tasks.values();
  }

  public Collection<FqpExchange> exchanges() {
    return exchanges.values();
  }

  /** Tasks ordered so every exchange producer precedes its consumer. */
  public List<FqpTask> topologicalTasks() {
    return topologicalTasks;
  }

  public List<FqpExchange> incomingExchanges(FqpTaskId taskId) {
    requireTask(taskId);
    final List<FqpExchange> result = new ArrayList<>();
    for (FqpExchange exchange : exchanges.values()) {
      if (exchange.consumerTaskId().equals(taskId)) {
        result.add(exchange);
      }
    }
    return Collections.unmodifiableList(result);
  }

  public List<FqpExchange> outgoingExchanges(FqpTaskId taskId) {
    requireTask(taskId);
    final List<FqpExchange> result = new ArrayList<>();
    for (FqpExchange exchange : exchanges.values()) {
      if (exchange.producerTaskId().equals(taskId)) {
        result.add(exchange);
      }
    }
    return Collections.unmodifiableList(result);
  }

  private void validateTasks() {
    for (FqpTask task : tasks.values()) {
      final String destinationId = task.destination().sourceId();
      if (!config.destination(destinationId).isPresent()) {
        throw new IllegalArgumentException("unknown task destination: " + destinationId);
      }
      for (List<String> table : task.localTables()) {
        final FqpTablePlacement placement = config.placement(table)
            .orElseThrow(() -> new IllegalArgumentException("no placement for local table "
                + table));
        if (!placement.sourceId().equals(destinationId)) {
          throw new IllegalArgumentException("task " + task.id() + " treats " + table
              + " as local at " + destinationId + " but its owner is "
              + placement.sourceId());
        }
      }
    }
  }

  private void validateExchanges() {
    final Map<FqpTaskId, Set<List<String>>> consumerNames = new HashMap<>();
    for (FqpExchange exchange : exchanges.values()) {
      final FqpTask producer = requireTask(exchange.producerTaskId());
      final FqpTask consumer = requireTask(exchange.consumerTaskId());
      if (producer.id().equals(consumer.id())) {
        throw new IllegalArgumentException("exchange cannot connect a task to itself: "
            + exchange.id());
      }
      if (!producer.destination().sourceId().equals(exchange.sourceId())) {
        throw new IllegalArgumentException("exchange source does not match producer: "
            + exchange.id());
      }
      if (!consumer.destination().sourceId().equals(exchange.destinationId())) {
        throw new IllegalArgumentException("exchange destination does not match consumer: "
            + exchange.id());
      }
      if (!producer.fragment().rowType().equals(exchange.rowType())) {
        throw new IllegalArgumentException("exchange schema does not match producer: "
            + exchange.id());
      }
      final Set<List<String>> names = consumerNames.computeIfAbsent(consumer.id(),
          ignored -> new LinkedHashSet<>());
      if (!names.add(exchange.temporaryInputName())) {
        throw new IllegalArgumentException("duplicate temporary input at task "
            + consumer.id() + ": " + exchange.temporaryInputName());
      }
    }
  }

  private List<FqpTask> validateAndSort() {
    final Map<FqpTaskId, List<FqpTaskId>> dependencies = new LinkedHashMap<>();
    for (FqpTaskId taskId : tasks.keySet()) {
      dependencies.put(taskId, new ArrayList<>());
    }
    for (FqpExchange exchange : exchanges.values()) {
      dependencies.get(exchange.consumerTaskId()).add(exchange.producerTaskId());
    }
    final Map<FqpTaskId, Integer> states = new HashMap<>();
    final List<FqpTask> ordered = new ArrayList<>();
    for (FqpTaskId taskId : tasks.keySet()) {
      visit(taskId, dependencies, states, ordered);
    }
    final Set<FqpTaskId> ancestors = new LinkedHashSet<>();
    collectAncestors(rootTaskId, dependencies, ancestors);
    if (ancestors.size() != tasks.size()) {
      final Set<FqpTaskId> orphaned = new LinkedHashSet<>(tasks.keySet());
      orphaned.removeAll(ancestors);
      throw new IllegalArgumentException("tasks do not contribute to root " + rootTaskId
          + ": " + orphaned);
    }
    return ordered;
  }

  private void visit(FqpTaskId taskId, Map<FqpTaskId, List<FqpTaskId>> dependencies,
      Map<FqpTaskId, Integer> states, List<FqpTask> ordered) {
    final int state = states.getOrDefault(taskId, 0);
    if (state == 1) {
      throw new IllegalArgumentException("cycle detected at task " + taskId);
    }
    if (state == 2) {
      return;
    }
    states.put(taskId, 1);
    for (FqpTaskId dependency : dependencies.get(taskId)) {
      visit(dependency, dependencies, states, ordered);
    }
    states.put(taskId, 2);
    ordered.add(tasks.get(taskId));
  }

  private static void collectAncestors(FqpTaskId taskId,
      Map<FqpTaskId, List<FqpTaskId>> dependencies, Set<FqpTaskId> result) {
    if (!result.add(taskId)) {
      return;
    }
    for (FqpTaskId dependency : dependencies.get(taskId)) {
      collectAncestors(dependency, dependencies, result);
    }
  }

  private FqpTask requireTask(FqpTaskId id) {
    final FqpTask task = tasks.get(id);
    if (task == null) {
      throw new IllegalArgumentException("unknown exchange task: " + id);
    }
    return task;
  }

  private static Map<FqpTaskId, FqpTask> indexTasks(Collection<FqpTask> values) {
    Objects.requireNonNull(values, "tasks");
    final Map<FqpTaskId, FqpTask> result = new LinkedHashMap<>();
    for (FqpTask value : values) {
      final FqpTask task = Objects.requireNonNull(value, "task");
      if (result.put(task.id(), task) != null) {
        throw new IllegalArgumentException("duplicate task: " + task.id());
      }
    }
    if (result.isEmpty()) {
      throw new IllegalArgumentException("distributed plan must contain a task");
    }
    return Collections.unmodifiableMap(result);
  }

  private static Map<FqpExchangeId, FqpExchange> indexExchanges(
      Collection<FqpExchange> values) {
    Objects.requireNonNull(values, "exchanges");
    final Map<FqpExchangeId, FqpExchange> result = new LinkedHashMap<>();
    for (FqpExchange value : values) {
      final FqpExchange exchange = Objects.requireNonNull(value, "exchange");
      if (result.put(exchange.id(), exchange) != null) {
        throw new IllegalArgumentException("duplicate exchange: " + exchange.id());
      }
    }
    return Collections.unmodifiableMap(result);
  }
}
