/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.cost;

import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpPlanningConfig;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Versioned, immutable statistics snapshot obtained before optimization. */
public final class DataFusionStatisticsSnapshot {
  private final String sourceId;
  private final String version;
  private final Instant loadedAt;
  private final Map<List<String>, DataFusionTableStatistics> tables;

  public DataFusionStatisticsSnapshot(String sourceId, String version, Instant loadedAt,
      Map<List<String>, DataFusionTableStatistics> tables) {
    this.sourceId = FqpDestination.requireNonBlank(sourceId, "sourceId");
    this.version = FqpDestination.requireNonBlank(version, "version");
    this.loadedAt = Objects.requireNonNull(loadedAt, "loadedAt");
    Objects.requireNonNull(tables, "tables");
    final Map<List<String>, DataFusionTableStatistics> copy = new LinkedHashMap<>();
    for (Map.Entry<List<String>, DataFusionTableStatistics> entry : tables.entrySet()) {
      copy.put(FqpPlanningConfig.copyName(entry.getKey()),
          Objects.requireNonNull(entry.getValue(), "statistics"));
    }
    this.tables = Collections.unmodifiableMap(copy);
  }

  public String sourceId() {
    return sourceId;
  }

  public String version() {
    return version;
  }

  public Instant loadedAt() {
    return loadedAt;
  }

  public Optional<DataFusionTableStatistics> table(List<String> logicalName) {
    return Optional.ofNullable(tables.get(FqpPlanningConfig.copyName(logicalName)));
  }
}
