/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.adapter.fqp.cost.DataFusionCostModel;
import org.apache.calcite.adapter.fqp.cost.DataFusionCostParameters;
import org.apache.calcite.adapter.fqp.cost.DataFusionStatisticsSnapshot;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Query-independent deployment configuration and local planning data. */
public final class FqpDeployment {
  private final FqpPlanningConfig planningConfig;
  private final Map<String, DataFusionStatisticsSnapshot> statistics;
  private final Map<String, DataFusionCostParameters> costParameters;

  public FqpDeployment(FqpPlanningConfig planningConfig,
      Map<String, DataFusionStatisticsSnapshot> statistics,
      Map<String, DataFusionCostParameters> costParameters) {
    this.planningConfig = Objects.requireNonNull(planningConfig, "planningConfig");
    this.statistics = copy(statistics, "statistics");
    this.costParameters = copy(costParameters, "costParameters");
    validateSources(this.statistics, "statistics");
    validateSources(this.costParameters, "cost parameters");
    this.statistics.forEach((source, snapshot) -> {
      if (!source.equals(snapshot.sourceId())) {
        throw new IllegalArgumentException("statistics source does not match key " + source);
      }
    });
  }

  public FqpPlanningConfig planningConfig() {
    return planningConfig;
  }

  public Map<String, DataFusionStatisticsSnapshot> statistics() {
    return statistics;
  }

  public Map<String, DataFusionCostParameters> costParameters() {
    return costParameters;
  }

  /** Creates a planning context using only locally loaded deployment data. */
  public FqpPlanningContext planningContext() {
    return new FqpPlanningContext(planningConfig,
        new DataFusionCostModel(planningConfig, statistics, costParameters));
  }

  private void validateSources(Map<String, ?> values, String description) {
    for (String sourceId : values.keySet()) {
      if (!planningConfig.destination(sourceId).isPresent()) {
        throw new IllegalArgumentException(description
            + " reference unknown destination " + sourceId);
      }
    }
  }

  private static <T> Map<String, T> copy(Map<String, T> values, String name) {
    Objects.requireNonNull(values, name);
    final Map<String, T> result = new LinkedHashMap<>();
    for (Map.Entry<String, T> entry : values.entrySet()) {
      result.put(FqpDestination.requireNonBlank(entry.getKey(), "sourceId"),
          Objects.requireNonNull(entry.getValue(), name + " value"));
    }
    return Collections.unmodifiableMap(result);
  }
}
