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

import org.apache.calcite.adapter.fqp.cost.DataFusionCostModel;

import java.util.Objects;

/** Per-optimization-request FQP state.
 *
 * <p>All costing exposed by this context is local and uses a statistics
 * snapshot loaded before optimization.</p>
 */
public final class FqpPlanningContext {
  private final FqpPlanningConfig config;
  private final FqpPlanningMetrics metrics;
  private final FqpFragmentExecutor fragmentExecutor;
  private final DataFusionCostModel dataFusionCostModel;

  public FqpPlanningContext(FqpPlanningConfig config) {
    this(config, DataFusionCostModel.empty(config));
  }

  public FqpPlanningContext(FqpPlanningConfig config,
      DataFusionCostModel dataFusionCostModel) {
    this(config, dataFusionCostModel, fragment -> {
      throw new FqpExecutionException("no FQP fragment executor configured");
    });
  }

  public FqpPlanningContext(FqpPlanningConfig config,
      DataFusionCostModel dataFusionCostModel, FqpFragmentExecutor fragmentExecutor) {
    this.config = Objects.requireNonNull(config, "config");
    this.metrics = new FqpPlanningMetrics();
    this.fragmentExecutor = Objects.requireNonNull(fragmentExecutor, "fragmentExecutor");
    this.dataFusionCostModel = Objects.requireNonNull(dataFusionCostModel,
        "dataFusionCostModel");
  }

  public FqpPlanningConfig config() {
    return config;
  }

  public FqpFragmentExecutor fragmentExecutor() {
    return fragmentExecutor;
  }

  public FqpPlanningMetrics metrics() {
    return metrics;
  }

  /** Local, network-free DataFusion estimator for this planning context. */
  public DataFusionCostModel dataFusionCostModel() {
    return dataFusionCostModel;
  }
}
