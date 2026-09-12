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
package org.apache.calcite.adapter.fqp.cost;

import org.apache.calcite.adapter.fqp.RemoteCostEstimate;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;

/** Maps a remote EXPLAIN estimate into Calcite's rows/CPU/I/O cost dimensions. */
public final class FqpCostModel {
  private FqpCostModel() {
  }

  public static RelOptCost toRelOptCost(RelOptPlanner planner,
      RemoteCostEstimate estimate, double movementCostFactor) {
    // RelOptCost has no startup dimension. Preserve total remote work as CPU
    // and model the explicit data-movement term as I/O.
    return planner.getCostFactory().makeCost(estimate.getRowCount(),
        Math.max(0D, estimate.getTotalCost()),
        estimate.movementCost(movementCostFactor));
  }

  /** Converts a locally computed DataFusion estimate into Calcite dimensions. */
  public static RelOptCost toRelOptCost(RelOptPlanner planner,
      DataFusionCostEstimate estimate) {
    return planner.getCostFactory().makeCost(estimate.rowCount(), estimate.cpu(),
        estimate.io() + estimate.network());
  }
}
