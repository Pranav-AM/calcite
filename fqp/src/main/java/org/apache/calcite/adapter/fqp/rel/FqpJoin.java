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
package org.apache.calcite.adapter.fqp.rel;

import org.apache.calcite.adapter.fqp.RemoteCostEstimate;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

/** FQP alternative for a remote join candidate. */
public class FqpJoin extends Join implements FqpRel {
  private final RemoteCostEstimate estimate;

  public FqpJoin(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints,
      RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet,
      JoinRelType joinType, RemoteCostEstimate estimate) {
    super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    this.estimate = estimate;
  }

  @Override public FqpJoin copy(RelTraitSet traitSet, RexNode condition,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new FqpJoin(getCluster(), traitSet, getHints(), left, right, condition,
        variablesSet, joinType, estimate);
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return planner.getCostFactory().makeCost(estimate.getRowCount(), estimate.cpu(),
        estimate.transferCost(1D));
  }

  /** TODO: Store the destination source and serialized remote join fragment. */
}
