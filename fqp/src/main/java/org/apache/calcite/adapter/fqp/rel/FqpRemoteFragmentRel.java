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

import org.apache.calcite.adapter.fqp.cost.FqpCostModel;
import org.apache.calcite.adapter.fqp.FqpFragment;
import org.apache.calcite.adapter.fqp.RemoteCostEstimate;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/** Opaque, remotely costed FQP fragment.
 *
 * <p>The fragment has no relational children. Its remote estimate is therefore
 * included exactly once, unlike a conventional join with separately costed
 * remote scan children.</p>
 */
public final class FqpRemoteFragmentRel extends AbstractRelNode implements FqpRel {
  private final FqpFragment fragment;
  private final RemoteCostEstimate estimate;
  private final double movementCostFactor;
  private final RelDataType rowType;

  public FqpRemoteFragmentRel(RelOptCluster cluster, RelTraitSet traitSet,
      RelDataType rowType, FqpFragment fragment, RemoteCostEstimate estimate,
      double movementCostFactor) {
    super(cluster, traitSet);
    this.rowType = Objects.requireNonNull(rowType, "rowType");
    this.fragment = Objects.requireNonNull(fragment, "fragment");
    this.estimate = Objects.requireNonNull(estimate, "estimate");
    this.movementCostFactor = movementCostFactor;
  }

  public FqpFragment fragment() {
    return fragment;
  }

  public RemoteCostEstimate estimate() {
    return estimate;
  }

  @Override protected RelDataType deriveRowType() {
    return rowType;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("destination", fragment.destination().sourceId())
        .item("payloadFormat", fragment.payload().format())
        .item("remoteTotalCost", estimate.getTotalCost())
        .item("remoteStartupCost", estimate.getStartupCost())
        .item("exchanges", fragment.exchanges());
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return FqpCostModel.toRelOptCost(planner, estimate, movementCostFactor);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    if (!inputs.isEmpty()) {
      throw new IllegalArgumentException("FQP fragments do not have relational inputs");
    }
    return new FqpRemoteFragmentRel(getCluster(), traitSet, rowType, fragment,
        estimate, movementCostFactor);
  }
}
