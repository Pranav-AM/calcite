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
package org.apache.calcite.adapter.fqp.rules;

import org.apache.calcite.adapter.fqp.FqpConvention;
import org.apache.calcite.adapter.fqp.FqpExchangeRequirement;
import org.apache.calcite.adapter.fqp.FqpFragment;
import org.apache.calcite.adapter.fqp.cost.DataFusionCostEstimate;
import org.apache.calcite.adapter.fqp.rel.FqpRemoteFragmentRel;
import org.apache.calcite.adapter.fqp.serialization.FqpFragmentSerializer;
import org.apache.calcite.adapter.fqp.serialization.FqpSerializationException;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;

/** Produces an opaque remote inner-join candidate for one destination. */
public final class FqpJoinRule extends ConverterRule {
  private final FqpConvention convention;
  private final FqpFragmentSerializer serializer;

  private FqpJoinRule(Config config, FqpConvention convention) {
    super(config);
    this.convention = convention;
    this.serializer = new FqpFragmentSerializer(convention.planningContext().config());
  }

  public static FqpJoinRule create(FqpConvention convention) {
    return Config.INSTANCE
        .withConversion(LogicalJoin.class, Convention.NONE, convention, "FqpJoinRule")
        .withRuleFactory(config -> new FqpJoinRule(config, convention))
        .toRule(FqpJoinRule.class);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    final LogicalJoin join = (LogicalJoin) rel;
    FqpFragment fragment;
    try {
      fragment = serializer.serialize(join, convention.destination());
    } catch (FqpSerializationException e) {
      return null;
    }
    // Match the PostgreSQL prototype's candidate scope: only a participating
    // non-coordinator source can be the destination for an intermediate join.
    if (!fragment.sourceIds().contains(convention.destination().sourceId())
        || convention.destination().sourceId().equals(
            convention.planningContext().config().coordinatorSourceId())) {
      return null;
    }
    final DataFusionCostEstimate estimate;
    try {
      estimate = convention.planningContext().dataFusionCostModel()
          .estimate(join, convention.destination().sourceId());
    } catch (IllegalArgumentException e) {
      return null;
    }
    final FqpExchangeRequirement resultExchange = new FqpExchangeRequirement(
        convention.destination().sourceId(), convention.planningContext().config()
            .coordinatorSourceId(),
        estimate.rowCount() * estimate.rowWidth());
    fragment = new FqpFragment(fragment.destination(), fragment.payload(), fragment.rowType(),
        fragment.sourceIds(), Collections.singletonList(resultExchange));
    final RelTraitSet traitSet = join.getTraitSet().replace(convention);
    return new FqpRemoteFragmentRel(join.getCluster(), traitSet, join.getRowType(), fragment,
        estimate);
  }
}
