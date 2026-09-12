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
import org.apache.calcite.adapter.fqp.FqpFragment;
import org.apache.calcite.adapter.fqp.cost.DataFusionCostEstimate;
import org.apache.calcite.adapter.fqp.rel.FqpRemoteFragmentRel;
import org.apache.calcite.adapter.fqp.serialization.FqpFragmentSerializer;
import org.apache.calcite.adapter.fqp.serialization.FqpSerializationException;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Converts a supported complete logical subtree into one opaque FQP fragment. */
public final class FqpPushdownRule extends ConverterRule {
  private final FqpConvention convention;
  private final FqpFragmentSerializer serializer;

  private FqpPushdownRule(Config config, FqpConvention convention) {
    super(config);
    this.convention = convention;
    this.serializer = new FqpFragmentSerializer(convention.planningContext().config());
  }

  public static FqpPushdownRule create(Class<? extends RelNode> relClass,
      String name, FqpConvention convention) {
    return Config.INSTANCE
        .withConversion(relClass, Convention.NONE, convention, name)
        .withRuleFactory(config -> new FqpPushdownRule(config, convention))
        .toRule(FqpPushdownRule.class);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    final FqpFragment fragment;
    try {
      fragment = serializer.serialize(rel, convention.destination());
    } catch (FqpSerializationException e) {
      return null;
    }
    if (!fragment.sourceIds().contains(convention.destination().sourceId())) {
      return null;
    }
    final DataFusionCostEstimate estimate;
    try {
      estimate = convention.planningContext().dataFusionCostModel()
          .estimate(rel, convention.destination().sourceId());
    } catch (IllegalArgumentException e) {
      return null;
    }
    final RelTraitSet traitSet = rel.getTraitSet().replace(convention);
    return new FqpRemoteFragmentRel(rel.getCluster(), traitSet, rel.getRowType(), fragment,
        estimate);
  }
}
