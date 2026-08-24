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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Produces one opaque FQP scan candidate at a configured destination. */
public final class FqpScanRule extends ConverterRule {
  private final FqpConvention convention;
  private final FqpFragmentSerializer serializer;

  private FqpScanRule(Config config, FqpConvention convention) {
    super(config);
    this.convention = convention;
    this.serializer = new FqpFragmentSerializer(convention.planningContext().config());
  }

  public static FqpScanRule create(FqpConvention convention) {
    return Config.INSTANCE
        .withConversion(LogicalTableScan.class, Convention.NONE, convention, "FqpScanRule")
        .withRuleFactory(config -> new FqpScanRule(config, convention))
        .toRule(FqpScanRule.class);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    final LogicalTableScan scan = (LogicalTableScan) rel;
    final FqpFragment fragment;
    try {
      fragment = serializer.serialize(scan, convention.destination());
    } catch (FqpSerializationException e) {
      return null;
    }
    if (!fragment.sourceIds().contains(convention.destination().sourceId())) {
      return null;
    }
    final RemoteCostResult result = convention.planningContext().remoteCostClient()
        .explain(new RemoteCostRequest(convention.planningContext().config()
            .coordinatorSourceId(), convention.destination().sourceId(), fragment.payload(),
            convention.planningContext().config().remoteCostTimeout()));
    if (!result.isSuccess()) {
      return null;
    }
    final RelTraitSet traitSet = scan.getTraitSet().replace(convention);
    return new FqpRemoteFragmentRel(scan.getCluster(), traitSet, scan.getRowType(),
        fragment, result.estimate().get(),
        convention.planningContext().config().movementCostFactor());
  }
}
