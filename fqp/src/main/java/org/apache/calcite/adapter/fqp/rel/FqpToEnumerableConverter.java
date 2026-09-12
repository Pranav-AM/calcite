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

import org.apache.calcite.adapter.fqp.FqpFragment;
import org.apache.calcite.adapter.fqp.FqpFragmentExecutor;
import org.apache.calcite.adapter.fqp.FqpRuntime;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/** Enumerable boundary that delegates selected FQP fragments to a configured executor. */
public final class FqpToEnumerableConverter extends ConverterImpl implements EnumerableRel {
  private final FqpFragmentExecutor executor;

  public FqpToEnumerableConverter(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, FqpFragmentExecutor executor) {
    super(cluster, ConventionTraitDef.INSTANCE, traitSet, input);
    this.executor = executor;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FqpToEnumerableConverter(getCluster(), traitSet, sole(inputs), executor);
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    final RelOptCost cost = super.computeSelfCost(planner, mq);
    return cost == null ? null : cost.multiplyBy(0.1D);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    if (!(getInput() instanceof FqpRemoteFragmentRel)) {
      throw new UnsupportedOperationException(
          "FQP execution requires an opaque FQP fragment");
    }
    final FqpRemoteFragmentRel input = (FqpRemoteFragmentRel) getInput();
    final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(),
        pref.prefer(JavaRowFormat.ARRAY));
    final BlockBuilder builder = new BlockBuilder();
    final Expression rows = builder.append("fqpRows", Expressions.call(FqpRuntime.class,
        "execute", implementor.stash(executor, FqpFragmentExecutor.class),
        implementor.stash(input.fragment(), FqpFragment.class)));
    builder.add(rows);
    return implementor.result(physType, builder.toBlock());
  }
}
