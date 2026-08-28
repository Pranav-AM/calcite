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
import org.apache.calcite.adapter.fqp.rel.FqpToEnumerableConverter;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Converts an FQP plan into the explicit plan-only enumerable boundary. */
public final class FqpToEnumerableConverterRule extends ConverterRule {
  private final FqpConvention convention;

  private FqpToEnumerableConverterRule(Config config, FqpConvention convention) {
    super(config);
    this.convention = convention;
  }

  public static FqpToEnumerableConverterRule create(FqpConvention convention) {
    return Config.INSTANCE
        .withConversion(RelNode.class, convention, EnumerableConvention.INSTANCE,
            "FqpToEnumerableConverterRule")
        .withRuleFactory(config -> new FqpToEnumerableConverterRule(config, convention))
        .toRule(FqpToEnumerableConverterRule.class);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    final RelTraitSet traitSet = rel.getTraitSet().replace(getOutTrait());
    return new FqpToEnumerableConverter(rel.getCluster(), traitSet, rel,
        convention.planningContext().fragmentExecutor());
  }
}
