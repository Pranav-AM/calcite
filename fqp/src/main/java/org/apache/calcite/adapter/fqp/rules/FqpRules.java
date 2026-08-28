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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;

import java.util.ArrayList;
import java.util.List;

/** Conversion-rule entry point, analogous to the mock-table pathlist hooks. */
public final class FqpRules {
  private FqpRules() {
  }

  /** Rules for a single FQP execution destination. */
  public static List<RelOptRule> rules(FqpConvention convention) {
    final List<RelOptRule> rules = new ArrayList<>();
    rules.add(FqpScanRule.create(convention));
    rules.add(FqpJoinRule.create(convention));
    rules.add(FqpPushdownRule.create(LogicalFilter.class, "FqpFilterRule", convention));
    rules.add(FqpPushdownRule.create(LogicalProject.class, "FqpProjectRule", convention));
    rules.add(FqpPushdownRule.create(LogicalAggregate.class, "FqpAggregateRule", convention));
    rules.add(FqpPushdownRule.create(LogicalSort.class, "FqpSortRule", convention));
    if (convention.planningContext().config().allowRemoteRoot()) {
      rules.add(FqpToEnumerableConverterRule.create(convention));
    }
    return rules;
  }
}
