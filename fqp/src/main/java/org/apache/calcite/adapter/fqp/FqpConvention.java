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
import org.apache.calcite.plan.RelOptPlanner;

/** Calling convention for one FQP destination source.
 *
 * <p>This convention only identifies an execution location. Rules and remote
 * costing are intentionally added in later milestones.</p>
 */
public class FqpConvention extends Convention.Impl {
  private final FqpDestination destination;
  private final FqpPlanningContext planningContext;

  public FqpConvention(FqpDestination destination,
      FqpPlanningContext planningContext) {
    super("FQP." + destination.sourceId(), FqpRel.class);
    this.destination = destination;
    this.planningContext = planningContext;
  }

  public FqpDestination destination() {
    return destination;
  }

  public FqpPlanningContext planningContext() {
    return planningContext;
  }

  @Override public void register(RelOptPlanner planner) {
    for (org.apache.calcite.plan.RelOptRule rule : FqpRules.rules(this)) {
      planner.addRule(rule);
    }
  }
}
