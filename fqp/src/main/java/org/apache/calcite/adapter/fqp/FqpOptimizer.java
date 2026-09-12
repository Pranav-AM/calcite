/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.fqp.rules.FqpPushdownRule;
import org.apache.calcite.adapter.fqp.rules.FqpRules;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Registers FQP alternatives and delegates plan selection to Calcite. */
public final class FqpOptimizer {
  private final FqpPlanningContext context;
  private final List<FqpConvention> conventions;

  public FqpOptimizer(FqpPlanningContext context,
      Collection<FqpDestination> destinations) {
    this.context = Objects.requireNonNull(context, "context");
    Objects.requireNonNull(destinations, "destinations");
    final List<FqpConvention> values = new ArrayList<>();
    for (FqpDestination destination : destinations) {
      values.add(new FqpConvention(
          Objects.requireNonNull(destination, "destination"), context));
    }
    if (values.isEmpty()) {
      throw new IllegalArgumentException("optimizer requires an FQP destination");
    }
    this.conventions = Collections.unmodifiableList(values);
  }

  /** Finds Calcite's lowest-cost executable plan for {@code logicalRoot}. */
  public RelNode optimize(RelNode logicalRoot) {
    Objects.requireNonNull(logicalRoot, "logicalRoot");
    final RelOptPlanner planner = logicalRoot.getCluster().getPlanner();
    for (FqpConvention convention : conventions) {
      final List<RelOptRule> rules = FqpRules.rules(convention);
      for (RelOptRule rule : rules) {
        planner.addRule(rule);
      }
      // A complete remote root is an opaque leaf, so register it as an
      // equivalent expression before asking Volcano for Enumerable output.
      for (RelOptRule rule : rules) {
        if (rule instanceof FqpPushdownRule) {
          final RelNode remote = ((FqpPushdownRule) rule).convert(logicalRoot);
          if (remote != null) {
            planner.register(remote, logicalRoot);
          }
          break;
        }
      }
    }
    planner.setRoot(logicalRoot);
    final RelTraitSet desiredTraits = logicalRoot.getTraitSet()
        .replace(EnumerableConvention.INSTANCE);
    final RelNode converted = planner.changeTraits(logicalRoot, desiredTraits);
    planner.setRoot(converted);
    return planner.findBestExp();
  }

  public FqpPlanningContext context() {
    return context;
  }

  /** Applies logical rewrites and selects distributed placements using Volcano. */
  public FqpPlacement optimizeForDistribution(RelNode logicalRoot) {
    Objects.requireNonNull(logicalRoot, "logicalRoot");
    requireLogical(logicalRoot);
    final HepPlanner planner = new HepPlanner(new HepProgramBuilder()
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.FILTER_MERGE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .build());
    planner.setRoot(logicalRoot);
    return new FqpPlacementOptimizer(context).optimize(planner.findBestExp());
  }

  private static void requireLogical(RelNode node) {
    if (node.getConvention() != Convention.NONE) {
      throw new IllegalArgumentException("distributed optimization requires a logical tree: "
          + node.getRelTypeName());
    }
    for (RelNode input : node.getInputs()) {
      requireLogical(input);
    }
  }

  public List<FqpConvention> conventions() {
    return conventions;
  }
}
