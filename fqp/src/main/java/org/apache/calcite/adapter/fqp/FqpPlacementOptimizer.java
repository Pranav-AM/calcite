/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.adapter.fqp.cost.DataFusionCostEstimate;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Registers owner-local operators and explicit transfers in a Volcano memo. */
public final class FqpPlacementOptimizer {
  private final FqpPlanningContext context;

  public FqpPlacementOptimizer(FqpPlanningContext context) {
    this.context = Objects.requireNonNull(context, "context");
  }

  public FqpPlacement optimize(RelNode root) {
    return new Search(context, root).optimize();
  }

  /** One isolated memo per request; no worker calls or mutation of the SQL planner. */
  private static final class Search {
    private final FqpPlanningContext context;
    private final RelNode root;
    private final VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, null);
    private final RelOptCluster cluster;
    private final Map<String, Convention> conventions = new LinkedHashMap<>();
    private final Map<RelNode, Map<String, RelSubset>> memo = new IdentityHashMap<>();
    private int nextId;

    Search(FqpPlanningContext context, RelNode root) {
      this.context = context;
      this.root = Objects.requireNonNull(root, "root");
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
      cluster = RelOptCluster.create(planner, new RexBuilder(root.getCluster().getTypeFactory()));
      for (FqpDestination destination : context.config().destinations()) {
        if (destination.capabilities().supports(FqpFragmentPayload.Format.SUBSTRAIT_BINARY)
            && destination.capabilities().executionEndpoint().isPresent()) {
          conventions.put(destination.sourceId(),
              new Convention.Impl("FQP_AT_" + destination.sourceId(), RelNode.class));
        }
      }
    }

    FqpPlacement optimize() {
      final Map<String, RelSubset> alternatives = register(root);
      final Convention resultConvention = new Convention.Impl("FQP_RESULT", RelNode.class);
      RelSubset result = null;
      for (Map.Entry<String, RelSubset> entry : alternatives.entrySet()) {
        if (!context.config().allowRemoteRoot()
            && !entry.getKey().equals(context.config().coordinatorSourceId())) {
          continue;
        }
        final DataFusionCostEstimate estimate = context.dataFusionCostModel()
            .estimate(root, entry.getKey());
        final double responseCost = entry.getKey().equals(context.config().coordinatorSourceId())
            ? 0D : estimate.rowCount() * estimate.rowWidth()
                * context.config().movementCostFactor();
        final PlacedRel candidate = new PlacedRel(cluster, cluster.traitSetOf(resultConvention),
            root, "result", nextId++, true, responseCost,
            Collections.singletonList(entry.getValue()));
        result = planner.register(candidate, result);
      }
      if (result == null) {
        throw new IllegalArgumentException("no executable placement satisfies remote-root policy");
      }
      planner.setRoot(result);
      final PlacedRel selected = (PlacedRel) planner.findBestExp();
      final Map<RelNode, String> destinations = new IdentityHashMap<>();
      final RelNode logical = materialize((PlacedRel) selected.getInput(0), destinations);
      return new FqpPlacement(logical, destinations, totalCost(selected));
    }

    private Map<String, RelSubset> register(RelNode logical) {
      final Map<String, RelSubset> existing = memo.get(logical);
      if (existing != null) {
        return existing;
      }
      final List<Map<String, RelSubset>> children = new ArrayList<>();
      for (RelNode input : logical.getInputs()) {
        children.add(register(input));
      }
      final Map<String, RelSubset> alternatives = new LinkedHashMap<>();
      RelSubset equivalent = null;
      final int id = nextId++;
      for (Map.Entry<String, Convention> destination : conventions.entrySet()) {
        final String source = destination.getKey();
        if (logical instanceof TableScan) {
          final FqpTablePlacement placement = context.config().placement(
              ((TableScan) logical).getTable().getQualifiedName()).orElseThrow(() ->
                  new IllegalArgumentException("missing table placement: " + logical));
          if (!placement.sourceId().equals(source)) {
            continue;
          }
        }
        final List<RelNode> inputs = new ArrayList<>();
        for (Map<String, RelSubset> child : children) {
          if (child.containsKey(source)) {
            inputs.add(child.get(source));
          }
        }
        if (inputs.size() != children.size()) {
          continue;
        }
        final double cost = context.dataFusionCostModel().estimateOperator(logical, source)
            .totalCost();
        if (!Double.isFinite(cost)) {
          throw new IllegalArgumentException("non-finite local cost for " + logical);
        }
        final PlacedRel candidate = new PlacedRel(cluster,
            cluster.traitSetOf(destination.getValue()), logical, source, id, false, cost, inputs);
        equivalent = planner.register(candidate, equivalent);
        alternatives.put(source, equivalent);
      }
      // Each source subset includes all ways to produce this expression there.
      // Transfers connect subsets instead of enumerating complete plan combinations.
      final Map<String, RelSubset> nativeAlternatives = new LinkedHashMap<>(alternatives);
      for (Map.Entry<String, Convention> destination : conventions.entrySet()) {
        final String target = destination.getKey();
        if (!context.config().destination(target).get().capabilities()
            .flightEndpoint().isPresent()) {
          continue;
        }
        for (Map.Entry<String, RelSubset> source : nativeAlternatives.entrySet()) {
          if (source.getKey().equals(target)) {
            continue;
          }
          final DataFusionCostEstimate estimate = context.dataFusionCostModel()
              .estimate(logical, source.getKey());
          final double cost = estimate.rowCount() * estimate.rowWidth()
              * context.config().movementCostFactor();
          final PlacedRel transfer = new PlacedRel(cluster,
              cluster.traitSetOf(destination.getValue()), logical, target, id, true, cost,
              Collections.singletonList(source.getValue()));
          final RelSubset subset = planner.register(transfer, equivalent);
          alternatives.put(target, subset);
        }
      }
      if (alternatives.isEmpty()) {
        throw new IllegalArgumentException("no capable destination for "
            + logical.getRelTypeName());
      }
      memo.put(logical, alternatives);
      return alternatives;
    }

    private RelNode materialize(PlacedRel placed, Map<RelNode, String> destinations) {
      final List<RelNode> inputs = new ArrayList<>();
      for (RelNode input : placed.getInputs()) {
        inputs.add(materialize((PlacedRel) input, destinations));
      }
      final RelNode logical;
      if (placed.transfer) {
        final RelNode input = inputs.get(0);
        logical = LogicalProject.create(input, Collections.emptyList(),
            input.getCluster().getRexBuilder().identityProjects(input.getRowType()),
            input.getRowType(), Collections.emptySet());
      } else {
        logical = placed.logical.copy(placed.logical.getTraitSet(), inputs);
      }
      destinations.put(logical, placed.destination);
      return logical;
    }

    private static double totalCost(PlacedRel node) {
      double cost = node.cost;
      for (RelNode input : node.getInputs()) {
        cost += totalCost((PlacedRel) input);
      }
      return cost;
    }
  }

  /** Physical operator or transfer, with incremental cost and memoized children. */
  private static final class PlacedRel extends AbstractRelNode {
    private final RelNode logical;
    private final String destination;
    private final int expressionId;
    private final boolean transfer;
    private final double cost;
    private final List<RelNode> inputs;

    PlacedRel(RelOptCluster cluster, RelTraitSet traits, RelNode logical, String destination,
        int expressionId, boolean transfer, double cost, List<RelNode> inputs) {
      super(cluster, traits);
      this.logical = logical;
      this.destination = destination;
      this.expressionId = expressionId;
      this.transfer = transfer;
      this.cost = cost;
      this.inputs = new ArrayList<>(inputs);
    }

    @Override protected RelDataType deriveRowType() {
      return logical.getRowType();
    }

    @Override public List<RelNode> getInputs() {
      return Collections.unmodifiableList(inputs);
    }

    @Override public void replaceInput(int ordinal, RelNode input) {
      inputs.set(ordinal, input);
      recomputeDigest();
    }

    @Override public RelNode copy(RelTraitSet traits, List<RelNode> newInputs) {
      return new PlacedRel(getCluster(), traits, logical, destination, expressionId,
          transfer, cost, newInputs);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return planner.getCostFactory().makeCost(cost, 0D, 0D);
    }

    @Override public RelWriter explainTerms(RelWriter writer) {
      super.explainTerms(writer);
      for (int i = 0; i < inputs.size(); i++) {
        writer.input("input" + i, inputs.get(i));
      }
      return writer.item("expression", expressionId).item("destination", destination)
          .item("transfer", transfer).item("cost", cost);
    }
  }
}
