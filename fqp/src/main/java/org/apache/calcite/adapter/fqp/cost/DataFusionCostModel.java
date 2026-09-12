/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.cost;

import org.apache.calcite.adapter.fqp.FqpPlanningConfig;
import org.apache.calcite.adapter.fqp.FqpTablePlacement;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Heuristic DataFusion cost model that performs no network communication. */
public final class DataFusionCostModel {
  private static final double DEFAULT_FILTER_SELECTIVITY = 0.25D;
  private final FqpPlanningConfig config;
  private final Map<String, DataFusionStatisticsSnapshot> snapshots;
  private final Map<String, DataFusionCostParameters> parameters;

  public DataFusionCostModel(FqpPlanningConfig config,
      Map<String, DataFusionStatisticsSnapshot> snapshots,
      Map<String, DataFusionCostParameters> parameters) {
    this.config = Objects.requireNonNull(config, "config");
    this.snapshots = immutableCopy(snapshots, "snapshots");
    this.parameters = immutableCopy(parameters, "parameters");
  }

  public static DataFusionCostModel empty(FqpPlanningConfig config) {
    return new DataFusionCostModel(config, Collections.emptyMap(), Collections.emptyMap());
  }

  /** Estimates {@code rel} as if its root executes at {@code destinationId}. */
  public DataFusionCostEstimate estimate(RelNode rel, String destinationId) {
    Objects.requireNonNull(rel, "rel");
    final DataFusionCostParameters coefficients =
        parameters.getOrDefault(destinationId, DataFusionCostParameters.DEFAULT);
    return estimate(rel, destinationId, coefficients);
  }

  public Map<String, DataFusionStatisticsSnapshot> snapshots() {
    return snapshots;
  }

  /** Local work for this operator only; children and transfers are costed separately. */
  public DataFusionCostEstimate estimateOperator(RelNode rel, String destinationId) {
    final DataFusionCostEstimate total = estimate(rel, destinationId);
    double cpu = total.cpu();
    double io = total.io();
    for (RelNode input : rel.getInputs()) {
      final DataFusionCostEstimate child = estimate(input, destinationId);
      cpu -= child.cpu();
      io -= child.io();
    }
    return new DataFusionCostEstimate(total.rowCount(), total.rowWidth(), cpu, io, 0D);
  }

  private DataFusionCostEstimate estimate(RelNode rel, String destinationId,
      DataFusionCostParameters coefficients) {
    if (rel instanceof TableScan) {
      return scan((TableScan) rel, destinationId, coefficients);
    }
    if (rel instanceof Filter) {
      final Filter filter = (Filter) rel;
      final DataFusionCostEstimate input = estimate(filter.getInput(), destinationId,
          coefficients);
      final double rows = input.rowCount() * selectivity(filter.getCondition(), filter);
      return new DataFusionCostEstimate(rows, input.rowWidth(),
          input.cpu() + input.rowCount() * coefficients.expressionCpuPerRow,
          input.io(), input.network());
    }
    if (rel instanceof Project) {
      final Project project = (Project) rel;
      final DataFusionCostEstimate input = estimate(project.getInput(), destinationId,
          coefficients);
      return new DataFusionCostEstimate(input.rowCount(), width(project.getRowType()),
          input.cpu() + input.rowCount() * project.getProjects().size()
              * coefficients.expressionCpuPerRow,
          input.io(), input.network());
    }
    if (rel instanceof Join) {
      return join((Join) rel, destinationId, coefficients);
    }
    if (rel instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) rel;
      final DataFusionCostEstimate input = estimate(aggregate.getInput(), destinationId,
          coefficients);
      final double rows = aggregate.getGroupCount() == 0 ? 1D
          : Math.max(1D, Math.min(input.rowCount(),
              Math.pow(Math.max(1D, input.rowCount()), 0.5D * aggregate.getGroupCount())));
      return new DataFusionCostEstimate(rows, width(aggregate.getRowType()),
          input.cpu() + input.rowCount() * coefficients.aggregateCpuPerRow,
          input.io(), input.network());
    }
    if (rel instanceof Sort) {
      final Sort sort = (Sort) rel;
      final DataFusionCostEstimate input = estimate(sort.getInput(), destinationId, coefficients);
      final double rows = sort.fetch instanceof RexLiteral
          ? Math.min(input.rowCount(), Math.max(0D,
              ((RexLiteral) sort.fetch).getValueAs(Long.class)))
          : input.rowCount();
      final double comparisons = input.rowCount()
          * log2(Math.max(2D, input.rowCount()));
      return new DataFusionCostEstimate(rows, input.rowWidth(),
          input.cpu() + comparisons * coefficients.sortCpuPerComparison,
          input.io(), input.network());
    }
    if (rel.getInputs().size() == 1) {
      return estimate(rel.getInput(0), destinationId, coefficients);
    }
    throw new IllegalArgumentException("unsupported DataFusion cost operator: "
        + rel.getRelTypeName());
  }

  private DataFusionCostEstimate scan(TableScan scan, String destinationId,
      DataFusionCostParameters coefficients) {
    final FqpTablePlacement placement = config.placement(scan.getTable().getQualifiedName())
        .orElseThrow(() -> new IllegalArgumentException("no FQP placement for table "
            + scan.getTable().getQualifiedName()));
    final DataFusionStatisticsSnapshot snapshot = snapshots.get(placement.sourceId());
    if (snapshot == null) {
      throw new IllegalArgumentException("no statistics snapshot for " + placement.sourceId());
    }
    final DataFusionTableStatistics table = snapshot.table(scan.getTable().getQualifiedName())
        .orElseThrow(() -> new IllegalArgumentException("no statistics for table "
            + scan.getTable().getQualifiedName()));
    final double bytes = table.rowCount() * table.averageRowWidth();
    final double network = placement.sourceId().equals(destinationId)
        ? 0D : bytes * config.movementCostFactor();
    return new DataFusionCostEstimate(table.rowCount(), table.averageRowWidth(),
        table.rowCount() * coefficients.scanCpuPerRow,
        bytes * coefficients.ioPerByte, network);
  }

  private DataFusionCostEstimate join(Join join, String destinationId,
      DataFusionCostParameters coefficients) {
    final DataFusionCostEstimate left = estimate(join.getLeft(), destinationId, coefficients);
    final DataFusionCostEstimate right = estimate(join.getRight(), destinationId, coefficients);
    double rows = left.rowCount() * right.rowCount() * DEFAULT_FILTER_SELECTIVITY;
    if (join.getCondition() instanceof RexCall
        && join.getCondition().getKind() == SqlKind.EQUALS) {
      final RexCall call = (RexCall) join.getCondition();
      if (call.getOperands().size() == 2
          && call.getOperands().get(0) instanceof RexInputRef
          && call.getOperands().get(1) instanceof RexInputRef) {
        final double leftNdv = distinctCount(join.getLeft(),
            ((RexInputRef) call.getOperands().get(0)).getIndex());
        final int rightIndex = ((RexInputRef) call.getOperands().get(1)).getIndex()
            - join.getLeft().getRowType().getFieldCount();
        final double rightNdv = distinctCount(join.getRight(), rightIndex);
        rows = left.rowCount() * right.rowCount() / Math.max(1D, Math.max(leftNdv, rightNdv));
      }
    }
    return new DataFusionCostEstimate(rows, left.rowWidth() + right.rowWidth(),
        left.cpu() + right.cpu()
            + (left.rowCount() + right.rowCount()) * coefficients.joinCpuPerRow,
        left.io() + right.io(), left.network() + right.network());
  }

  private double distinctCount(RelNode rel, int fieldIndex) {
    if (fieldIndex < 0 || fieldIndex >= rel.getRowType().getFieldCount()) {
      return 1D;
    }
    if (rel instanceof TableScan) {
      final TableScan scan = (TableScan) rel;
      final FqpTablePlacement placement = config.placement(scan.getTable().getQualifiedName())
          .orElse(null);
      if (placement != null) {
        final DataFusionStatisticsSnapshot snapshot = snapshots.get(placement.sourceId());
        if (snapshot != null) {
          final String field = rel.getRowType().getFieldList().get(fieldIndex).getName();
          return snapshot.table(scan.getTable().getQualifiedName())
              .flatMap(table -> table.column(field))
              .map(DataFusionColumnStatistics::distinctCount).orElse(1D);
        }
      }
    }
    return Math.max(1D, Math.sqrt(estimateRowCount(rel)));
  }

  private double estimateRowCount(RelNode rel) {
    if (rel instanceof TableScan) {
      final TableScan scan = (TableScan) rel;
      final FqpTablePlacement placement = config.placement(scan.getTable().getQualifiedName())
          .orElse(null);
      if (placement != null && snapshots.containsKey(placement.sourceId())) {
        return snapshots.get(placement.sourceId()).table(scan.getTable().getQualifiedName())
            .map(DataFusionTableStatistics::rowCount).orElse(1D);
      }
    }
    return 1D;
  }

  private double selectivity(RexNode condition, Filter filter) {
    if (condition.isAlwaysTrue()) {
      return 1D;
    }
    if (condition.isAlwaysFalse()) {
      return 0D;
    }
    if (condition instanceof RexCall) {
      final RexCall call = (RexCall) condition;
      if (condition.getKind() == SqlKind.AND) {
        double result = 1D;
        for (RexNode operand : call.getOperands()) {
          result *= selectivity(operand, filter);
        }
        return result;
      }
      if (condition.getKind() == SqlKind.EQUALS && call.getOperands().size() == 2) {
        final RexInputRef reference = inputReference(call.getOperands().get(0),
            call.getOperands().get(1));
        if (reference != null) {
          return 1D / Math.max(1D, distinctCount(filter.getInput(), reference.getIndex()));
        }
      }
    }
    return DEFAULT_FILTER_SELECTIVITY;
  }

  private static RexInputRef inputReference(RexNode left, RexNode right) {
    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      return (RexInputRef) left;
    }
    if (right instanceof RexInputRef && left instanceof RexLiteral) {
      return (RexInputRef) right;
    }
    return null;
  }

  private static int width(RelDataType rowType) {
    int width = 0;
    for (RelDataTypeField field : rowType.getFieldList()) {
      final SqlTypeName type = field.getType().getSqlTypeName();
      if (type == SqlTypeName.BOOLEAN || type == SqlTypeName.TINYINT) {
        width += 1;
      } else if (type == SqlTypeName.SMALLINT) {
        width += 2;
      } else if (type == SqlTypeName.INTEGER || type == SqlTypeName.REAL
          || type == SqlTypeName.DATE) {
        width += 4;
      } else if (type == SqlTypeName.BIGINT || type == SqlTypeName.DOUBLE
          || type == SqlTypeName.TIMESTAMP) {
        width += 8;
      } else if (type == SqlTypeName.DECIMAL) {
        width += 16;
      } else {
        width += field.getType().getPrecision() > 0
            ? Math.min(256, field.getType().getPrecision()) : 32;
      }
    }
    return width;
  }

  private static double log2(double value) {
    return Math.log(value) / Math.log(2D);
  }

  private static <T> Map<String, T> immutableCopy(Map<String, T> values, String name) {
    Objects.requireNonNull(values, name);
    final Map<String, T> copy = new LinkedHashMap<>();
    for (Map.Entry<String, T> entry : values.entrySet()) {
      copy.put(Objects.requireNonNull(entry.getKey(), "sourceId"),
          Objects.requireNonNull(entry.getValue(), name + " value"));
    }
    return Collections.unmodifiableMap(copy);
  }
}
