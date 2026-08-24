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

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Serializes the supported logical FQP subset as destination-specific SQL.
 *
 * <p>The serializer deliberately accepts only scans, filters, projects,
 * aggregates, sorts and inner equi-joins. Unsupported subtrees fail before
 * SQL generation rather than producing an inaccurate remote fragment.</p>
 */
public final class FqpSqlSerializer {
  private final FqpPlanningConfig config;

  public FqpSqlSerializer(FqpPlanningConfig config) {
    this.config = Objects.requireNonNull(config, "config");
  }

  /** Serializes {@code root} for the destination identified by {@code sourceId}. */
  public FqpFragment serialize(RelNode root, String sourceId) {
    final FqpDestination destination = config.destination(sourceId)
        .orElseThrow(() -> new FqpSerializationException(
            "unknown FQP destination: " + sourceId));
    return serialize(root, destination);
  }

  /** Serializes {@code root} for {@code destination}. */
  public FqpFragment serialize(RelNode root, FqpDestination destination) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(destination, "destination");
    validate(root);

    final DestinationTableRewriter rewriter =
        new DestinationTableRewriter(config, destination.sourceId());
    final RelNode rewritten = root.accept(rewriter);
    final RelToSqlConverter converter = new RelToSqlConverter(destination.dialect());
    final String sql = converter.visitRoot(rewritten).asStatement()
        .toSqlString(c -> c.withDialect(destination.dialect())
            .withAlwaysUseParentheses(false)
            .withSelectListItemsOnSeparateLines(false)
            .withUpdateSetListNewline(false)
            .withIndentation(0))
        .getSql();
    return new FqpFragment(destination, sql, root.getRowType(),
        rewriter.sourceIds(), Collections.emptyList());
  }

  private static void validate(RelNode node) {
    if (node instanceof TableScan) {
      return;
    }
    if (node instanceof Filter || node instanceof Project || node instanceof Aggregate
        || node instanceof Sort) {
      validate(node.getInput(0));
      return;
    }
    if (node instanceof Join) {
      final Join join = (Join) node;
      if (join.getJoinType() != JoinRelType.INNER) {
        throw new FqpSerializationException(
            "only inner joins are supported: " + join.getJoinType());
      }
      final JoinInfo joinInfo = join.analyzeCondition();
      if (!joinInfo.isEqui() || joinInfo.leftKeys.isEmpty()) {
        throw new FqpSerializationException(
            "only inner equi-joins are supported");
      }
      validate(join.getLeft());
      validate(join.getRight());
      return;
    }
    throw new FqpSerializationException(
        "unsupported FQP relational operator: " + node.getRelTypeName());
  }

  /** Replaces logical table names with names visible from one destination. */
  private static final class DestinationTableRewriter extends RelShuttleImpl {
    private final FqpPlanningConfig config;
    private final String destinationId;
    private final Set<String> sourceIds = new LinkedHashSet<>();

    DestinationTableRewriter(FqpPlanningConfig config, String destinationId) {
      this.config = config;
      this.destinationId = destinationId;
    }

    @Override public RelNode visit(TableScan scan) {
      final RelOptTable sourceTable = scan.getTable();
      final FqpTablePlacement placement = config.placement(sourceTable.getQualifiedName())
          .orElseThrow(() -> new FqpSerializationException(
              "no FQP placement for table " + sourceTable.getQualifiedName()));
      final List<String> visibleName = placement.visibleName(destinationId)
          .orElseThrow(() -> new FqpSerializationException(
              "table " + sourceTable.getQualifiedName()
                  + " is not visible at destination " + destinationId));
      sourceIds.add(placement.sourceId());
      return LogicalTableScan.create(scan.getCluster(),
          new DestinationRelOptTable(sourceTable, visibleName), scan.getHints());
    }

    Set<String> sourceIds() {
      return Collections.unmodifiableSet(new LinkedHashSet<>(sourceIds));
    }
  }

  /** Delegating table whose identity is the table name visible at a destination. */
  private static final class DestinationRelOptTable implements RelOptTable {
    private final RelOptTable delegate;
    private final List<String> qualifiedName;

    DestinationRelOptTable(RelOptTable delegate, List<String> qualifiedName) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.qualifiedName = FqpPlanningConfig.copyName(qualifiedName);
    }

    @Override public List<String> getQualifiedName() {
      return qualifiedName;
    }

    @Override public double getRowCount() {
      return delegate.getRowCount();
    }

    @Override public RelDataType getRowType() {
      return delegate.getRowType();
    }

    @Override public @Nullable RelOptSchema getRelOptSchema() {
      return delegate.getRelOptSchema();
    }

    @Override public RelNode toRel(ToRelContext context) {
      return delegate.toRel(context);
    }

    @Override public @Nullable List<RelCollation> getCollationList() {
      return delegate.getCollationList();
    }

    @Override public @Nullable RelDistribution getDistribution() {
      return delegate.getDistribution();
    }

    @Override public boolean isKey(ImmutableBitSet columns) {
      return delegate.isKey(columns);
    }

    @Override public @Nullable List<ImmutableBitSet> getKeys() {
      return delegate.getKeys();
    }

    @Override public @Nullable List<RelReferentialConstraint> getReferentialConstraints() {
      return delegate.getReferentialConstraints();
    }

    @Override public @Nullable Expression getExpression(Class clazz) {
      return delegate.getExpression(clazz);
    }

    @Override public RelOptTable extend(List<RelDataTypeField> extendedFields) {
      return new DestinationRelOptTable(delegate.extend(extendedFields), qualifiedName);
    }

    @Override public List<ColumnStrategy> getColumnStrategies() {
      return delegate.getColumnStrategies();
    }

    @Override public <C> @Nullable C unwrap(Class<C> clazz) {
      return delegate.unwrap(clazz);
    }
  }
}
