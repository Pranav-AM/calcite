/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.calcite.adapter.fqp;

import io.substrait.proto.Expression;
import io.substrait.proto.FilterRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Serializes the first portable FQP subset to a binary Substrait plan.
 *
 * <p>The subset is intentionally small: named table reads, projections of
 * input fields, and boolean literal filters. Predicate functions are deferred
 * until destination capability negotiation can select compatible function
 * extensions.</p>
 */
public final class SubstraitFragmentSerializer {
  private final FqpPlanningConfig config;

  public SubstraitFragmentSerializer(FqpPlanningConfig config) {
    this.config = Objects.requireNonNull(config, "config");
  }

  public FqpFragment serialize(RelNode root, String sourceId) {
    final FqpDestination destination = config.destination(sourceId)
        .orElseThrow(() -> new SubstraitSerializationException(
            "unknown FQP destination: " + sourceId));
    return serialize(root, destination);
  }

  public FqpFragment serialize(RelNode root, FqpDestination destination) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(destination, "destination");
    if (!destination.capabilities().supports(FqpFragmentPayload.Format.SUBSTRAIT_BINARY)) {
      throw new SubstraitSerializationException("destination " + destination.sourceId()
          + " does not accept binary Substrait");
    }
    final Set<String> sourceIds = new LinkedHashSet<>();
    final Rel relation = relation(root, destination.sourceId(), sourceIds);
    final RelRoot.Builder rootRelation = RelRoot.newBuilder().setInput(relation);
    for (RelDataTypeField field : root.getRowType().getFieldList()) {
      rootRelation.addNames(field.getName());
    }
    final Plan plan = Plan.newBuilder().addRelations(
        PlanRel.newBuilder().setRoot(rootRelation)).build();
    return new FqpFragment(destination,
        FqpFragmentPayload.binary(FqpFragmentPayload.Format.SUBSTRAIT_BINARY,
            plan.toByteArray()), root.getRowType(), sourceIds, Collections.emptyList());
  }

  private Rel relation(RelNode node, String destinationId, Set<String> sourceIds) {
    if (node instanceof TableScan) {
      return read((TableScan) node, destinationId, sourceIds);
    }
    if (node instanceof Project) {
      final Project project = (Project) node;
      final ProjectRel.Builder result = ProjectRel.newBuilder()
          .setInput(relation(project.getInput(), destinationId, sourceIds));
      for (RexNode expression : project.getProjects()) {
        if (!(expression instanceof RexInputRef)) {
          throw unsupportedExpression(expression, "project");
        }
        result.addExpressions(fieldReference(((RexInputRef) expression).getIndex()));
      }
      return Rel.newBuilder().setProject(result).build();
    }
    if (node instanceof Filter) {
      final Filter filter = (Filter) node;
      if (!(filter.getCondition() instanceof RexLiteral)) {
        throw unsupportedExpression(filter.getCondition(), "filter");
      }
      final RexLiteral literal = (RexLiteral) filter.getCondition();
      if (!literal.isAlwaysTrue() && !literal.isAlwaysFalse()) {
        throw unsupportedExpression(filter.getCondition(), "filter");
      }
      final boolean value = literal.isAlwaysTrue();
      return Rel.newBuilder().setFilter(FilterRel.newBuilder()
          .setInput(relation(filter.getInput(), destinationId, sourceIds))
          .setCondition(Expression.newBuilder().setLiteral(
              Expression.Literal.newBuilder().setBoolean(value))))
          .build();
    }
    throw new SubstraitSerializationException("unsupported Substrait relational operator: "
        + node.getRelTypeName());
  }

  private Rel read(TableScan scan, String destinationId, Set<String> sourceIds) {
    final FqpTablePlacement placement = config.placement(scan.getTable().getQualifiedName())
        .orElseThrow(() -> new SubstraitSerializationException("no FQP placement for table "
            + scan.getTable().getQualifiedName()));
    final List<String> name = placement.visibleName(destinationId)
        .orElseThrow(() -> new SubstraitSerializationException("table "
            + scan.getTable().getQualifiedName() + " is not visible at destination "
            + destinationId));
    sourceIds.add(placement.sourceId());
    final NamedStruct.Builder schema = NamedStruct.newBuilder().setStruct(
        struct(scan.getRowType()));
    for (RelDataTypeField field : scan.getRowType().getFieldList()) {
      schema.addNames(field.getName());
    }
    final ReadRel.NamedTable.Builder table = ReadRel.NamedTable.newBuilder();
    for (String part : name) {
      table.addNames(part);
    }
    return Rel.newBuilder().setRead(ReadRel.newBuilder()
        .setBaseSchema(schema).setNamedTable(table)).build();
  }

  private static Expression fieldReference(int index) {
    return Expression.newBuilder().setSelection(Expression.FieldReference.newBuilder()
        .setRootReference(Expression.FieldReference.RootReference.newBuilder())
        .setDirectReference(Expression.ReferenceSegment.newBuilder().setStructField(
            Expression.ReferenceSegment.StructField.newBuilder().setField(index)))).build();
  }

  private static Type.Struct struct(RelDataType rowType) {
    final Type.Struct.Builder struct = Type.Struct.newBuilder()
        .setNullability(Type.Nullability.NULLABILITY_REQUIRED);
    for (RelDataTypeField field : rowType.getFieldList()) {
      struct.addTypes(type(field.getType()));
    }
    return struct.build();
  }

  private static Type type(RelDataType type) {
    final Type.Nullability nullability = type.isNullable()
        ? Type.Nullability.NULLABILITY_NULLABLE : Type.Nullability.NULLABILITY_REQUIRED;
    if (type.getSqlTypeName() == SqlTypeName.INTEGER) {
      return Type.newBuilder().setI32(Type.I32.newBuilder()
          .setNullability(nullability)).build();
    }
    if (type.getSqlTypeName() == SqlTypeName.BIGINT) {
      return Type.newBuilder().setI64(Type.I64.newBuilder()
          .setNullability(nullability)).build();
    }
    if (type.getSqlTypeName() == SqlTypeName.VARCHAR
        || type.getSqlTypeName() == SqlTypeName.CHAR) {
      return Type.newBuilder().setString(Type.String.newBuilder()
          .setNullability(nullability)).build();
    }
    throw new SubstraitSerializationException("unsupported Substrait type: " + type);
  }

  private static SubstraitSerializationException unsupportedExpression(RexNode node,
      String context) {
    return new SubstraitSerializationException("unsupported Substrait " + context
        + " expression: " + node);
  }
}
