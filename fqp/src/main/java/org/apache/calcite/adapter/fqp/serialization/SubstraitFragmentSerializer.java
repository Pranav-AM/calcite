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
package org.apache.calcite.adapter.fqp.serialization;

import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpFragment;
import org.apache.calcite.adapter.fqp.FqpFragmentPayload;
import org.apache.calcite.adapter.fqp.FqpPlanningConfig;
import org.apache.calcite.adapter.fqp.FqpTablePlacement;

import io.substrait.proto.Expression;
import io.substrait.proto.FilterRel;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.JoinRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SimpleExtensionDeclaration;
import io.substrait.proto.SimpleExtensionURI;
import io.substrait.proto.Type;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
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
  private static final int COMPARISON_URI_ANCHOR = 1;
  private static final int EQUAL_FUNCTION_ANCHOR = 1;
  private static final String COMPARISON_FUNCTIONS_URI =
      "https://github.com/substrait-io/substrait/blob/main/extensions/"
          + "functions_comparison.yaml";
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
    final Plan.Builder plan = Plan.newBuilder().addRelations(
        PlanRel.newBuilder().setRoot(rootRelation));
    if (containsJoin(root)) {
      plan.addExtensionUris(SimpleExtensionURI.newBuilder()
          .setExtensionUriAnchor(COMPARISON_URI_ANCHOR)
          .setUri(COMPARISON_FUNCTIONS_URI));
      plan.addExtensions(SimpleExtensionDeclaration.newBuilder()
          .setExtensionFunction(SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
              .setExtensionUriReference(COMPARISON_URI_ANCHOR)
              .setFunctionAnchor(EQUAL_FUNCTION_ANCHOR)
              .setName("equal:any_any")));
    }
    return new FqpFragment(destination,
        FqpFragmentPayload.binary(FqpFragmentPayload.Format.SUBSTRAIT_BINARY,
            plan.build().toByteArray()), root.getRowType(), sourceIds, Collections.emptyList());
  }

  private Rel relation(RelNode node, String destinationId, Set<String> sourceIds) {
    if (node instanceof TableScan) {
      return read((TableScan) node, destinationId, sourceIds);
    }
    if (node instanceof Project) {
      final Project project = (Project) node;
      final ProjectRel.Builder result = ProjectRel.newBuilder()
          .setInput(relation(project.getInput(), destinationId, sourceIds));
      final RelCommon.Emit.Builder emit = RelCommon.Emit.newBuilder();
      final int inputFieldCount = project.getInput().getRowType().getFieldCount();
      for (RexNode expression : project.getProjects()) {
        if (!(expression instanceof RexInputRef)) {
          throw unsupportedExpression(expression, "project");
        }
        result.addExpressions(fieldReference(((RexInputRef) expression).getIndex()));
        emit.addOutputMapping(inputFieldCount + emit.getOutputMappingCount());
      }
      result.setCommon(RelCommon.newBuilder().setEmit(emit));
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
    if (node instanceof Join) {
      final Join join = (Join) node;
      if (join.getJoinType() != JoinRelType.INNER) {
        throw new SubstraitSerializationException(
            "only inner joins are supported by Substrait FQP");
      }
      return Rel.newBuilder().setJoin(JoinRel.newBuilder()
          .setLeft(relation(join.getLeft(), destinationId, sourceIds))
          .setRight(relation(join.getRight(), destinationId, sourceIds))
          .setType(JoinRel.JoinType.JOIN_TYPE_INNER)
          .setExpression(joinCondition(join.getCondition()))).build();
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

  private static Expression joinCondition(RexNode condition) {
    if (!(condition instanceof RexCall) || condition.getKind() != SqlKind.EQUALS) {
      throw unsupportedExpression(condition, "join condition");
    }
    final RexCall call = (RexCall) condition;
    if (call.getOperands().size() != 2
        || !(call.getOperands().get(0) instanceof RexInputRef)
        || !(call.getOperands().get(1) instanceof RexInputRef)) {
      throw unsupportedExpression(condition, "join condition");
    }
    final Expression left = fieldReference(
        ((RexInputRef) call.getOperands().get(0)).getIndex());
    final Expression right = fieldReference(
        ((RexInputRef) call.getOperands().get(1)).getIndex());
    return Expression.newBuilder().setScalarFunction(Expression.ScalarFunction.newBuilder()
        .setFunctionReference(EQUAL_FUNCTION_ANCHOR)
        .addArguments(FunctionArgument.newBuilder().setValue(left))
        .addArguments(FunctionArgument.newBuilder().setValue(right))
        .setOutputType(Type.newBuilder().setBool(Type.Boolean.newBuilder()
            .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))).build();
  }

  private static boolean containsJoin(RelNode node) {
    if (node instanceof Join) {
      return true;
    }
    for (RelNode input : node.getInputs()) {
      if (containsJoin(input)) {
        return true;
      }
    }
    return false;
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
