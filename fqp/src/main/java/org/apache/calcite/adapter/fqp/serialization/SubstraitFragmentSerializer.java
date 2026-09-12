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

import io.substrait.proto.AggregateFunction;
import io.substrait.proto.AggregateRel;
import io.substrait.proto.AggregationPhase;
import io.substrait.proto.Expression;
import io.substrait.proto.FetchRel;
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
import io.substrait.proto.SortField;
import io.substrait.proto.SortRel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.protobuf.ByteString;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Serializes the reusable relational subset required by FQP benchmarks. */
public final class SubstraitFragmentSerializer {
  private static final int COMPARISON_URI_ANCHOR = 1;
  private static final int BOOLEAN_URI_ANCHOR = 2;
  private static final int ARITHMETIC_URI_ANCHOR = 3;
  private static final int AGGREGATE_URI_ANCHOR = 4;
  private static final int EQUAL_FUNCTION_ANCHOR = 1;
  private static final int AND_FUNCTION_ANCHOR = 2;
  private static final int MULTIPLY_FUNCTION_ANCHOR = 3;
  private static final int SUBTRACT_FUNCTION_ANCHOR = 4;
  private static final int SUM_FUNCTION_ANCHOR = 5;
  private static final int LESS_THAN_FUNCTION_ANCHOR = 6;
  private static final int GREATER_THAN_FUNCTION_ANCHOR = 7;
  private static final int OR_FUNCTION_ANCHOR = 8;
  private static final int DATE_PART_FUNCTION_ANCHOR = 9;
  private static final int DATETIME_URI_ANCHOR = 5;
  private static final String COMPARISON_FUNCTIONS_URI =
      "https://github.com/substrait-io/substrait/blob/main/extensions/"
          + "functions_comparison.yaml";
  private static final String BOOLEAN_FUNCTIONS_URI =
      "https://github.com/substrait-io/substrait/blob/main/extensions/"
          + "functions_boolean.yaml";
  private static final String ARITHMETIC_FUNCTIONS_URI =
      "https://github.com/substrait-io/substrait/blob/main/extensions/"
          + "functions_arithmetic.yaml";
  private static final String AGGREGATE_FUNCTIONS_URI =
      "https://github.com/substrait-io/substrait/blob/main/extensions/"
          + "functions_aggregate_generic.yaml";
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
    return serialize(root, destination, Collections.emptyMap());
  }

  /** Serializes a task, replacing boundary subtrees with temporary inputs. */
  public FqpFragment serialize(RelNode root, FqpDestination destination,
      Map<RelNode, List<String>> temporaryInputs) {
    Objects.requireNonNull(root, "root");
    Objects.requireNonNull(destination, "destination");
    if (!destination.capabilities().supports(FqpFragmentPayload.Format.SUBSTRAIT_BINARY)) {
      throw new SubstraitSerializationException("destination " + destination.sourceId()
          + " does not accept binary Substrait");
    }
    final Set<String> sourceIds = new LinkedHashSet<>();
    final Map<RelNode, List<String>> inputs = new IdentityHashMap<>();
    for (Map.Entry<RelNode, List<String>> entry : temporaryInputs.entrySet()) {
      inputs.put(Objects.requireNonNull(entry.getKey(), "temporary input node"),
          FqpPlanningConfig.copyName(entry.getValue()));
    }
    final Rel relation = relation(root, destination.sourceId(), sourceIds, inputs);
    final RelRoot.Builder rootRelation = RelRoot.newBuilder().setInput(relation);
    for (RelDataTypeField field : root.getRowType().getFieldList()) {
      rootRelation.addNames(field.getName());
    }
    final Plan.Builder plan = Plan.newBuilder().addRelations(
        PlanRel.newBuilder().setRoot(rootRelation));
    addExtension(plan, COMPARISON_URI_ANCHOR, COMPARISON_FUNCTIONS_URI,
        EQUAL_FUNCTION_ANCHOR, "equal:any_any");
    addFunction(plan, COMPARISON_URI_ANCHOR, LESS_THAN_FUNCTION_ANCHOR, "lt:any_any");
    addFunction(plan, COMPARISON_URI_ANCHOR, GREATER_THAN_FUNCTION_ANCHOR, "gt:any_any");
    addExtension(plan, BOOLEAN_URI_ANCHOR, BOOLEAN_FUNCTIONS_URI,
        AND_FUNCTION_ANCHOR, "and:bool_bool");
    addFunction(plan, BOOLEAN_URI_ANCHOR, OR_FUNCTION_ANCHOR, "or:bool_bool");
    // DataFusion's consumer resolves this engine extension through its UDF registry.
    addExtension(plan, DATETIME_URI_ANCHOR, "urn:datafusion:functions:datetime",
        DATE_PART_FUNCTION_ANCHOR, "date_part:str_date");
    addExtension(plan, ARITHMETIC_URI_ANCHOR, ARITHMETIC_FUNCTIONS_URI,
        MULTIPLY_FUNCTION_ANCHOR, "multiply:any_any");
    addFunction(plan, ARITHMETIC_URI_ANCHOR, SUBTRACT_FUNCTION_ANCHOR,
        "subtract:any_any");
    addExtension(plan, AGGREGATE_URI_ANCHOR, AGGREGATE_FUNCTIONS_URI,
        SUM_FUNCTION_ANCHOR, "sum:any");
    return new FqpFragment(destination,
        FqpFragmentPayload.binary(FqpFragmentPayload.Format.SUBSTRAIT_BINARY,
            plan.build().toByteArray()), root.getRowType(), sourceIds, Collections.emptyList());
  }

  private Rel relation(RelNode node, String destinationId, Set<String> sourceIds,
      Map<RelNode, List<String>> temporaryInputs) {
    final List<String> temporaryName = temporaryInputs.get(node);
    if (temporaryName != null) {
      return temporaryRead(node.getRowType(), temporaryName);
    }
    if (node instanceof TableScan) {
      return read((TableScan) node, destinationId, sourceIds);
    }
    if (node instanceof Project) {
      final Project project = (Project) node;
      final ProjectRel.Builder result = ProjectRel.newBuilder()
          .setInput(relation(project.getInput(), destinationId, sourceIds,
              temporaryInputs));
      final RelCommon.Emit.Builder emit = RelCommon.Emit.newBuilder();
      final int inputFieldCount = project.getInput().getRowType().getFieldCount();
      for (RexNode expression : project.getProjects()) {
        result.addExpressions(expression(expression));
        emit.addOutputMapping(inputFieldCount + emit.getOutputMappingCount());
      }
      result.setCommon(RelCommon.newBuilder().setEmit(emit));
      return Rel.newBuilder().setProject(result).build();
    }
    if (node instanceof Filter) {
      final Filter filter = (Filter) node;
      return Rel.newBuilder().setFilter(FilterRel.newBuilder()
          .setInput(relation(filter.getInput(), destinationId, sourceIds,
              temporaryInputs))
          .setCondition(expression(filter.getCondition())))
          .build();
    }
    if (node instanceof Join) {
      final Join join = (Join) node;
      if (join.getJoinType() != JoinRelType.INNER) {
        throw new SubstraitSerializationException(
            "only inner joins are supported by Substrait FQP");
      }
      return Rel.newBuilder().setJoin(JoinRel.newBuilder()
          .setLeft(relation(join.getLeft(), destinationId, sourceIds, temporaryInputs))
          .setRight(relation(join.getRight(), destinationId, sourceIds, temporaryInputs))
          .setType(JoinRel.JoinType.JOIN_TYPE_INNER)
          .setExpression(expression(join.getCondition()))).build();
    }
    if (node instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) node;
      final AggregateRel.Grouping.Builder grouping = AggregateRel.Grouping.newBuilder();
      final AggregateRel.Builder result = AggregateRel.newBuilder()
          .setInput(relation(aggregate.getInput(), destinationId, sourceIds,
              temporaryInputs));
      for (int field : aggregate.getGroupSet()) {
        grouping.addExpressionReferences(result.getGroupingExpressionsCount());
        result.addGroupingExpressions(fieldReference(field));
      }
      result.addGroupings(grouping);
      for (AggregateCall call : aggregate.getAggCallList()) {
        if (call.getAggregation().getKind() != SqlKind.SUM || call.getArgList().size() != 1) {
          throw new SubstraitSerializationException("unsupported Substrait aggregate: " + call);
        }
        result.addMeasures(AggregateRel.Measure.newBuilder().setMeasure(
            AggregateFunction.newBuilder()
                .setFunctionReference(SUM_FUNCTION_ANCHOR)
                .addArguments(FunctionArgument.newBuilder()
                    .setValue(fieldReference(call.getArgList().get(0))))
                .setOutputType(type(call.getType()))
                .setPhase(AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_RESULT)
                .setInvocation(call.isDistinct()
                    ? AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_DISTINCT
                    : AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL)));
      }
      return Rel.newBuilder().setAggregate(result).build();
    }
    if (node instanceof Sort) {
      final Sort sort = (Sort) node;
      final SortRel.Builder sorted = SortRel.newBuilder()
          .setInput(relation(sort.getInput(), destinationId, sourceIds,
              temporaryInputs));
      for (RelFieldCollation field : sort.getCollation().getFieldCollations()) {
        final boolean descending = field.getDirection().isDescending();
        final boolean nullsFirst = field.nullDirection == RelFieldCollation.NullDirection.FIRST;
        final SortField.SortDirection direction = descending
            ? (nullsFirst ? SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_FIRST
                : SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST)
            : (nullsFirst ? SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST
                : SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST);
        sorted.addSorts(SortField.newBuilder().setExpr(fieldReference(field.getFieldIndex()))
            .setDirection(direction));
      }
      final Rel sortedRel = Rel.newBuilder().setSort(sorted).build();
      if (sort.fetch == null && sort.offset == null) {
        return sortedRel;
      }
      final FetchRel.Builder fetch = FetchRel.newBuilder().setInput(sortedRel);
      if (sort.offset != null) {
        fetch.setOffsetExpr(longLiteral(literalLong(sort.offset, "sort offset")));
      }
      if (sort.fetch != null) {
        fetch.setCountExpr(longLiteral(literalLong(sort.fetch, "sort fetch")));
      }
      return Rel.newBuilder().setFetch(fetch).build();
    }
    throw new SubstraitSerializationException("unsupported Substrait relational operator: "
        + node.getRelTypeName());
  }

  private Rel temporaryRead(RelDataType rowType, List<String> name) {
    final NamedStruct.Builder schema = NamedStruct.newBuilder().setStruct(struct(rowType));
    for (RelDataTypeField field : rowType.getFieldList()) {
      schema.addNames(field.getName());
    }
    return Rel.newBuilder().setRead(ReadRel.newBuilder().setBaseSchema(schema)
        .setNamedTable(ReadRel.NamedTable.newBuilder().addAllNames(name))).build();
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

  private static Expression expression(RexNode node) {
    if (node instanceof RexInputRef) {
      return fieldReference(((RexInputRef) node).getIndex());
    }
    if (node instanceof RexLiteral) {
      return literal((RexLiteral) node);
    }
    if (!(node instanceof RexCall)) {
      throw unsupportedExpression(node, "scalar");
    }
    final RexCall call = (RexCall) node;
    if (call.getKind() == SqlKind.EXTRACT) {
      if (call.getOperands().size() != 2
          || !(call.getOperands().get(0) instanceof RexLiteral)
          || !"YEAR".equals(((RexLiteral) call.getOperands().get(0)).getValue().toString())
          || call.getOperands().get(1).getType().getSqlTypeName() != SqlTypeName.DATE) {
        throw unsupportedExpression(node, "date extraction (only YEAR from DATE)");
      }
      final Expression extracted = Expression.newBuilder().setScalarFunction(
          Expression.ScalarFunction.newBuilder().setFunctionReference(DATE_PART_FUNCTION_ANCHOR)
              .setOutputType(Type.newBuilder().setI32(Type.I32.newBuilder()
                  .setNullability(Type.Nullability.NULLABILITY_NULLABLE)))
              .addArguments(FunctionArgument.newBuilder().setValue(Expression.newBuilder()
                  .setLiteral(Expression.Literal.newBuilder().setString("year"))))
              .addArguments(FunctionArgument.newBuilder()
                  .setValue(expression(call.getOperands().get(1))))).build();
      // DataFusion year extraction returns i32; preserve Calcite's declared result type.
      return Expression.newBuilder().setCast(Expression.Cast.newBuilder()
          .setInput(extracted).setType(type(call.getType()))).build();
    }
    if (call.getKind() == SqlKind.CAST && call.getOperands().size() == 1) {
      return Expression.newBuilder().setCast(Expression.Cast.newBuilder()
          .setInput(expression(call.getOperands().get(0))).setType(type(call.getType()))).build();
    }
    final int functionAnchor;
    switch (call.getKind()) {
    case EQUALS:
      functionAnchor = EQUAL_FUNCTION_ANCHOR;
      break;
    case LESS_THAN:
      functionAnchor = LESS_THAN_FUNCTION_ANCHOR;
      break;
    case GREATER_THAN:
      functionAnchor = GREATER_THAN_FUNCTION_ANCHOR;
      break;
    case AND:
      functionAnchor = AND_FUNCTION_ANCHOR;
      break;
    case OR:
      functionAnchor = OR_FUNCTION_ANCHOR;
      break;
    case TIMES:
      functionAnchor = MULTIPLY_FUNCTION_ANCHOR;
      break;
    case MINUS:
      functionAnchor = SUBTRACT_FUNCTION_ANCHOR;
      break;
    default:
      throw unsupportedExpression(node, "scalar");
    }
    final Expression.ScalarFunction.Builder function = Expression.ScalarFunction.newBuilder()
        .setFunctionReference(functionAnchor).setOutputType(type(call.getType()));
    for (RexNode operand : call.getOperands()) {
      function.addArguments(FunctionArgument.newBuilder().setValue(expression(operand)));
    }
    return Expression.newBuilder().setScalarFunction(function).build();
  }

  private static Expression literal(RexLiteral literal) {
    final Expression.Literal.Builder result = Expression.Literal.newBuilder();
    switch (literal.getTypeName()) {
    case BOOLEAN:
      result.setBoolean(Boolean.TRUE.equals(literal.getValueAs(Boolean.class)));
      break;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
      result.setI32(literal.getValueAs(Integer.class));
      break;
    case BIGINT:
      result.setI64(literal.getValueAs(Long.class));
      break;
    case FLOAT:
    case REAL:
      result.setFp32(literal.getValueAs(Float.class));
      break;
    case DOUBLE:
      result.setFp64(literal.getValueAs(Double.class));
      break;
    case CHAR:
    case VARCHAR:
      result.setString(literal.getValueAs(String.class));
      break;
    case DATE:
      result.setDate(literal.getValueAs(Integer.class));
      break;
    case DECIMAL:
      final BigDecimal decimal = literal.getValueAs(BigDecimal.class);
      result.setDecimal(Expression.Literal.Decimal.newBuilder()
          .setValue(decimalBytes(decimal.unscaledValue()))
          .setPrecision(literal.getType().getPrecision())
          .setScale(literal.getType().getScale()));
      break;
    default:
      throw unsupportedExpression(literal, "literal");
    }
    return Expression.newBuilder().setLiteral(result).build();
  }

  private static long literalLong(RexNode node, String context) {
    if (!(node instanceof RexLiteral)) {
      throw unsupportedExpression(node, context);
    }
    return ((RexLiteral) node).getValueAs(Long.class);
  }

  private static Expression longLiteral(long value) {
    return Expression.newBuilder().setLiteral(
        Expression.Literal.newBuilder().setI64(value)).build();
  }

  private static ByteString decimalBytes(BigInteger value) {
    final byte[] bigEndian = value.toByteArray();
    if (bigEndian.length > 16) {
      throw new SubstraitSerializationException("decimal literal exceeds 128 bits: " + value);
    }
    final byte[] littleEndian = new byte[16];
    if (value.signum() < 0) {
      Arrays.fill(littleEndian, (byte) 0xff);
    }
    for (int i = 0; i < bigEndian.length; i++) {
      littleEndian[i] = bigEndian[bigEndian.length - i - 1];
    }
    return ByteString.copyFrom(littleEndian);
  }

  private static void addExtension(Plan.Builder plan, int uriAnchor, String uri,
      int functionAnchor, String functionName) {
    plan.addExtensionUris(SimpleExtensionURI.newBuilder()
        .setExtensionUriAnchor(uriAnchor).setUri(uri));
    addFunction(plan, uriAnchor, functionAnchor, functionName);
  }

  private static void addFunction(Plan.Builder plan, int uriAnchor,
      int functionAnchor, String functionName) {
    plan.addExtensions(SimpleExtensionDeclaration.newBuilder()
        .setExtensionFunction(SimpleExtensionDeclaration.ExtensionFunction.newBuilder()
            .setExtensionUriReference(uriAnchor).setFunctionAnchor(functionAnchor)
            .setName(functionName)));
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
    if (type.getSqlTypeName() == SqlTypeName.BOOLEAN) {
      return Type.newBuilder().setBool(Type.Boolean.newBuilder()
          .setNullability(nullability)).build();
    }
    if (type.getSqlTypeName() == SqlTypeName.INTEGER) {
      return Type.newBuilder().setI32(Type.I32.newBuilder()
          .setNullability(nullability)).build();
    }
    if (type.getSqlTypeName() == SqlTypeName.BIGINT) {
      return Type.newBuilder().setI64(Type.I64.newBuilder()
          .setNullability(nullability)).build();
    }
    if (type.getSqlTypeName() == SqlTypeName.FLOAT
        || type.getSqlTypeName() == SqlTypeName.REAL) {
      return Type.newBuilder().setFp32(Type.FP32.newBuilder()
          .setNullability(nullability)).build();
    }
    if (type.getSqlTypeName() == SqlTypeName.DOUBLE) {
      return Type.newBuilder().setFp64(Type.FP64.newBuilder()
          .setNullability(nullability)).build();
    }
    if (type.getSqlTypeName() == SqlTypeName.DATE) {
      return Type.newBuilder().setDate(Type.Date.newBuilder()
          .setNullability(nullability)).build();
    }
    if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
      return Type.newBuilder().setDecimal(Type.Decimal.newBuilder()
          .setPrecision(type.getPrecision()).setScale(type.getScale())
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
