/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpExecutionException;

import org.apache.calcite.rel.type.RelDataType;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

/** Validates uploaded Arrow types against the selected exchange's Calcite schema. */
final class ArrowExchangeSchema {
  private ArrowExchangeSchema() {
  }

  static void validate(RelDataType expected, Schema actual) {
    if (expected.getFieldCount() != actual.getFields().size()) {
      throw new FqpExecutionException("exchange schema field count mismatch");
    }
    for (int i = 0; i < expected.getFieldCount(); i++) {
      final RelDataType type = expected.getFieldList().get(i).getType();
      final ArrowType arrow = actual.getFields().get(i).getType();
      if (!matches(type, arrow)) {
        throw new FqpExecutionException("exchange schema mismatch at field " + i
            + ": expected " + type + ", received " + arrow);
      }
    }
  }

  private static boolean matches(RelDataType type, ArrowType arrow) {
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
      return arrow instanceof ArrowType.Bool;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
      return arrow instanceof ArrowType.Int && ((ArrowType.Int) arrow).getIsSigned()
          && ((ArrowType.Int) arrow).getBitWidth() == 32;
    case BIGINT:
      return arrow instanceof ArrowType.Int && ((ArrowType.Int) arrow).getIsSigned()
          && ((ArrowType.Int) arrow).getBitWidth() == 64;
    case REAL:
    case FLOAT:
      return arrow instanceof ArrowType.FloatingPoint
          && ((ArrowType.FloatingPoint) arrow).getPrecision() == FloatingPointPrecision.SINGLE;
    case DOUBLE:
      return arrow instanceof ArrowType.FloatingPoint
          && ((ArrowType.FloatingPoint) arrow).getPrecision() == FloatingPointPrecision.DOUBLE;
    case DECIMAL:
      return arrow instanceof ArrowType.Decimal
          && ((ArrowType.Decimal) arrow).getPrecision() == type.getPrecision()
          && ((ArrowType.Decimal) arrow).getScale() == type.getScale();
    case DATE:
      return arrow instanceof ArrowType.Date && ((ArrowType.Date) arrow).getUnit() == DateUnit.DAY;
    case CHAR:
    case VARCHAR:
      return arrow instanceof ArrowType.Utf8 || arrow instanceof ArrowType.LargeUtf8
          || arrow instanceof ArrowType.Utf8View;
    default:
      return false;
    }
  }
}
