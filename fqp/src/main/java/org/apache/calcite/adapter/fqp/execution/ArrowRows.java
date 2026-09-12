/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpExecutionException;

import org.apache.calcite.rel.type.RelDataType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/** Converts an Arrow IPC stream to Calcite array rows. */
public final class ArrowRows {
  private ArrowRows() {
  }

  public static List<Object[]> read(byte[] stream) {
    return read(stream, null);
  }

  /** Converts Arrow objects to Calcite's internal SQL representations when typed. */
  public static List<Object[]> read(byte[] stream, @Nullable RelDataType rowType) {
    final List<Object[]> rows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
         ArrowStreamReader reader =
             new ArrowStreamReader(new ByteArrayInputStream(stream), allocator)) {
      final VectorSchemaRoot root = reader.getVectorSchemaRoot();
      if (rowType != null && root.getFieldVectors().size() != rowType.getFieldCount()) {
        throw new FqpExecutionException("Arrow result field count does not match root schema");
      }
      while (reader.loadNextBatch()) {
        final List<FieldVector> vectors = root.getFieldVectors();
        for (int row = 0; row < root.getRowCount(); row++) {
          final Object[] values = new Object[vectors.size()];
          for (int field = 0; field < vectors.size(); field++) {
            final Object value = vectors.get(field).getObject(row);
            values[field] = rowType == null || value == null ? value
                : calciteValue(value, rowType.getFieldList().get(field).getType());
          }
          rows.add(values);
        }
      }
      return rows;
    } catch (IOException e) {
      throw new FqpExecutionException("invalid Arrow response", e);
    }
  }

  private static Object calciteValue(Object value, RelDataType type) {
    switch (type.getSqlTypeName()) {
    case CHAR:
    case VARCHAR:
      return value.toString();
    case DATE:
      return value instanceof LocalDate
          ? Math.toIntExact(((LocalDate) value).toEpochDay()) : value;
    case TINYINT:
      return ((Number) value).byteValue();
    case SMALLINT:
      return ((Number) value).shortValue();
    case INTEGER:
      return ((Number) value).intValue();
    case BIGINT:
      return ((Number) value).longValue();
    case REAL:
      return ((Number) value).floatValue();
    case FLOAT:
    case DOUBLE:
      return ((Number) value).doubleValue();
    default:
      return value;
    }
  }
}
