/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpExecutionException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Converts an Arrow IPC stream to Calcite array rows. */
public final class ArrowRows {
  private ArrowRows() {
  }

  public static List<Object[]> read(byte[] stream) {
    final List<Object[]> rows = new ArrayList<>();
    try (BufferAllocator allocator = new RootAllocator();
         ArrowStreamReader reader =
             new ArrowStreamReader(new ByteArrayInputStream(stream), allocator)) {
      final VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        final List<FieldVector> vectors = root.getFieldVectors();
        for (int row = 0; row < root.getRowCount(); row++) {
          final Object[] values = new Object[vectors.size()];
          for (int field = 0; field < vectors.size(); field++) {
            values[field] = vectors.get(field).getObject(row);
          }
          rows.add(values);
        }
      }
      return rows;
    } catch (IOException e) {
      throw new FqpExecutionException("invalid Arrow response", e);
    }
  }
}
