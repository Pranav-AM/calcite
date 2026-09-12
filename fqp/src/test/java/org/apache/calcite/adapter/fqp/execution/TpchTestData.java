/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exports Calcite's generated tables and measures statistics before planning. */
final class TpchTestData {
  private TpchTestData() {
  }

  static ObjectNode export(Connection connection, String table, Path directory) throws Exception {
    Files.createDirectories(directory);
    final String fileName = table.toLowerCase(Locale.ROOT);
    final Path arrow = directory.resolve(fileName + ".arrow");
    final ObjectNode stats = new ObjectMapper().createObjectNode();
    stats.putArray("logicalName").add("TPCH").add(table);
    try (Statement statement = connection.createStatement();
         ResultSet rows = statement.executeQuery("select * from \"TPCH\".\""
             + table.replace("\"", "\"\"") + "\"");
         RootAllocator allocator = new RootAllocator();
         OutputStream output = Files.newOutputStream(arrow)) {
      final ResultSetMetaData metadata = rows.getMetaData();
      final List<FieldVector> vectors = new ArrayList<>();
      final List<Set<Object>> distinct = new ArrayList<>();
      final long[] nulls = new long[metadata.getColumnCount()];
      for (int i = 1; i <= metadata.getColumnCount(); i++) {
        vectors.add(vector(metadata.getColumnName(i), metadata.getColumnType(i), allocator));
        distinct.add(new HashSet<>());
      }
      long count = 0;
      long bytes = 0;
      try (VectorSchemaRoot root = VectorSchemaRoot.of(vectors.toArray(new FieldVector[0]));
           ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
        root.allocateNew();
        writer.start();
        int batchCount = 0;
        while (rows.next()) {
          for (int i = 0; i < vectors.size(); i++) {
            final Object value = rows.getObject(i + 1);
            if (value == null) {
              vectors.get(i).setNull(batchCount);
              nulls[i]++;
            } else {
              distinct.get(i).add(value);
              bytes += write(vectors.get(i), batchCount, value);
            }
          }
          count++;
          if (++batchCount == 1024) {
            root.setRowCount(batchCount);
            writer.writeBatch();
            root.clear();
            root.allocateNew();
            batchCount = 0;
          }
        }
        if (batchCount > 0) {
          root.setRowCount(batchCount);
          writer.writeBatch();
        }
        writer.end();
      }
      stats.put("rowCount", count);
      stats.put("averageRowWidth", count == 0 ? 0 : (int) Math.ceil((double) bytes / count));
      final ObjectNode columns = stats.putObject("columns");
      for (int i = 0; i < vectors.size(); i++) {
        columns.putObject(metadata.getColumnName(i + 1))
            .put("distinctCount", distinct.get(i).size()).put("nullCount", nulls[i]);
      }
    }
    final Path log = directory.resolve(fileName + "-conversion.log");
    final Process converter = new ProcessBuilder(System.getProperty("fqp.arrow.converter"),
        arrow.toString(), directory.resolve(fileName + ".parquet").toString())
        .redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(converter.waitFor(60, TimeUnit.SECONDS), "Parquet conversion timed out");
      assertEquals(0, converter.exitValue(),
          String.join("\n", Files.readAllLines(log, StandardCharsets.UTF_8)));
    } finally {
      if (converter.isAlive()) {
        converter.destroyForcibly();
        assertTrue(converter.waitFor(5, TimeUnit.SECONDS));
      }
    }
    return stats;
  }

  private static FieldVector vector(String name, int type, RootAllocator allocator) {
    switch (type) {
    case Types.INTEGER:
      return new IntVector(name, allocator);
    case Types.BIGINT:
      return new BigIntVector(name, allocator);
    case Types.DOUBLE:
      return new Float8Vector(name, allocator);
    case Types.DATE:
      return new DateDayVector(name, allocator);
    case Types.CHAR:
    case Types.VARCHAR:
      return new VarCharVector(name, allocator);
    default:
      throw new IllegalArgumentException("unsupported TPC-H JDBC type " + type);
    }
  }

  private static int write(FieldVector vector, int row, Object value) {
    if (vector instanceof IntVector) {
      ((IntVector) vector).setSafe(row, ((Number) value).intValue());
      return 4;
    }
    if (vector instanceof BigIntVector) {
      ((BigIntVector) vector).setSafe(row, ((Number) value).longValue());
      return 8;
    }
    if (vector instanceof Float8Vector) {
      ((Float8Vector) vector).setSafe(row, ((Number) value).doubleValue());
      return 8;
    }
    if (vector instanceof DateDayVector) {
      ((DateDayVector) vector).setSafe(row,
          Math.toIntExact(((Date) value).toLocalDate().toEpochDay()));
      return 4;
    }
    final byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
    ((VarCharVector) vector).setSafe(row, bytes);
    return bytes.length + 4;
  }
}
