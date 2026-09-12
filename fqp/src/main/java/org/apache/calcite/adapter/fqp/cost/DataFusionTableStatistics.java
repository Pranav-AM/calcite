/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.cost;

import org.apache.calcite.adapter.fqp.FqpDestination;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Immutable table statistics loaded from a DataFusion worker at startup. */
public final class DataFusionTableStatistics {
  private final double rowCount;
  private final int averageRowWidth;
  private final Map<String, DataFusionColumnStatistics> columns;

  public DataFusionTableStatistics(double rowCount, int averageRowWidth,
      Map<String, DataFusionColumnStatistics> columns) {
    if (rowCount < 0D || averageRowWidth < 0) {
      throw new IllegalArgumentException("row count and width must not be negative");
    }
    this.rowCount = rowCount;
    this.averageRowWidth = averageRowWidth;
    Objects.requireNonNull(columns, "columns");
    final Map<String, DataFusionColumnStatistics> copy = new LinkedHashMap<>();
    for (Map.Entry<String, DataFusionColumnStatistics> entry : columns.entrySet()) {
      copy.put(normalize(entry.getKey()), Objects.requireNonNull(entry.getValue(), "statistics"));
    }
    this.columns = Collections.unmodifiableMap(copy);
  }

  public static DataFusionTableStatistics of(double rowCount, int averageRowWidth) {
    return new DataFusionTableStatistics(rowCount, averageRowWidth, Collections.emptyMap());
  }

  public double rowCount() {
    return rowCount;
  }

  public int averageRowWidth() {
    return averageRowWidth;
  }

  public Optional<DataFusionColumnStatistics> column(String name) {
    return Optional.ofNullable(columns.get(normalize(name)));
  }

  private static String normalize(String value) {
    return FqpDestination.requireNonBlank(value, "column name").toUpperCase(Locale.ROOT);
  }
}
