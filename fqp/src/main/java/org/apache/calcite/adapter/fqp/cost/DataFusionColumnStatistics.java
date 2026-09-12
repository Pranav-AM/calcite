/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.cost;

import java.util.Optional;

/** Immutable column statistics loaded before an FQP planning session. */
public final class DataFusionColumnStatistics {
  private final double distinctCount;
  private final double nullCount;
  private final Comparable<?> minimum;
  private final Comparable<?> maximum;

  public DataFusionColumnStatistics(double distinctCount, double nullCount,
      Comparable<?> minimum, Comparable<?> maximum) {
    if (distinctCount < 0D || nullCount < 0D) {
      throw new IllegalArgumentException("statistics counts must not be negative");
    }
    this.distinctCount = distinctCount;
    this.nullCount = nullCount;
    this.minimum = minimum;
    this.maximum = maximum;
  }

  public static DataFusionColumnStatistics of(double distinctCount, double nullCount) {
    return new DataFusionColumnStatistics(distinctCount, nullCount, null, null);
  }

  public double distinctCount() {
    return distinctCount;
  }

  public double nullCount() {
    return nullCount;
  }

  public Optional<Comparable<?>> minimum() {
    return Optional.ofNullable(minimum);
  }

  public Optional<Comparable<?>> maximum() {
    return Optional.ofNullable(maximum);
  }
}
