/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.cost;

/** Calibratable DataFusion coefficients loaded or configured before planning. */
public final class DataFusionCostParameters {
  public static final DataFusionCostParameters DEFAULT =
      new DataFusionCostParameters(0.01D, 0.02D, 0.05D, 0.03D, 0.01D, 0.0001D);

  final double scanCpuPerRow;
  final double expressionCpuPerRow;
  final double joinCpuPerRow;
  final double aggregateCpuPerRow;
  final double sortCpuPerComparison;
  final double ioPerByte;

  public DataFusionCostParameters(double scanCpuPerRow, double expressionCpuPerRow,
      double joinCpuPerRow, double aggregateCpuPerRow, double sortCpuPerComparison,
      double ioPerByte) {
    this.scanCpuPerRow = nonNegative(scanCpuPerRow, "scanCpuPerRow");
    this.expressionCpuPerRow = nonNegative(expressionCpuPerRow, "expressionCpuPerRow");
    this.joinCpuPerRow = nonNegative(joinCpuPerRow, "joinCpuPerRow");
    this.aggregateCpuPerRow = nonNegative(aggregateCpuPerRow, "aggregateCpuPerRow");
    this.sortCpuPerComparison = nonNegative(sortCpuPerComparison, "sortCpuPerComparison");
    this.ioPerByte = nonNegative(ioPerByte, "ioPerByte");
  }

  private static double nonNegative(double value, String name) {
    if (!Double.isFinite(value) || value < 0D) {
      throw new IllegalArgumentException(name + " must be finite and non-negative");
    }
    return value;
  }
}
