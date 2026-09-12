/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.cost;

/** Local estimate for one relational subtree at one DataFusion destination. */
public final class DataFusionCostEstimate {
  private final double rowCount;
  private final int rowWidth;
  private final double cpu;
  private final double io;
  private final double network;

  DataFusionCostEstimate(double rowCount, int rowWidth, double cpu, double io,
      double network) {
    this.rowCount = Math.max(0D, rowCount);
    this.rowWidth = Math.max(0, rowWidth);
    this.cpu = Math.max(0D, cpu);
    this.io = Math.max(0D, io);
    this.network = Math.max(0D, network);
  }

  public double rowCount() {
    return rowCount;
  }

  public int rowWidth() {
    return rowWidth;
  }

  public double cpu() {
    return cpu;
  }

  public double io() {
    return io;
  }

  public double network() {
    return network;
  }

  public double totalCost() {
    return cpu + io + network;
  }
}
