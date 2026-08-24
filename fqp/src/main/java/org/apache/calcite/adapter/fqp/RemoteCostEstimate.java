/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.fqp;

/** Immutable remote-planner estimate for one FQP candidate. */
public final class RemoteCostEstimate {
  private final double startupCost;
  private final double totalCost;
  private final double rowCount;
  private final int rowWidth;

  public RemoteCostEstimate(double startupCost, double totalCost,
      double rowCount, int rowWidth) {
    this.startupCost = startupCost;
    this.totalCost = totalCost;
    this.rowCount = rowCount;
    this.rowWidth = rowWidth;
  }

  public double getStartupCost() {
    return startupCost;
  }

  public double getTotalCost() {
    return totalCost;
  }

  public double getRowCount() {
    return rowCount;
  }

  public int getRowWidth() {
    return rowWidth;
  }

  public double cpu() {
    return Math.max(0D, totalCost - startupCost);
  }

  public double transferCost(double bytesPerRowFactor) {
    return Math.max(0D, rowCount) * Math.max(0, rowWidth) * bytesPerRowFactor;
  }

  /** Returns the PostgreSQL-prototype movement penalty for this result. */
  public double movementCost(double movementCostFactor) {
    return transferCost(movementCostFactor);
  }
}
