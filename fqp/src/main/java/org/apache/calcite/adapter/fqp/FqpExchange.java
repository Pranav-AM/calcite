/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Objects;

/** Typed producer-to-consumer data movement in a selected distributed plan. */
public final class FqpExchange {
  private final FqpExchangeId id;
  private final FqpTaskId producerTaskId;
  private final FqpTaskId consumerTaskId;
  private final String sourceId;
  private final String destinationId;
  private final RelDataType rowType;
  private final List<String> temporaryInputName;
  private final double estimatedRows;
  private final double estimatedBytes;

  public FqpExchange(FqpExchangeId id, FqpTaskId producerTaskId,
      FqpTaskId consumerTaskId, String sourceId, String destinationId,
      RelDataType rowType, List<String> temporaryInputName,
      double estimatedRows, double estimatedBytes) {
    this.id = Objects.requireNonNull(id, "id");
    this.producerTaskId = Objects.requireNonNull(producerTaskId, "producerTaskId");
    this.consumerTaskId = Objects.requireNonNull(consumerTaskId, "consumerTaskId");
    this.sourceId = FqpDestination.requireNonBlank(sourceId, "sourceId");
    this.destinationId = FqpDestination.requireNonBlank(destinationId, "destinationId");
    this.rowType = Objects.requireNonNull(rowType, "rowType");
    this.temporaryInputName = FqpPlanningConfig.copyName(temporaryInputName);
    if (!Double.isFinite(estimatedRows) || estimatedRows < 0D
        || !Double.isFinite(estimatedBytes) || estimatedBytes < 0D) {
      throw new IllegalArgumentException("exchange estimates must be finite and non-negative");
    }
    this.estimatedRows = estimatedRows;
    this.estimatedBytes = estimatedBytes;
  }

  public FqpExchangeId id() {
    return id;
  }

  public FqpTaskId producerTaskId() {
    return producerTaskId;
  }

  public FqpTaskId consumerTaskId() {
    return consumerTaskId;
  }

  public String sourceId() {
    return sourceId;
  }

  public String destinationId() {
    return destinationId;
  }

  public RelDataType rowType() {
    return rowType;
  }

  public List<String> temporaryInputName() {
    return temporaryInputName;
  }

  public double estimatedRows() {
    return estimatedRows;
  }

  public double estimatedBytes() {
    return estimatedBytes;
  }
}
