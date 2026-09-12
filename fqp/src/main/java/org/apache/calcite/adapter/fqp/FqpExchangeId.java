/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

/** Stable identifier for one data exchange in a distributed plan. */
public final class FqpExchangeId {
  private final String value;

  public FqpExchangeId(String value) {
    this.value = FqpDestination.requireNonBlank(value, "exchangeId");
  }

  public String value() {
    return value;
  }

  @Override public boolean equals(Object obj) {
    return this == obj || obj instanceof FqpExchangeId
        && value.equals(((FqpExchangeId) obj).value);
  }

  @Override public int hashCode() {
    return value.hashCode();
  }

  @Override public String toString() {
    return value;
  }
}
