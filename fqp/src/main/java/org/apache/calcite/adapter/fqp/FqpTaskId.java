/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

/** Stable identifier for one task in a selected distributed plan. */
public final class FqpTaskId {
  private final String value;

  public FqpTaskId(String value) {
    this.value = FqpDestination.requireNonBlank(value, "taskId");
  }

  public String value() {
    return value;
  }

  @Override public boolean equals(Object obj) {
    return this == obj || obj instanceof FqpTaskId
        && value.equals(((FqpTaskId) obj).value);
  }

  @Override public int hashCode() {
    return value.hashCode();
  }

  @Override public String toString() {
    return value;
  }
}
