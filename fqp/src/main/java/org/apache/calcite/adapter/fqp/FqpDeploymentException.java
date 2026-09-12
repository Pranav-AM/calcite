/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

/** Indicates that external FQP deployment configuration is invalid. */
public final class FqpDeploymentException extends RuntimeException {
  public FqpDeploymentException(String message, Throwable cause) {
    super(message, cause);
  }
}
