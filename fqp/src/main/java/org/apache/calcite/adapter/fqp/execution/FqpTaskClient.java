/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpTask;

/** Executes one distributed-plan task and returns an Arrow IPC stream. */
public interface FqpTaskClient {
  byte[] execute(FqpTask task);
}
