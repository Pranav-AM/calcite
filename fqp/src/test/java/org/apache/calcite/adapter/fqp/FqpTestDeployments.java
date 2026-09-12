/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import java.io.IOException;
import java.io.InputStream;

/** Shared deployment documents for FQP tests. */
public final class FqpTestDeployments {
  private FqpTestDeployments() {
  }

  public static FqpPlanningConfig threeOwnerTpch() {
    return threeOwnerTpchDeployment().planningConfig();
  }

  public static FqpDeployment threeOwnerTpchDeployment() {
    try (InputStream input = FqpTestDeployments.class.getResourceAsStream(
        "/org/apache/calcite/adapter/fqp/tpch-three-owner.json")) {
      if (input == null) {
        throw new AssertionError("missing test deployment");
      }
      return FqpDeploymentConfigLoader.loadDeployment(input);
    } catch (IOException e) {
      throw new AssertionError("invalid test deployment", e);
    }
  }
}
