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

import org.apache.calcite.adapter.fqp.cost.CachingRemoteCostClient;

import java.util.Objects;

/** Per-optimization-request FQP state.
 *
 * <p>The remote-estimate cache is deliberately added in the remote-costing
 * milestone, so this class can already be passed through planner setup without
 * introducing any network behavior.</p>
 */
public final class FqpPlanningContext {
  private final FqpPlanningConfig config;
  private final RemoteCostClient remoteCostClient;
  private final FqpPlanningMetrics metrics;
  private final FqpFragmentExecutor fragmentExecutor;

  public FqpPlanningContext(FqpPlanningConfig config) {
    this(config, request -> RemoteCostResult.failure("no remote cost client configured"),
        fragment -> {
          throw new FqpExecutionException("no FQP fragment executor configured");
        });
  }

  public FqpPlanningContext(FqpPlanningConfig config, RemoteCostClient remoteCostClient) {
    this(config, remoteCostClient, fragment -> {
      throw new FqpExecutionException("no FQP fragment executor configured");
    });
  }

  public FqpPlanningContext(FqpPlanningConfig config, RemoteCostClient remoteCostClient,
      FqpFragmentExecutor fragmentExecutor) {
    this.config = Objects.requireNonNull(config, "config");
    this.metrics = new FqpPlanningMetrics();
    this.remoteCostClient = new CachingRemoteCostClient(
        Objects.requireNonNull(remoteCostClient, "remoteCostClient"), metrics);
    this.fragmentExecutor = Objects.requireNonNull(fragmentExecutor, "fragmentExecutor");
  }

  public FqpPlanningConfig config() {
    return config;
  }

  /** Returns a cache scoped to this planning context. */
  public RemoteCostClient remoteCostClient() {
    return remoteCostClient;
  }

  public FqpFragmentExecutor fragmentExecutor() {
    return fragmentExecutor;
  }

  public FqpPlanningMetrics metrics() {
    return metrics;
  }
}
