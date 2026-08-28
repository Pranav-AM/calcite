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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/** Per-planning-request counters for remote-cost instrumentation. */
public final class FqpPlanningMetrics {
  private final AtomicLong costRequests = new AtomicLong();
  private final AtomicLong cacheHits = new AtomicLong();
  private final AtomicLong remoteCalls = new AtomicLong();
  private final AtomicLong remoteNanos = new AtomicLong();

  public void recordCostRequest() {
    costRequests.incrementAndGet();
  }

  public void recordCacheHit() {
    cacheHits.incrementAndGet();
  }

  public void recordRemoteCall(long elapsedNanos) {
    remoteCalls.incrementAndGet();
    remoteNanos.addAndGet(elapsedNanos);
  }

  public long costRequests() {
    return costRequests.get();
  }

  public long cacheHits() {
    return cacheHits.get();
  }

  public long remoteCalls() {
    return remoteCalls.get();
  }

  public Duration remoteTime() {
    return Duration.ofNanos(remoteNanos.get());
  }
}
