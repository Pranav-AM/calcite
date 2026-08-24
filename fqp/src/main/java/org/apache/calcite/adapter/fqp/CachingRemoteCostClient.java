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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/** Planning-session cache for remote estimates. */
public final class CachingRemoteCostClient implements RemoteCostClient {
  private final RemoteCostClient delegate;
  private final FqpPlanningMetrics metrics;
  private final Map<Key, RemoteCostResult> cache = new ConcurrentHashMap<>();

  public CachingRemoteCostClient(RemoteCostClient delegate) {
    this(delegate, new FqpPlanningMetrics());
  }

  CachingRemoteCostClient(RemoteCostClient delegate, FqpPlanningMetrics metrics) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
  }

  @Override public RemoteCostResult explain(RemoteCostRequest request) {
    final Key key = new Key(request);
    metrics.recordCostRequest();
    final RemoteCostResult cached = cache.get(key);
    if (cached != null) {
      metrics.recordCacheHit();
      return cached;
    }
    return cache.computeIfAbsent(key, ignored -> {
      final long started = System.nanoTime();
      final RemoteCostResult result = delegate.explain(request);
      metrics.recordRemoteCall(System.nanoTime() - started);
      return result;
    });
  }

  /** Number of distinct request keys in this planning session. */
  public int size() {
    return cache.size();
  }

  public FqpPlanningMetrics metrics() {
    return metrics;
  }

  private static final class Key {
    private final String caller;
    private final String destination;
    private final FqpFragmentPayload.Format format;
    private final byte[] payload;
    private final long timeoutMillis;

    Key(RemoteCostRequest request) {
      this.caller = request.callerSourceId();
      this.destination = request.destinationSourceId();
      this.format = request.payload().format();
      this.payload = request.payload().bytes();
      this.timeoutMillis = request.timeout().toMillis();
    }

    @Override public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof Key)) {
        return false;
      }
      final Key that = (Key) obj;
      return timeoutMillis == that.timeoutMillis && caller.equals(that.caller)
          && destination.equals(that.destination) && format == that.format
          && java.util.Arrays.equals(payload, that.payload);
    }

    @Override public int hashCode() {
      int result = Objects.hash(caller, destination, format, timeoutMillis);
      result = 31 * result + java.util.Arrays.hashCode(payload);
      return result;
    }
  }
}
