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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachingRemoteCostClientTest {
  @Test void cachesEquivalentPlanningRequest() {
    final AtomicInteger calls = new AtomicInteger();
    final CachingRemoteCostClient client = new CachingRemoteCostClient(request -> {
      calls.incrementAndGet();
      return RemoteCostResult.success(new RemoteCostEstimate(2D, 5D, 10D, 8));
    });
    final RemoteCostRequest request = new RemoteCostRequest("coordinator", "pg1",
        "SELECT * FROM \"public\".\"orders\"", Duration.ofSeconds(1));

    assertTrue(client.explain(request).isSuccess());
    assertTrue(client.explain(request).isSuccess());
    assertEquals(1, calls.get());
    assertEquals(1, client.size());
    assertEquals(2, client.metrics().costRequests());
    assertEquals(1, client.metrics().cacheHits());
    assertEquals(1, client.metrics().remoteCalls());
  }
}
