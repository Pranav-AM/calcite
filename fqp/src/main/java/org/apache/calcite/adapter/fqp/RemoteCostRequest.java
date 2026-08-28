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
import java.util.Objects;

/** A relay request for one destination-specific executable fragment. */
public final class RemoteCostRequest {
  private final String callerSourceId;
  private final String destinationSourceId;
  private final FqpFragmentPayload payload;
  private final Duration timeout;

  public RemoteCostRequest(String callerSourceId, String destinationSourceId,
      FqpFragmentPayload payload, Duration timeout) {
    this.callerSourceId =
        FqpDestination.requireNonBlank(callerSourceId, "callerSourceId");
    this.destinationSourceId =
        FqpDestination.requireNonBlank(destinationSourceId, "destinationSourceId");
    this.payload = Objects.requireNonNull(payload, "payload");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative() || timeout.isZero()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
  }

  public String callerSourceId() {
    return callerSourceId;
  }

  public String destinationSourceId() {
    return destinationSourceId;
  }

  public FqpFragmentPayload payload() {
    return payload;
  }

  public Duration timeout() {
    return timeout;
  }
}
