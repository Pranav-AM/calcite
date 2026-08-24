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

import java.util.Objects;
import java.util.Optional;

/** Result of remote fragment costing; failures never fabricate an estimate. */
public final class RemoteCostResult {
  private final RemoteCostEstimate estimate;
  private final String failureMessage;

  private RemoteCostResult(RemoteCostEstimate estimate, String failureMessage) {
    this.estimate = estimate;
    this.failureMessage = failureMessage;
  }

  public static RemoteCostResult success(RemoteCostEstimate estimate) {
    return new RemoteCostResult(Objects.requireNonNull(estimate, "estimate"), null);
  }

  public static RemoteCostResult failure(String message) {
    return new RemoteCostResult(null, FqpDestination.requireNonBlank(message, "message"));
  }

  public boolean isSuccess() {
    return estimate != null;
  }

  public Optional<RemoteCostEstimate> estimate() {
    return Optional.ofNullable(estimate);
  }

  public Optional<String> failureMessage() {
    return Optional.ofNullable(failureMessage);
  }
}
