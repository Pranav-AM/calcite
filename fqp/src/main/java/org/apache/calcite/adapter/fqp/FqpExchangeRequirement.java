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

/** A future data movement operation required by an FQP fragment. */
public final class FqpExchangeRequirement {
  private final String sourceId;
  private final String destinationId;
  private final double estimatedBytes;

  public FqpExchangeRequirement(String sourceId, String destinationId,
      double estimatedBytes) {
    this.sourceId = FqpDestination.requireNonBlank(sourceId, "sourceId");
    this.destinationId =
        FqpDestination.requireNonBlank(destinationId, "destinationId");
    if (estimatedBytes < 0D) {
      throw new IllegalArgumentException("estimatedBytes must not be negative");
    }
    this.estimatedBytes = estimatedBytes;
  }

  public String sourceId() {
    return sourceId;
  }

  public String destinationId() {
    return destinationId;
  }

  public double estimatedBytes() {
    return estimatedBytes;
  }
}
