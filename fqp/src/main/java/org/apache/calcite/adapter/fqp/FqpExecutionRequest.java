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

/** Request sent to an external coordinator for fragment execution and exchanges. */
public final class FqpExecutionRequest {
  private final String coordinatorSourceId;
  private final FqpFragment fragment;

  public FqpExecutionRequest(String coordinatorSourceId, FqpFragment fragment) {
    this.coordinatorSourceId =
        FqpDestination.requireNonBlank(coordinatorSourceId, "coordinatorSourceId");
    this.fragment = Objects.requireNonNull(fragment, "fragment");
  }

  public String coordinatorSourceId() {
    return coordinatorSourceId;
  }

  public FqpFragment fragment() {
    return fragment;
  }
}
