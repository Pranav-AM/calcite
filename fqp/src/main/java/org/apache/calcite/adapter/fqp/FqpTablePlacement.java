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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Location and destination-specific names for one logical table. */
public final class FqpTablePlacement {
  private final List<String> logicalName;
  private final String sourceId;
  private final Map<String, List<String>> visibleNames;

  public FqpTablePlacement(List<String> logicalName, String sourceId,
      Map<String, List<String>> visibleNames) {
    this.logicalName = FqpPlanningConfig.copyName(logicalName);
    this.sourceId = FqpDestination.requireNonBlank(sourceId, "sourceId");
    Objects.requireNonNull(visibleNames, "visibleNames");
    final Map<String, List<String>> names = new LinkedHashMap<>();
    for (Map.Entry<String, List<String>> entry : visibleNames.entrySet()) {
      names.put(FqpDestination.requireNonBlank(entry.getKey(), "destinationId"),
          FqpPlanningConfig.copyName(entry.getValue()));
    }
    if (!names.containsKey(sourceId)) {
      throw new IllegalArgumentException(
          "visibleNames must contain the owning source " + sourceId);
    }
    this.visibleNames = Collections.unmodifiableMap(names);
  }

  public List<String> logicalName() {
    return logicalName;
  }

  public String sourceId() {
    return sourceId;
  }

  /** Returns the table name visible when executing at {@code destinationId}. */
  public Optional<List<String>> visibleName(String destinationId) {
    return Optional.ofNullable(visibleNames.get(destinationId));
  }
}
