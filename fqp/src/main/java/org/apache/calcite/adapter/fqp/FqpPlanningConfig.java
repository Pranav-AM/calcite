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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Immutable placement and policy configuration for one FQP planning request. */
public final class FqpPlanningConfig {
  private final String coordinatorSourceId;
  private final Map<String, FqpDestination> destinations;
  private final Map<List<String>, FqpTablePlacement> placements;
  private final double movementCostFactor;
  private final boolean allowRemoteRoot;
  private final Duration remoteCostTimeout;

  private FqpPlanningConfig(String coordinatorSourceId,
      Collection<FqpDestination> destinations,
      Collection<FqpTablePlacement> placements, double movementCostFactor,
      boolean allowRemoteRoot, Duration remoteCostTimeout) {
    this.coordinatorSourceId =
        FqpDestination.requireNonBlank(coordinatorSourceId, "coordinatorSourceId");
    if (movementCostFactor < 0D) {
      throw new IllegalArgumentException("movementCostFactor must not be negative");
    }
    this.movementCostFactor = movementCostFactor;
    this.allowRemoteRoot = allowRemoteRoot;
    this.remoteCostTimeout = Objects.requireNonNull(remoteCostTimeout, "remoteCostTimeout");
    if (remoteCostTimeout.isNegative() || remoteCostTimeout.isZero()) {
      throw new IllegalArgumentException("remoteCostTimeout must be positive");
    }
    this.destinations = indexDestinations(destinations);
    this.placements = indexPlacements(placements);
  }

  public static FqpPlanningConfig of(String coordinatorSourceId,
      Collection<FqpDestination> destinations,
      Collection<FqpTablePlacement> placements, double movementCostFactor,
      boolean allowRemoteRoot) {
    return new FqpPlanningConfig(coordinatorSourceId, destinations, placements,
        movementCostFactor, allowRemoteRoot, Duration.ofSeconds(30));
  }

  public static FqpPlanningConfig of(String coordinatorSourceId,
      Collection<FqpDestination> destinations,
      Collection<FqpTablePlacement> placements, double movementCostFactor,
      boolean allowRemoteRoot, Duration remoteCostTimeout) {
    return new FqpPlanningConfig(coordinatorSourceId, destinations, placements,
        movementCostFactor, allowRemoteRoot, remoteCostTimeout);
  }

  public String coordinatorSourceId() {
    return coordinatorSourceId;
  }

  public Optional<FqpDestination> destination(String sourceId) {
    return Optional.ofNullable(destinations.get(sourceId));
  }

  public Optional<FqpTablePlacement> placement(List<String> logicalName) {
    return Optional.ofNullable(placements.get(copyName(logicalName)));
  }

  public double movementCostFactor() {
    return movementCostFactor;
  }

  public boolean allowRemoteRoot() {
    return allowRemoteRoot;
  }

  public Duration remoteCostTimeout() {
    return remoteCostTimeout;
  }

  private static Map<String, FqpDestination> indexDestinations(
      Collection<FqpDestination> values) {
    Objects.requireNonNull(values, "destinations");
    final Map<String, FqpDestination> result = new LinkedHashMap<>();
    for (FqpDestination value : values) {
      if (result.put(value.sourceId(), value) != null) {
        throw new IllegalArgumentException("duplicate destination: " + value.sourceId());
      }
    }
    return Collections.unmodifiableMap(result);
  }

  private static Map<List<String>, FqpTablePlacement> indexPlacements(
      Collection<FqpTablePlacement> values) {
    Objects.requireNonNull(values, "placements");
    final Map<List<String>, FqpTablePlacement> result = new LinkedHashMap<>();
    for (FqpTablePlacement value : values) {
      final List<String> name = value.logicalName();
      if (result.put(name, value) != null) {
        throw new IllegalArgumentException("duplicate table placement: " + name);
      }
    }
    return Collections.unmodifiableMap(result);
  }

  static List<String> copyName(List<String> name) {
    Objects.requireNonNull(name, "name");
    if (name.isEmpty()) {
      throw new IllegalArgumentException("table name must not be empty");
    }
    final List<String> copy = new ArrayList<>(name.size());
    for (String part : name) {
      copy.add(FqpDestination.requireNonBlank(part, "table name part"));
    }
    return Collections.unmodifiableList(copy);
  }
}
