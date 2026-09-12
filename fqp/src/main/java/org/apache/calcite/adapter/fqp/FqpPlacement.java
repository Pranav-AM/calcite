/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

/** A logical tree with execution locations selected by Calcite's cost search. */
public final class FqpPlacement {
  private final RelNode root;
  private final Map<RelNode, String> destinations;
  private final double cost;

  FqpPlacement(RelNode root, Map<RelNode, String> destinations, double cost) {
    this.root = root;
    this.destinations = Collections.unmodifiableMap(new IdentityHashMap<>(destinations));
    this.cost = cost;
  }

  public RelNode root() {
    return root;
  }

  public Map<RelNode, String> destinations() {
    return destinations;
  }

  /** Sum of local work and movement, including the root response to Calcite. */
  public double cost() {
    return cost;
  }
}
