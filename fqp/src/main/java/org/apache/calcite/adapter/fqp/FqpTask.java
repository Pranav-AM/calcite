/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** One destination-specific executable task in a distributed plan. */
public final class FqpTask {
  private final FqpTaskId id;
  private final FqpFragment fragment;
  private final Set<List<String>> localTables;

  public FqpTask(FqpTaskId id, FqpFragment fragment,
      Set<List<String>> localTables) {
    this.id = Objects.requireNonNull(id, "id");
    this.fragment = Objects.requireNonNull(fragment, "fragment");
    Objects.requireNonNull(localTables, "localTables");
    final Set<List<String>> tables = new LinkedHashSet<>();
    for (List<String> table : localTables) {
      tables.add(FqpPlanningConfig.copyName(table));
    }
    this.localTables = Collections.unmodifiableSet(tables);
  }

  public FqpTaskId id() {
    return id;
  }

  public FqpFragment fragment() {
    return fragment;
  }

  public FqpDestination destination() {
    return fragment.destination();
  }

  public Set<List<String>> localTables() {
    return localTables;
  }
}
