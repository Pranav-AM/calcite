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

import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable executable fragment selected for a particular FQP destination. */
public final class FqpFragment {
  private final FqpDestination destination;
  private final FqpFragmentPayload payload;
  private final RelDataType rowType;
  private final Set<String> sourceIds;
  private final List<FqpExchangeRequirement> exchanges;

  public FqpFragment(FqpDestination destination, String sql, RelDataType rowType,
      Set<String> sourceIds, List<FqpExchangeRequirement> exchanges) {
    this(destination, FqpFragmentPayload.sql(sql), rowType, sourceIds, exchanges);
  }

  public FqpFragment(FqpDestination destination, FqpFragmentPayload payload,
      RelDataType rowType, Set<String> sourceIds, List<FqpExchangeRequirement> exchanges) {
    this.destination = Objects.requireNonNull(destination, "destination");
    this.payload = Objects.requireNonNull(payload, "payload");
    if (!destination.capabilities().supports(payload.format())) {
      throw new IllegalArgumentException("destination " + destination.sourceId()
          + " does not support " + payload.format());
    }
    this.rowType = Objects.requireNonNull(rowType, "rowType");
    this.sourceIds = Collections.unmodifiableSet(new LinkedHashSet<>(sourceIds));
    this.exchanges = Collections.unmodifiableList(new ArrayList<>(exchanges));
  }

  public FqpDestination destination() {
    return destination;
  }

  public String sql() {
    if (payload.format() != FqpFragmentPayload.Format.SQL) {
      throw new IllegalStateException("fragment payload is " + payload.format() + ", not SQL");
    }
    return payload.utf8Text();
  }

  public FqpFragmentPayload payload() {
    return payload;
  }

  public RelDataType rowType() {
    return rowType;
  }

  public Set<String> sourceIds() {
    return sourceIds;
  }

  public List<FqpExchangeRequirement> exchanges() {
    return exchanges;
  }
}
