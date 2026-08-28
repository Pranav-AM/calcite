/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.calcite.adapter.fqp;

import java.net.URI;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Execution and transport features advertised by one FQP destination. */
public final class FqpDestinationCapabilities {
  private final Set<FqpFragmentPayload.Format> payloadFormats;
  private final URI executionEndpoint;
  private final URI costEndpoint;
  private final URI flightEndpoint;

  private FqpDestinationCapabilities(Set<FqpFragmentPayload.Format> payloadFormats,
      URI executionEndpoint, URI costEndpoint, URI flightEndpoint) {
    if (payloadFormats.isEmpty()) {
      throw new IllegalArgumentException("payloadFormats must not be empty");
    }
    this.payloadFormats = Collections.unmodifiableSet(EnumSet.copyOf(payloadFormats));
    this.executionEndpoint = executionEndpoint;
    this.costEndpoint = costEndpoint;
    this.flightEndpoint = flightEndpoint;
  }

  public static FqpDestinationCapabilities substraitOnly() {
    return of(EnumSet.of(FqpFragmentPayload.Format.SUBSTRAIT_BINARY), null, null, null);
  }

  public static FqpDestinationCapabilities of(Set<FqpFragmentPayload.Format> payloadFormats,
      URI executionEndpoint, URI costEndpoint, URI flightEndpoint) {
    return new FqpDestinationCapabilities(Objects.requireNonNull(payloadFormats, "payloadFormats"),
        executionEndpoint, costEndpoint, flightEndpoint);
  }

  public boolean supports(FqpFragmentPayload.Format format) {
    return payloadFormats.contains(format);
  }

  public Set<FqpFragmentPayload.Format> payloadFormats() {
    return payloadFormats;
  }

  public Optional<URI> executionEndpoint() {
    return Optional.ofNullable(executionEndpoint);
  }

  public Optional<URI> costEndpoint() {
    return Optional.ofNullable(costEndpoint);
  }

  public Optional<URI> flightEndpoint() {
    return Optional.ofNullable(flightEndpoint);
  }
}
