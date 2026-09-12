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
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpExecutionException;
import org.apache.calcite.adapter.fqp.FqpFragment;
import org.apache.calcite.adapter.fqp.FqpFragmentExecutor;
import org.apache.calcite.adapter.fqp.FqpFragmentPayload;
import org.apache.calcite.adapter.fqp.FqpPlanningConfig;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/** Executes binary Substrait fragments at an endpoint-local DataFusion worker. */
public final class DataFusionFragmentExecutor implements FqpFragmentExecutor {
  private final FqpPlanningConfig config;

  public DataFusionFragmentExecutor(FqpPlanningConfig config) {
    this.config = Objects.requireNonNull(config, "config");
  }

  @Override public Set<FqpFragmentPayload.Format> supportedPayloadFormats() {
    return Collections.singleton(FqpFragmentPayload.Format.SUBSTRAIT_BINARY);
  }

  @Override public Enumerable<Object[]> execute(FqpFragment fragment) {
    if (!supportedPayloadFormats().contains(fragment.payload().format())) {
      throw new FqpExecutionException("DataFusion executor requires binary Substrait payloads");
    }
    if (!fragment.exchanges().isEmpty()) {
      throw new FqpExecutionException("DataFusion Flight exchanges are not implemented yet");
    }
    final FqpDestination destination = config.destination(fragment.destination().sourceId())
        .orElseThrow(() -> new FqpExecutionException("unknown destination "
            + fragment.destination().sourceId()));
    final URI endpoint = destination.capabilities().executionEndpoint()
        .orElseThrow(() -> new FqpExecutionException("no DataFusion execution endpoint for "
            + destination.sourceId()));
    return Linq4j.asEnumerable(ArrowRows.read(executeArrow(endpoint,
        fragment.payload().bytes())));
  }

  static byte[] executeArrow(URI endpoint, byte[] payload) {
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) endpoint.toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/vnd.substrait.plan");
      connection.setRequestProperty("Accept", "application/vnd.apache.arrow.stream");
      connection.setFixedLengthStreamingMode(payload.length);
      try (OutputStream output = connection.getOutputStream()) {
        output.write(payload);
      }
      if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new FqpExecutionException("DataFusion endpoint returned HTTP "
            + connection.getResponseCode());
      }
      try (InputStream input = connection.getInputStream();
           ByteArrayOutputStream output = new ByteArrayOutputStream()) {
        final byte[] buffer = new byte[8192];
        int count;
        while ((count = input.read(buffer)) >= 0) {
          output.write(buffer, 0, count);
        }
        return output.toByteArray();
      }
    } catch (IOException e) {
      throw new FqpExecutionException("DataFusion fragment execution failed", e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

}
