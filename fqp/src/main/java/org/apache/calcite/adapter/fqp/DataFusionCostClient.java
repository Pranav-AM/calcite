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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Obtains normalized estimates from an endpoint-local DataFusion worker. */
public final class DataFusionCostClient implements RemoteCostClient {
  private final FqpPlanningConfig config;
  private final ObjectMapper mapper;

  public DataFusionCostClient(FqpPlanningConfig config) {
    this(config, new ObjectMapper());
  }

  DataFusionCostClient(FqpPlanningConfig config, ObjectMapper mapper) {
    this.config = Objects.requireNonNull(config, "config");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
  }

  @Override public RemoteCostResult explain(RemoteCostRequest request) {
    final FqpDestination destination = config.destination(request.destinationSourceId())
        .orElse(null);
    if (destination == null || !destination.capabilities().supports(request.payload().format())
        || !destination.capabilities().costEndpoint().isPresent()) {
      return RemoteCostResult.failure("no compatible DataFusion cost endpoint for "
          + request.destinationSourceId());
    }
    HttpURLConnection connection = null;
    try {
      final URI endpoint = destination.capabilities().costEndpoint().get();
      connection = (HttpURLConnection) endpoint.toURL().openConnection();
      final int timeout = Math.toIntExact(Math.min(Integer.MAX_VALUE,
          request.timeout().toMillis()));
      connection.setConnectTimeout(timeout);
      connection.setReadTimeout(timeout);
      connection.setRequestMethod("POST");
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", contentType(request.payload().format()));
      connection.setRequestProperty("X-Fqp-Caller-Source-Id", request.callerSourceId());
      final byte[] payload = request.payload().bytes();
      connection.setFixedLengthStreamingMode(payload.length);
      try (OutputStream stream = connection.getOutputStream()) {
        stream.write(payload);
      }
      final int status = connection.getResponseCode();
      final String response = read(connection, status);
      if (status != HttpURLConnection.HTTP_OK) {
        return RemoteCostResult.failure("DataFusion cost endpoint returned HTTP " + status);
      }
      final JsonNode node = mapper.readTree(response);
      if (!node.path("startup_cost").isNumber() || !node.path("total_cost").isNumber()) {
        return RemoteCostResult.failure("DataFusion cost response has no numeric costs");
      }
      return RemoteCostResult.success(new RemoteCostEstimate(
          node.path("startup_cost").doubleValue(), node.path("total_cost").doubleValue(),
          node.path("row_count").isNumber() ? node.path("row_count").doubleValue() : 0D,
          node.path("row_width").isNumber() ? node.path("row_width").intValue() : 0));
    } catch (IOException | ArithmeticException e) {
      return RemoteCostResult.failure("DataFusion cost request failed: " + e.getMessage());
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static String contentType(FqpFragmentPayload.Format format) {
    return format == FqpFragmentPayload.Format.SUBSTRAIT_BINARY
        ? "application/vnd.substrait.plan" : "application/json";
  }

  private static String read(HttpURLConnection connection, int status) throws IOException {
    final java.io.InputStream input = status >= 400 ? connection.getErrorStream()
        : connection.getInputStream();
    if (input == null) {
      return "";
    }
    try (BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(input,
        StandardCharsets.UTF_8))) {
      final StringBuilder result = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        result.append(line);
      }
      return result.toString();
    }
  }
}
