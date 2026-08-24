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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Minimal JSON coordinator client for executing the exact FQP fragment that was costed. */
public final class HttpFqpCoordinatorClient implements FqpCoordinatorClient {
  private final URI endpoint;
  private final ObjectMapper mapper;

  public HttpFqpCoordinatorClient(URI endpoint) {
    this(endpoint, new ObjectMapper());
  }

  HttpFqpCoordinatorClient(URI endpoint, ObjectMapper mapper) {
    this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
  }

  @Override public FqpExecutionResult execute(FqpExecutionRequest request) {
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) endpoint.toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");
      final byte[] payload = mapper.writeValueAsBytes(toJson(request));
      connection.setFixedLengthStreamingMode(payload.length);
      try (OutputStream stream = connection.getOutputStream()) {
        stream.write(payload);
      }
      final int status = connection.getResponseCode();
      final String response = readResponse(connection, status);
      if (status != HttpURLConnection.HTTP_OK) {
        throw new FqpExecutionException("coordinator returned HTTP " + status);
      }
      return fromJson(response);
    } catch (IOException e) {
      throw new FqpExecutionException("coordinator execution failed", e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static Map<String, Object> toJson(FqpExecutionRequest request) {
    final FqpFragment fragment = request.fragment();
    if (fragment.payload().format() != FqpFragmentPayload.Format.SQL) {
      throw new FqpExecutionException("JSON coordinator supports SQL fragments only");
    }
    final Map<String, Object> result = new LinkedHashMap<>();
    result.put("version", 1);
    result.put("coordinator_source_id", request.coordinatorSourceId());
    result.put("destination_source_id", fragment.destination().sourceId());
    result.put("sql_text", fragment.sql());
    final List<Map<String, Object>> exchanges = new ArrayList<>();
    for (FqpExchangeRequirement exchange : fragment.exchanges()) {
      final Map<String, Object> exchangeJson = new LinkedHashMap<>();
      exchangeJson.put("source_id", exchange.sourceId());
      exchangeJson.put("destination_id", exchange.destinationId());
      exchangeJson.put("estimated_bytes", exchange.estimatedBytes());
      exchanges.add(exchangeJson);
    }
    result.put("exchanges", exchanges);
    return result;
  }

  private FqpExecutionResult fromJson(String response) throws IOException {
    final JsonNode rowsNode = mapper.readTree(response).get("rows");
    if (rowsNode == null || !rowsNode.isArray()) {
      throw new FqpExecutionException("coordinator response has no rows array");
    }
    final List<Object[]> rows = new ArrayList<>();
    for (JsonNode row : rowsNode) {
      if (!row.isArray()) {
        throw new FqpExecutionException("coordinator row is not an array");
      }
      final Object[] values = new Object[row.size()];
      for (int i = 0; i < row.size(); i++) {
        values[i] = mapper.treeToValue(row.get(i), Object.class);
      }
      rows.add(values);
    }
    return new FqpExecutionResult(rows);
  }

  private static String readResponse(HttpURLConnection connection, int status)
      throws IOException {
    final java.io.InputStream stream = status >= 400
        ? connection.getErrorStream() : connection.getInputStream();
    if (stream == null) {
      return "";
    }
    try (BufferedReader reader = new BufferedReader(
        new java.io.InputStreamReader(stream, StandardCharsets.UTF_8))) {
      final StringBuilder result = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        result.append(line);
      }
      return result.toString();
    }
  }
}
