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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** HTTP implementation of the PostgreSQL prototype's relay cost contract. */
public final class HttpRemoteCostClient implements RemoteCostClient {
  private final URI relayUri;
  private final ObjectMapper mapper;

  public HttpRemoteCostClient(URI relayUri) {
    this(relayUri, new ObjectMapper());
  }

  HttpRemoteCostClient(URI relayUri, ObjectMapper mapper) {
    this.relayUri = Objects.requireNonNull(relayUri, "relayUri");
    this.mapper = Objects.requireNonNull(mapper, "mapper");
  }

  @Override public RemoteCostResult explain(RemoteCostRequest request) {
    if (request.payload().format() != FqpFragmentPayload.Format.SQL) {
      return RemoteCostResult.failure("SQL relay cannot cost " + request.payload().format());
    }
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) relayUri.toURL().openConnection();
      final int timeoutMillis = Math.toIntExact(
          Math.min(Integer.MAX_VALUE, request.timeout().toMillis()));
      connection.setConnectTimeout(timeoutMillis);
      connection.setReadTimeout(timeoutMillis);
      connection.setRequestMethod("POST");
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/json");

      final Map<String, Object> body = new LinkedHashMap<>();
      body.put("version", 1);
      body.put("source_id", request.callerSourceId());
      body.put("target_source_id", request.destinationSourceId());
      body.put("sql_text", request.sql());
      body.put("timeout_ms", timeoutMillis);
      final byte[] bytes = mapper.writeValueAsBytes(body);
      connection.setFixedLengthStreamingMode(bytes.length);
      try (OutputStream stream = connection.getOutputStream()) {
        stream.write(bytes);
      }

      final int status = connection.getResponseCode();
      final String response = readResponse(connection, status);
      if (status != HttpURLConnection.HTTP_OK) {
        return RemoteCostResult.failure("remote EXPLAIN returned HTTP " + status);
      }
      return parse(response);
    } catch (IOException | ArithmeticException e) {
      return RemoteCostResult.failure("remote EXPLAIN failed: " + e.getMessage());
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private RemoteCostResult parse(String response) {
    try {
      final JsonNode node = mapper.readTree(response);
      final JsonNode startup = node.get("startup_cost");
      final JsonNode total = node.get("total_cost");
      if (startup == null || total == null || !startup.isNumber() || !total.isNumber()) {
        return RemoteCostResult.failure("remote EXPLAIN response has no numeric costs");
      }
      final JsonNode rows = first(node, "plan_rows", "row_count");
      final JsonNode width = first(node, "plan_width", "row_width");
      return RemoteCostResult.success(new RemoteCostEstimate(startup.doubleValue(),
          total.doubleValue(), rows != null && rows.isNumber() ? rows.doubleValue() : 0D,
          width != null && width.isNumber() ? width.intValue() : 0));
    } catch (IOException e) {
      return RemoteCostResult.failure("invalid remote EXPLAIN JSON: " + e.getMessage());
    }
  }

  private static JsonNode first(JsonNode node, String first, String second) {
    final JsonNode result = node.get(first);
    return result != null ? result : node.get(second);
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
