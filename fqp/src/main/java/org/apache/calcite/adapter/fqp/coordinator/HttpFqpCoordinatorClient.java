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
package org.apache.calcite.adapter.fqp.coordinator;

import org.apache.calcite.adapter.fqp.FqpCoordinatorClient;
import org.apache.calcite.adapter.fqp.FqpExchangeRequirement;
import org.apache.calcite.adapter.fqp.FqpExecutionException;
import org.apache.calcite.adapter.fqp.FqpExecutionRequest;
import org.apache.calcite.adapter.fqp.FqpExecutionResult;
import org.apache.calcite.adapter.fqp.FqpFragmentPayload;
import org.apache.calcite.adapter.fqp.execution.ArrowRows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Objects;

/** Sends a binary Substrait fragment to a coordinator and consumes Arrow rows. */
public final class HttpFqpCoordinatorClient implements FqpCoordinatorClient {
  private final URI endpoint;

  public HttpFqpCoordinatorClient(URI endpoint) {
    this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
  }

  @Override public FqpExecutionResult execute(FqpExecutionRequest request) {
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) endpoint.toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setDoOutput(true);
      connection.setRequestProperty("Content-Type", "application/vnd.substrait.plan");
      connection.setRequestProperty("Accept", "application/vnd.apache.arrow.stream");
      connection.setRequestProperty("X-Fqp-Coordinator-Source-Id",
          request.coordinatorSourceId());
      connection.setRequestProperty("X-Fqp-Destination-Source-Id",
          request.fragment().destination().sourceId());
      for (FqpExchangeRequirement exchange : request.fragment().exchanges()) {
        connection.addRequestProperty("X-Fqp-Exchange", exchange.sourceId() + ","
            + exchange.destinationId() + "," + exchange.estimatedBytes());
      }
      if (request.fragment().payload().format()
          != FqpFragmentPayload.Format.SUBSTRAIT_BINARY) {
        throw new FqpExecutionException("coordinator requires binary Substrait");
      }
      final byte[] payload = request.fragment().payload().bytes();
      connection.setFixedLengthStreamingMode(payload.length);
      try (OutputStream stream = connection.getOutputStream()) {
        stream.write(payload);
      }
      final int status = connection.getResponseCode();
      if (status != HttpURLConnection.HTTP_OK) {
        throw new FqpExecutionException("coordinator returned HTTP " + status);
      }
      return new FqpExecutionResult(ArrowRows.read(readResponse(connection)));
    } catch (IOException e) {
      throw new FqpExecutionException("coordinator execution failed", e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private static byte[] readResponse(HttpURLConnection connection) throws IOException {
    try (InputStream input = connection.getInputStream();
         ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      final byte[] buffer = new byte[8192];
      int count;
      while ((count = input.read(buffer)) >= 0) {
        output.write(buffer, 0, count);
      }
      return output.toByteArray();
    }
  }
}
