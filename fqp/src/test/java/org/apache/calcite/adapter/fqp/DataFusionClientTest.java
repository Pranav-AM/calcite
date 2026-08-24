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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataFusionClientTest {
  @Test void executesSubstraitAndConsumesArrowStream() throws Exception {
    try (TestServer server = new TestServer()) {
      final FqpPlanningConfig config = config(server.uri("/v1/execute"), server.uri("/v1/cost"));
      final FqpDestination destination = config.destination("df1").get();
      final FqpFragment fragment = new FqpFragment(destination,
          FqpFragmentPayload.binary(FqpFragmentPayload.Format.SUBSTRAIT_BINARY,
              new byte[] {1, 2, 3}), new JavaTypeFactoryImpl().builder().build(),
          Collections.singleton("df1"), Collections.emptyList());

      assertEquals(42, new DataFusionFragmentExecutor(config).execute(fragment)
          .toList().get(0)[0]);
      assertArrayEquals(new byte[] {1, 2, 3}, server.executionPayload);
    }
  }

  @Test void costsBinarySubstraitThroughDestinationEndpoint() throws Exception {
    try (TestServer server = new TestServer()) {
      final RemoteCostResult result = new DataFusionCostClient(config(server.uri("/v1/execute"),
          server.uri("/v1/cost"))).explain(new RemoteCostRequest("coordinator", "df1",
              FqpFragmentPayload.binary(FqpFragmentPayload.Format.SUBSTRAIT_BINARY,
                  new byte[] {9}), Duration.ofSeconds(1)));

      assertTrue(result.isSuccess());
      assertEquals(12D, result.estimate().get().getTotalCost());
      assertEquals(100D, result.estimate().get().getRowCount());
    }
  }

  private static FqpPlanningConfig config(URI execution, URI cost) {
    final FqpDestinationCapabilities capabilities = FqpDestinationCapabilities.of(
        EnumSet.of(FqpFragmentPayload.Format.SUBSTRAIT_BINARY), execution, cost, null);
    return FqpPlanningConfig.of("coordinator", Collections.singletonList(
        new FqpDestination("df1", DuckDBSqlDialect.DEFAULT, capabilities)),
        Collections.emptyList(), 0.01D, true);
  }

  private static final class TestServer implements AutoCloseable {
    private final HttpServer server;
    private volatile byte[] executionPayload;

    TestServer() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/v1/execute", this::execute);
      server.createContext("/v1/cost", this::cost);
      server.start();
    }

    URI uri(String path) {
      return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
    }

    private void execute(HttpExchange exchange) throws IOException {
      executionPayload = readAll(exchange.getRequestBody());
      exchange.getResponseHeaders().set("Content-Type", "application/vnd.apache.arrow.stream");
      exchange.sendResponseHeaders(200, 0);
      try (RootAllocator allocator = new RootAllocator();
           IntVector vector = new IntVector("answer", allocator)) {
        vector.allocateNew(1);
        vector.set(0, 42);
        vector.setValueCount(1);
        try (VectorSchemaRoot root = VectorSchemaRoot.of(vector);
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null,
                 exchange.getResponseBody())) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }
      }
    }

    private void cost(HttpExchange exchange) throws IOException {
      readAll(exchange.getRequestBody());
      final byte[] response = "{\"startup_cost\":2,\"total_cost\":12,\"row_count\":100,\"row_width\":8}"
          .getBytes(java.nio.charset.StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, response.length);
      exchange.getResponseBody().write(response);
      exchange.close();
    }

    @Override public void close() {
      server.stop(0);
    }

    private static byte[] readAll(java.io.InputStream input) throws IOException {
      final java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
      final byte[] buffer = new byte[1024];
      int count;
      while ((count = input.read(buffer)) >= 0) {
        output.write(buffer, 0, count);
      }
      return output.toByteArray();
    }
  }
}
