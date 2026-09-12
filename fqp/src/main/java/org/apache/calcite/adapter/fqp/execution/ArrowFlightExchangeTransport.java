/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpExchange;
import org.apache.calcite.adapter.fqp.FqpExecutionException;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.SyncPutListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Uploads exchange data to the consumer using Arrow Flight DoPut. */
public final class ArrowFlightExchangeTransport implements FqpExchangeTransport,
    AutoCloseable {
  private final BufferAllocator allocator;
  private final Duration timeout;

  public ArrowFlightExchangeTransport() {
    this(new RootAllocator());
  }

  public ArrowFlightExchangeTransport(BufferAllocator allocator) {
    this(allocator, Duration.ofSeconds(30));
  }

  public ArrowFlightExchangeTransport(BufferAllocator allocator, Duration timeout) {
    this.allocator = Objects.requireNonNull(allocator, "allocator");
    this.timeout = Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative() || timeout.isZero()) {
      throw new IllegalArgumentException("Flight timeout must be positive");
    }
  }

  @Override public void transfer(FqpExchange exchange, FqpDestination destination,
      byte[] arrowStream) {
    Objects.requireNonNull(exchange, "exchange");
    Objects.requireNonNull(destination, "destination");
    Objects.requireNonNull(arrowStream, "arrowStream");
    if (!exchange.destinationId().equals(destination.sourceId())) {
      throw new FqpExecutionException("exchange destination mismatch: " + exchange.id());
    }
    final URI endpoint = destination.capabilities().flightEndpoint()
        .orElseThrow(() -> new FqpExecutionException("destination has no Flight endpoint: "
            + destination.sourceId()));
    final Location location = location(endpoint);
    final List<String> path = new ArrayList<>(exchange.temporaryInputName());
    try (FlightClient client = FlightClient.builder(allocator, location).build();
         ArrowStreamReader reader = new ArrowStreamReader(
             new ByteArrayInputStream(arrowStream), allocator)) {
      final VectorSchemaRoot root = reader.getVectorSchemaRoot();
      ArrowExchangeSchema.validate(exchange.rowType(), root.getSchema());
      final SyncPutListener acknowledgements = new SyncPutListener();
      final FlightClient.ClientStreamListener stream = client.startPut(
          FlightDescriptor.path(path), root, acknowledgements,
          CallOptions.timeout(timeout.toMillis(), TimeUnit.MILLISECONDS));
      try {
        while (reader.loadNextBatch()) {
          stream.putNext();
        }
        stream.completed();
        acknowledgements.getResult();
      } catch (Exception e) {
        stream.error(e);
        throw e;
      }
    } catch (Exception e) {
      throw new FqpExecutionException("Flight exchange " + exchange.id()
          + " to " + destination.sourceId() + " failed", e);
    }
  }

  private static Location location(URI endpoint) {
    final String scheme = endpoint.getScheme();
    if ("grpc+tls".equalsIgnoreCase(scheme)) {
      return Location.forGrpcTls(endpoint.getHost(), endpoint.getPort());
    }
    if ("grpc".equalsIgnoreCase(scheme)) {
      return Location.forGrpcInsecure(endpoint.getHost(), endpoint.getPort());
    }
    throw new FqpExecutionException("unsupported Flight endpoint: " + endpoint);
  }

  @Override public void close() {
    allocator.close();
  }
}
