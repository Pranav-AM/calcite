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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FqpFragmentPayloadTest {
  @Test void destinationExposesPayloadAndEndpointCapabilities() {
    final FqpDestinationCapabilities capabilities = FqpDestinationCapabilities.of(
        EnumSet.of(FqpFragmentPayload.Format.SQL,
            FqpFragmentPayload.Format.SUBSTRAIT_BINARY),
        URI.create("http://df1:8080/v1/execute"), URI.create("http://df1:8080/v1/cost"),
        URI.create("grpc://df1:32010"));
    final FqpDestination destination = new FqpDestination("df1", DuckDBSqlDialect.DEFAULT,
        capabilities);

    assertTrue(destination.capabilities().supports(FqpFragmentPayload.Format.SUBSTRAIT_BINARY));
    assertEquals("http://df1:8080/v1/execute",
        destination.capabilities().executionEndpoint().get().toString());
    assertEquals("grpc://df1:32010", destination.capabilities().flightEndpoint().get().toString());
  }

  @Test void fragmentRequiresDestinationPayloadSupport() {
    final FqpDestination destination = new FqpDestination("sql", DuckDBSqlDialect.DEFAULT);
    assertThrows(IllegalArgumentException.class, () -> new FqpFragment(destination,
        FqpFragmentPayload.binary(FqpFragmentPayload.Format.SUBSTRAIT_BINARY, new byte[] {1}),
        new JavaTypeFactoryImpl().builder().build(), Collections.emptySet(),
        Collections.emptyList()));
  }

  @Test void binaryPayloadIsDefensivelyCopied() {
    final byte[] bytes = {1, 2};
    final FqpFragmentPayload payload = FqpFragmentPayload.binary(
        FqpFragmentPayload.Format.SUBSTRAIT_BINARY, bytes);
    bytes[0] = 3;
    assertEquals(1, payload.bytes()[0]);
    assertThrows(IllegalStateException.class, payload::utf8Text);
  }
}
