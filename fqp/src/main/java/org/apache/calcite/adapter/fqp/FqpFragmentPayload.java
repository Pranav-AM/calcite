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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Immutable executable representation of an FQP fragment. */
public final class FqpFragmentPayload {
  /** Wire formats understood by FQP execution endpoints. */
  public enum Format {
    SQL,
    SUBSTRAIT_BINARY,
    SUBSTRAIT_JSON
  }

  private final Format format;
  private final byte[] bytes;

  private FqpFragmentPayload(Format format, byte[] bytes) {
    this.format = Objects.requireNonNull(format, "format");
    this.bytes = Arrays.copyOf(Objects.requireNonNull(bytes, "bytes"), bytes.length);
  }

  public static FqpFragmentPayload sql(String sql) {
    return new FqpFragmentPayload(Format.SQL,
        FqpDestination.requireNonBlank(sql, "sql").getBytes(StandardCharsets.UTF_8));
  }

  public static FqpFragmentPayload binary(Format format, byte[] bytes) {
    if (format == Format.SQL) {
      throw new IllegalArgumentException("use sql(String) for SQL payloads");
    }
    return new FqpFragmentPayload(format, bytes);
  }

  public Format format() {
    return format;
  }

  public byte[] bytes() {
    return Arrays.copyOf(bytes, bytes.length);
  }

  public String utf8Text() {
    if (format == Format.SUBSTRAIT_BINARY) {
      throw new IllegalStateException("binary Substrait payload has no UTF-8 text form");
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
