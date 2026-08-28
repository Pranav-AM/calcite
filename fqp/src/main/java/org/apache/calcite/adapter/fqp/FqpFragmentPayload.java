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

import java.util.Arrays;
import java.util.Objects;

/** Immutable executable representation of an FQP fragment. */
public final class FqpFragmentPayload {
  /** Wire formats understood by FQP execution endpoints. */
  public enum Format {
    SUBSTRAIT_BINARY
  }

  private final Format format;
  private final byte[] bytes;

  private FqpFragmentPayload(Format format, byte[] bytes) {
    this.format = Objects.requireNonNull(format, "format");
    this.bytes = Arrays.copyOf(Objects.requireNonNull(bytes, "bytes"), bytes.length);
  }

  public static FqpFragmentPayload binary(Format format, byte[] bytes) {
    return new FqpFragmentPayload(format, bytes);
  }

  public Format format() {
    return format;
  }

  public byte[] bytes() {
    return Arrays.copyOf(bytes, bytes.length);
  }

}
