/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpDestination;
import org.apache.calcite.adapter.fqp.FqpDestinationCapabilities;
import org.apache.calcite.adapter.fqp.FqpExchange;
import org.apache.calcite.adapter.fqp.FqpExchangeId;
import org.apache.calcite.adapter.fqp.FqpExecutionException;
import org.apache.calcite.adapter.fqp.FqpFragmentPayload;
import org.apache.calcite.adapter.fqp.FqpPlanningConfig;
import org.apache.calcite.adapter.fqp.FqpTablePlacement;
import org.apache.calcite.adapter.fqp.FqpTask;
import org.apache.calcite.adapter.fqp.FqpTaskId;
import org.apache.calcite.adapter.fqp.serialization.SubstraitFragmentSerializer;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.dialect.DuckDBSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.SyncPutListener;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Real Java DoPut to Rust MemTable, followed by a Substrait HTTP task. */
@Tag("flightIntegration")
class FlightWorkerIntegrationTest {
  @TempDir Path directory;

  @Test void uploadsMultipleBatchesAndScansTheirSchemaAndValues() throws Exception {
    try (DataFusionTestWorker worker = new DataFusionTestWorker(directory);
         ArrowFlightExchangeTransport transport = new ArrowFlightExchangeTransport()) {
      transport.transfer(exchange(rowType()), worker.destination, stream(false));
      final List<Object[]> rows = ArrowRows.read(scan(worker), rowType());
      assertEquals(3, rows.size());
      assertArrayEquals(new Object[] {1L, "caf\u00e9", new BigDecimal("12.34"),
          9204, true, 1.5D}, rows.get(0));
      assertArrayEquals(new Object[] {null, null, null, null, null, null}, rows.get(1));
      assertArrayEquals(new Object[] {2L, "second", new BigDecimal("-1.25"),
          9205, false, 2.5D}, rows.get(2));
    }
  }

  @Test void preservesSchemaForEmptyUploadsAndEmptyQueryResults() throws Exception {
    try (DataFusionTestWorker worker = new DataFusionTestWorker(directory);
         ArrowFlightExchangeTransport transport = new ArrowFlightExchangeTransport()) {
      transport.transfer(exchange(rowType()), worker.destination, stream(true));
      assertTrue(ArrowRows.read(scan(worker), rowType()).isEmpty());
    }
  }

  @Test void rejectsInvalidDescriptorAtRustReceiver() throws Exception {
    try (DataFusionTestWorker worker = new DataFusionTestWorker(directory);
         RootAllocator allocator = new RootAllocator();
         FlightClient client = FlightClient.builder(allocator,
             Location.forGrpcInsecure("127.0.0.1", worker.flightPort)).build();
         IntVector vector = new IntVector("id", allocator);
         VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
      for (FlightDescriptor descriptor : Arrays.asList(
          FlightDescriptor.command(new byte[] {1}), FlightDescriptor.path(""),
          FlightDescriptor.path("schema", "table"), FlightDescriptor.path("bad.name"))) {
        final SyncPutListener listener = new SyncPutListener();
        final FlightClient.ClientStreamListener upload = client.startPut(descriptor, root,
            listener, CallOptions.timeout(5, TimeUnit.SECONDS));
        upload.completed();
        final FlightRuntimeException error = assertThrows(FlightRuntimeException.class,
            listener::getResult);
        assertEquals(FlightStatusCode.INVALID_ARGUMENT, error.status().code());
      }
    }
  }

  @Test void reportsMissingEndpointsSchemaMismatchesAndTransferFailures() throws Exception {
    final byte[] bytes = stream(false);
    try (DataFusionTestWorker worker = new DataFusionTestWorker(directory);
         ArrowFlightExchangeTransport transport = new ArrowFlightExchangeTransport(
             new RootAllocator(), Duration.ofSeconds(2))) {
      final FqpDestination missing = destination(null, null);
      assertThrows(FqpExecutionException.class,
          () -> transport.transfer(exchange(rowType()), missing, bytes));
      final RelDataType wrong = new JavaTypeFactoryImpl().builder()
          .add("id", SqlTypeName.INTEGER)
          .addAll(rowType().getFieldList().subList(1, rowType().getFieldCount())).build();
      final FqpExecutionException mismatch = assertThrows(FqpExecutionException.class,
          () -> transport.transfer(exchange(wrong), worker.destination, bytes));
      assertTrue(mismatch.getCause().getMessage().contains("schema"));
      worker.close();
      assertThrows(FqpExecutionException.class,
          () -> transport.transfer(exchange(rowType()), worker.destination, bytes));
    }
  }

  private static byte[] scan(DataFusionTestWorker worker) {
    final List<String> name = Collections.singletonList("exchange_input");
    final FqpPlanningConfig config = FqpPlanningConfig.of("calcite",
        Collections.singletonList(worker.destination), Collections.singletonList(
            new FqpTablePlacement(name, "worker", Collections.singletonMap("worker", name))),
        0.01D, true);
    final SchemaPlus schema = Frameworks.createRootSchema(true);
    schema.add("exchange_input", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory factory) {
        return rowType();
      }
    });
    final RelNode scan = RelBuilder.create(Frameworks.newConfigBuilder()
        .defaultSchema(schema).build()).scan("exchange_input").build();
    final FqpTask task = new FqpTask(new FqpTaskId("consumer"),
        new SubstraitFragmentSerializer(config).serialize(scan, worker.destination),
        Collections.singleton(name));
    return new DataFusionTaskClient(config).execute(task);
  }

  private static FqpExchange exchange(RelDataType rowType) {
    return new FqpExchange(new FqpExchangeId("test"), new FqpTaskId("producer"),
        new FqpTaskId("consumer"), "producer", "worker", rowType,
        Collections.singletonList("exchange_input"), 3D, 120D);
  }

  private static RelDataType rowType() {
    return new JavaTypeFactoryImpl().builder()
        .add("id", SqlTypeName.BIGINT).nullable(true)
        .add("label", SqlTypeName.VARCHAR, 40).nullable(true)
        .add("amount", SqlTypeName.DECIMAL, 12, 2).nullable(true)
        .add("day", SqlTypeName.DATE).nullable(true)
        .add("flag", SqlTypeName.BOOLEAN).nullable(true)
        .add("rate", SqlTypeName.DOUBLE).nullable(true).build();
  }

  private static byte[] stream(boolean empty) throws Exception {
    try (RootAllocator allocator = new RootAllocator();
         BigIntVector id = new BigIntVector("id", allocator);
         VarCharVector label = new VarCharVector("label", allocator);
         DecimalVector amount = new DecimalVector("amount", allocator, 12, 2);
         DateDayVector day = new DateDayVector("day", allocator);
         BitVector flag = new BitVector("flag", allocator);
         Float8Vector rate = new Float8Vector("rate", allocator);
         VectorSchemaRoot root = VectorSchemaRoot.of(id, label, amount, day, flag, rate);
         ByteArrayOutputStream output = new ByteArrayOutputStream();
         ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
      root.allocateNew();
      writer.start();
      if (!empty) {
        id.setSafe(0, 1L);
        label.setSafe(0, "caf\u00e9".getBytes(StandardCharsets.UTF_8));
        amount.setSafe(0, new BigDecimal("12.34"));
        day.setSafe(0, 9204);
        flag.setSafe(0, 1);
        rate.setSafe(0, 1.5D);
        root.setRowCount(2);
        writer.writeBatch();
        id.setSafe(0, 2L);
        label.setSafe(0, "second".getBytes(StandardCharsets.UTF_8));
        amount.setSafe(0, new BigDecimal("-1.25"));
        day.setSafe(0, 9205);
        flag.setSafe(0, 0);
        rate.setSafe(0, 2.5D);
        root.setRowCount(1);
        writer.writeBatch();
      }
      writer.end();
      return output.toByteArray();
    }
  }

  private static FqpDestination destination(URI http, URI flight) {
    return new FqpDestination("worker", DuckDBSqlDialect.DEFAULT,
        FqpDestinationCapabilities.of(Collections.singleton(
            FqpFragmentPayload.Format.SUBSTRAIT_BINARY), http, null, flight));
  }

}
