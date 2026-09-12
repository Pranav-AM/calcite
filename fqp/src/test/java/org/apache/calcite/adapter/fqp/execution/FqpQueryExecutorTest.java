/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpDistributedPlan;
import org.apache.calcite.adapter.fqp.FqpExecutionException;
import org.apache.calcite.adapter.fqp.FqpPlanningContext;
import org.apache.calcite.adapter.fqp.FqpTestDeployments;
import org.apache.calcite.adapter.fqp.cost.TpchQ3Fixture;

import org.apache.calcite.linq4j.Enumerable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the logical-plan to Enumerable distributed execution boundary. */
class FqpQueryExecutorTest {
  @Test void preparesWithoutRpcAndExecutesInDependencyOrder() throws Exception {
    final FqpPlanningContext context = FqpTestDeployments.threeOwnerTpchDeployment()
        .planningContext();
    final List<String> events = new ArrayList<>();
    final byte[] arrow = resultStream();
    final FqpQueryExecutor executor = new FqpQueryExecutor(context, task -> {
      events.add("execute:" + task.destination().sourceId());
      return arrow;
    }, (exchange, destination, stream) -> {
      assertArrayEquals(arrow, stream);
      events.add("transfer:" + exchange.sourceId() + ":" + destination.sourceId());
    });

    final FqpDistributedPlan prepared = executor.prepare(TpchQ3Fixture.logicalPlan());
    assertEquals(3, prepared.tasks().size());
    assertEquals(2, prepared.exchanges().size());
    assertTrue(events.isEmpty());

    final Enumerable<Object[]> rows = executor.execute(TpchQ3Fixture.logicalPlan());
    assertArrayEquals(new Object[] {42L, 123.5D,
        Math.toIntExact(LocalDate.of(1995, 3, 15).toEpochDay()), 0}, rows.single());
    final List<String> expected = new ArrayList<>();
    prepared.topologicalTasks().forEach(task -> {
      expected.add("execute:" + task.destination().sourceId());
      prepared.outgoingExchanges(task.id()).forEach(exchange ->
          expected.add("transfer:" + exchange.sourceId() + ":" + exchange.destinationId()));
    });
    assertEquals(expected, events);
    assertEquals(0L, context.metrics().remoteCalls());
  }

  @Test void stopsBeforeConsumerWhenTransferFails() throws Exception {
    final List<String> executed = new ArrayList<>();
    final byte[] arrow = resultStream();
    final FqpQueryExecutor executor = new FqpQueryExecutor(
        FqpTestDeployments.threeOwnerTpchDeployment().planningContext(), task -> {
      executed.add(task.destination().sourceId());
      return arrow;
    }, (exchange, destination, stream) -> {
      throw new FqpExecutionException("test transfer failure");
    });
    assertThrows(FqpExecutionException.class,
        () -> executor.execute(TpchQ3Fixture.logicalPlan()));
    assertEquals(Arrays.asList("df-customer"), executed);
  }

  @Test void givesSeparateExecutionsDistinctTemporaryNames() throws Exception {
    final FqpQueryExecutor executor = new FqpQueryExecutor(
        FqpTestDeployments.threeOwnerTpchDeployment());
    final Set<List<String>> names = new HashSet<>();
    for (int i = 0; i < 2; i++) {
      executor.prepare(TpchQ3Fixture.logicalPlan()).exchanges().forEach(exchange ->
          assertTrue(names.add(exchange.temporaryInputName())));
    }
    assertEquals(4, names.size());
  }

  @Test void missingStatisticsFailsBeforeExecution() {
    final FqpQueryExecutor executor = new FqpQueryExecutor(
        new FqpPlanningContext(FqpTestDeployments.threeOwnerTpch()), task -> {
      throw new AssertionError("planning must not execute a task");
    }, (exchange, destination, stream) -> {
      throw new AssertionError("planning must not transfer data");
    });
    assertThrows(IllegalArgumentException.class,
        () -> executor.execute(TpchQ3Fixture.logicalPlan()));
  }

  private static byte[] resultStream() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
         BigIntVector key = new BigIntVector("L_ORDERKEY", allocator);
         Float8Vector revenue = new Float8Vector("REVENUE", allocator);
         DateDayVector date = new DateDayVector("O_ORDERDATE", allocator);
         IntVector priority = new IntVector("O_SHIPPRIORITY", allocator);
         VectorSchemaRoot root = VectorSchemaRoot.of(key, revenue, date, priority);
         ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      root.allocateNew();
      key.setSafe(0, 42L);
      revenue.setSafe(0, 123.5D);
      date.setSafe(0, Math.toIntExact(LocalDate.of(1995, 3, 15).toEpochDay()));
      priority.setSafe(0, 0);
      root.setRowCount(1);
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      return output.toByteArray();
    }
  }
}
