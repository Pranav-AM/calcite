/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.execution;

import org.apache.calcite.adapter.fqp.FqpDistributedPlan;
import org.apache.calcite.adapter.fqp.FqpExchange;
import org.apache.calcite.adapter.fqp.FqpExchangeId;
import org.apache.calcite.adapter.fqp.FqpFragment;
import org.apache.calcite.adapter.fqp.FqpFragmentPayload;
import org.apache.calcite.adapter.fqp.FqpPlanningConfig;
import org.apache.calcite.adapter.fqp.FqpTask;
import org.apache.calcite.adapter.fqp.FqpTaskId;
import org.apache.calcite.adapter.fqp.FqpTestDeployments;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests dependency-ordered distributed execution and exchange staging. */
class FqpDistributedPlanExecutorTest {
  private final FqpPlanningConfig config = FqpTestDeployments.threeOwnerTpch();
  private final RelDataType rowType = new JavaTypeFactoryImpl().builder()
      .add("KEY", SqlTypeName.BIGINT).build();

  @Test void executesAndStagesEveryTaskInDependencyOrder() {
    final FqpTask customer = task("customer", "df-customer", "CUSTOMER");
    final FqpTask orders = task("orders", "df-orders", "ORDERS");
    final FqpTask lineitem = task("lineitem", "df-lineitem", "LINEITEM");
    final FqpExchange first = exchange("customer-orders", customer, orders,
        "customer_input");
    final FqpExchange second = exchange("orders-lineitem", orders, lineitem,
        "orders_input");
    final FqpDistributedPlan plan = new FqpDistributedPlan(config,
        Arrays.asList(lineitem, orders, customer), Arrays.asList(second, first),
        lineitem.id());
    final List<String> events = new ArrayList<>();
    final FqpTaskClient tasks = task -> {
      events.add("execute:" + task.id());
      return new byte[] {(byte) events.size()};
    };
    final FqpExchangeTransport exchanges = (exchange, destination, stream) ->
        events.add("transfer:" + exchange.id() + ":" + destination.sourceId());

    final byte[] result = new FqpDistributedPlanExecutor(tasks, exchanges).execute(plan);

    assertArrayEquals(new byte[] {5}, result);
    assertEquals(Arrays.asList("execute:customer",
        "transfer:customer-orders:df-orders", "execute:orders",
        "transfer:orders-lineitem:df-lineitem", "execute:lineitem"), events);
  }

  private FqpTask task(String id, String destinationId, String table) {
    final FqpFragment fragment = new FqpFragment(config.destination(destinationId).get(),
        FqpFragmentPayload.binary(FqpFragmentPayload.Format.SUBSTRAIT_BINARY,
            new byte[] {1}), rowType, Collections.singleton(destinationId),
        Collections.emptyList());
    return new FqpTask(new FqpTaskId(id), fragment,
        new LinkedHashSet<>(Collections.singletonList(Arrays.asList("TPCH", table))));
  }

  private FqpExchange exchange(String id, FqpTask producer, FqpTask consumer,
      String temporaryName) {
    return new FqpExchange(new FqpExchangeId(id), producer.id(), consumer.id(),
        producer.destination().sourceId(), consumer.destination().sourceId(), rowType,
        Collections.singletonList(temporaryName), 1D, 8D);
  }
}
