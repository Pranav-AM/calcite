/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests validation and dependency ordering of distributed FQP plans. */
class FqpDistributedPlanTest {
  private final FqpPlanningConfig config = FqpTestDeployments.threeOwnerTpch();
  private final RelDataType rowType = new JavaTypeFactoryImpl().builder()
      .add("KEY", org.apache.calcite.sql.type.SqlTypeName.BIGINT).build();

  @Test void ordersQ3TasksProducerBeforeConsumer() {
    final FqpTask customer = task("customer-filter", "df-customer", "CUSTOMER");
    final FqpTask orders = task("orders-join", "df-orders", "ORDERS");
    final FqpTask lineitem = task("lineitem-final", "df-lineitem", "LINEITEM");
    final FqpExchange customerToOrders = exchange("customer-keys", customer, orders,
        "incoming_customer", 30_000D);
    final FqpExchange ordersToLineitem = exchange("eligible-orders", orders, lineitem,
        "incoming_orders", 120_000D);

    final FqpDistributedPlan plan = new FqpDistributedPlan(config,
        Arrays.asList(lineitem, customer, orders),
        Arrays.asList(ordersToLineitem, customerToOrders), lineitem.id());

    assertEquals(Arrays.asList("customer-filter", "orders-join", "lineitem-final"),
        plan.topologicalTasks().stream().map(task -> task.id().value())
            .collect(Collectors.toList()));
    assertEquals(Collections.singletonList(customerToOrders),
        plan.incomingExchanges(orders.id()));
    assertEquals(Collections.singletonList(ordersToLineitem),
        plan.outgoingExchanges(orders.id()));
    assertEquals(lineitem, plan.rootTask());
  }

  @Test void rejectsTableClaimedLocalAtNonOwner() {
    final FqpTask wrong = task("wrong", "df-orders", "CUSTOMER");
    assertThrows(IllegalArgumentException.class, () -> new FqpDistributedPlan(config,
        Collections.singletonList(wrong), Collections.emptyList(), wrong.id()));
  }

  @Test void rejectsCyclicTaskGraph() {
    final FqpTask customer = task("customer", "df-customer", "CUSTOMER");
    final FqpTask orders = task("orders", "df-orders", "ORDERS");
    final FqpExchange forward = exchange("forward", customer, orders, "customer_input", 10D);
    final FqpExchange backward = exchange("backward", orders, customer, "orders_input", 10D);
    assertThrows(IllegalArgumentException.class, () -> new FqpDistributedPlan(config,
        Arrays.asList(customer, orders), Arrays.asList(forward, backward), orders.id()));
  }

  private FqpTask task(String taskId, String destinationId, String table) {
    final FqpDestination destination = config.destination(destinationId).get();
    final FqpFragment fragment = new FqpFragment(destination,
        FqpFragmentPayload.binary(FqpFragmentPayload.Format.SUBSTRAIT_BINARY,
            new byte[] {1}), rowType, Collections.singleton(destinationId),
        Collections.emptyList());
    final Set<List<String>> localTables = new LinkedHashSet<>();
    localTables.add(Arrays.asList("TPCH", table));
    return new FqpTask(new FqpTaskId(taskId), fragment, localTables);
  }

  private FqpExchange exchange(String exchangeId, FqpTask producer,
      FqpTask consumer, String temporaryName, double rows) {
    return new FqpExchange(new FqpExchangeId(exchangeId), producer.id(), consumer.id(),
        producer.destination().sourceId(), consumer.destination().sourceId(), rowType,
        Collections.singletonList(temporaryName), rows, rows * 8D);
  }
}
