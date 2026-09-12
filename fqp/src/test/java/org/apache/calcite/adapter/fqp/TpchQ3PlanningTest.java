/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp;

import org.apache.calcite.adapter.fqp.cost.TpchQ3Fixture;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Verifies the FQP test fixture can plan Q3 with the external plan's predicates. */
class TpchQ3PlanningTest {
  @Test void parsesAndBuildsLogicalPlan() throws Exception {
    final RelNode plan = TpchQ3Fixture.logicalPlan();
    final String text = RelOptUtil.toString(plan);

    assertTrue(text.contains("LogicalSort"));
    assertTrue(text.contains("LogicalAggregate"));
    assertTrue(text.contains("LogicalJoin"));
    assertTrue(text.contains("[TPCH, CUSTOMER]"));
    assertTrue(text.contains("[TPCH, ORDERS]"));
    assertTrue(text.contains("[TPCH, LINEITEM]"));
  }
}
