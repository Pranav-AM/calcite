/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.calcite.adapter.fqp.cost;

import org.apache.calcite.adapter.tpch.TpchSchema;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

/** Q3 matching the external plan's predicates, isolated for FQP tests. */
public final class TpchQ3Fixture {
  // Retains the query 03 structure from plus/.../TpchTest, with the external
  // plan's BUILDING segment and March 15 date predicates.
  private static final String SQL = "select\n"
      + "  l.l_orderkey,\n"
      + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
      + "  o.o_orderdate,\n"
      + "  o.o_shippriority\n"
      + "from\n"
      + "  tpch.customer c,\n"
      + "  tpch.orders o,\n"
      + "  tpch.lineitem l\n"
      + "where\n"
      + "  c.c_mktsegment = 'BUILDING'\n"
      + "  and c.c_custkey = o.o_custkey\n"
      + "  and l.l_orderkey = o.o_orderkey\n"
      + "  and o.o_orderdate < date '1995-03-15'\n"
      + "  and l.l_shipdate > date '1995-03-15'\n"
      + "group by\n"
      + "  l.l_orderkey,\n"
      + "  o.o_orderdate,\n"
      + "  o.o_shippriority\n"
      + "order by\n"
      + "  revenue desc,\n"
      + "  o.o_orderdate\n"
      + "limit 10";

  private TpchQ3Fixture() {
  }

  public static RelNode logicalPlan() throws Exception {
    return logicalPlan(0.01D);
  }

  public static String sql() {
    return SQL;
  }

  public static RelNode logicalPlan(double scale) throws Exception {
    final SchemaPlus root = Frameworks.createRootSchema(true);
    root.add("TPCH", new TpchSchema(scale, 1, 1, false));
    final Planner planner = Frameworks.getPlanner(Frameworks.newConfigBuilder()
        .defaultSchema(root).build());
    final SqlNode parsed = planner.parse(SQL);
    return planner.rel(planner.validate(parsed)).rel;
  }
}
