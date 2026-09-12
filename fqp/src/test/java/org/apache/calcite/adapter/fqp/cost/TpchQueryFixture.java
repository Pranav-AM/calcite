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

/** Calcite's checked-in Q5/Q7 variants, with date predicates left commented out. */
public final class TpchQueryFixture {
  private static final String Q5 = "select\n"
      + "  n.n_name,\n"
      + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue\n"
      + "\n"
      + "from\n"
      + "  tpch.customer c,\n"
      + "  tpch.orders o,\n"
      + "  tpch.lineitem l,\n"
      + "  tpch.supplier s,\n"
      + "  tpch.nation n,\n"
      + "  tpch.region r\n"
      + "\n"
      + "where\n"
      + "  c.c_custkey = o.o_custkey\n"
      + "  and l.l_orderkey = o.o_orderkey\n"
      + "  and l.l_suppkey = s.s_suppkey\n"
      + "  and c.c_nationkey = s.s_nationkey\n"
      + "  and s.s_nationkey = n.n_nationkey\n"
      + "  and n.n_regionkey = r.r_regionkey\n"
      + "  and r.r_name = 'EUROPE'\n"
      + "--  and o.o_orderdate >= date '1997-01-01'\n"
      + "--  and o.o_orderdate < date '1997-01-01' + interval '1' year\n"
      + "group by\n"
      + "  n.n_name\n"
      + "\n"
      + "order by\n"
      + "  revenue desc";

  private static final String Q7 = "select\n"
      + "  supp_nation,\n"
      + "  cust_nation,\n"
      + "  l_year,\n"
      + "  sum(volume) as revenue\n"
      + "from\n"
      + "  (\n"
      + "    select\n"
      + "      n1.n_name as supp_nation,\n"
      + "      n2.n_name as cust_nation,\n"
      + "      extract(year from l.l_shipdate) as l_year,\n"
      + "      l.l_extendedprice * (1 - l.l_discount) as volume\n"
      + "    from\n"
      + "      tpch.supplier s,\n"
      + "      tpch.lineitem l,\n"
      + "      tpch.orders o,\n"
      + "      tpch.customer c,\n"
      + "      tpch.nation n1,\n"
      + "      tpch.nation n2\n"
      + "    where\n"
      + "      s.s_suppkey = l.l_suppkey\n"
      + "      and o.o_orderkey = l.l_orderkey\n"
      + "      and c.c_custkey = o.o_custkey\n"
      + "      and s.s_nationkey = n1.n_nationkey\n"
      + "      and c.c_nationkey = n2.n_nationkey\n"
      + "      and (\n"
      + "        (n1.n_name = 'EGYPT' and n2.n_name = 'UNITED STATES')\n"
      + "        or (n1.n_name = 'UNITED STATES' and n2.n_name = 'EGYPT')\n"
      + "      )\n"
      + "--      and l.l_shipdate between date '1995-01-01' and date '1996-12-31'\n"
      + "  ) as shipping\n"
      + "group by\n"
      + "  supp_nation,\n"
      + "  cust_nation,\n"
      + "  l_year\n"
      + "order by\n"
      + "  supp_nation,\n"
      + "  cust_nation,\n"
      + "  l_year";

  private TpchQueryFixture() {
  }

  public static String sql(int query) {
    switch (query) {
    case 3:
      return TpchQ3Fixture.sql();
    case 5:
      return Q5;
    case 7:
      return Q7;
    default:
      throw new IllegalArgumentException("unsupported test query: " + query);
    }
  }

  public static RelNode logicalPlan(int query, double scale) throws Exception {
    final SchemaPlus root = Frameworks.createRootSchema(true);
    root.add("TPCH", new TpchSchema(scale, 1, 1, false));
    try (Planner planner = Frameworks.getPlanner(Frameworks.newConfigBuilder()
        .defaultSchema(root).build())) {
      final SqlNode parsed = planner.parse(sql(query));
      return planner.rel(planner.validate(parsed)).rel;
    }
  }
}
