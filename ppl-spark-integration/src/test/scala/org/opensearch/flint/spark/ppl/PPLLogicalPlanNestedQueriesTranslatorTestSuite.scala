/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, GreaterThan, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

class PPLLogicalPlanNestedQueriesTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()
  

  test("create ppl simple query with nested field 1 range filter test") {
    val context = new CatalystPlanContext
    val logicalPlan =
      planTransformer.visit(plan(pplParser, "source = schema.table | where struct_col.field2 > 200 | sort  - struct_col.field2 | fields  int_col, struct_col.field2", false), context)

    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("spark_catalog", "default", "flint_ppl_test"))
    // Define the expected logical plan components
    val filterPlan =
      Filter(GreaterThan(UnresolvedAttribute("struct_col.field2"), Literal(200)), table)
    val sortedPlan: LogicalPlan =
      Sort(
        Seq(SortOrder(UnresolvedAttribute("struct_col.field2"), Descending)),
        global = true,
        filterPlan)
    val expectedPlan =
      Project(
        Seq(UnresolvedAttribute("int_col"), UnresolvedAttribute("struct_col.field2")),
        sortedPlan)

    // Compare the two plans
    assert(compareByString(expectedPlan) === compareByString(logicalPlan))
  }

  test("create ppl simple query with nested field string filter test") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "source = catalog.table where struct_col2.field1.subfield > 'valueA' | sort int_col | fields  int_col, struct_col.field1.subfield, struct_col2.field1.subfield", false), context)

    // Define the expected logical plan
    val table = UnresolvedRelation(Seq("catalog", "table"))
    // Define the expected logical plan components
    val filterPlan = Filter(
      GreaterThan(UnresolvedAttribute("struct_col2.field1.subfield"), Literal("valueA")),
      table)
    val sortedPlan: LogicalPlan =
      Sort(Seq(SortOrder(UnresolvedAttribute("int_col"), Ascending)), global = true, filterPlan)
    val expectedPlan =
      Project(
        Seq(
          UnresolvedAttribute("int_col"),
          UnresolvedAttribute("struct_col.field1.subfield"),
          UnresolvedAttribute("struct_col2.field1.subfield")),
        sortedPlan)

    // Compare the two plans
    comparePlans(expectedPlan, logPlan, false)
  }
  
  test("test simple search with schema.table and one nested field projected") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=schema.table | fields A.nested", false),
        context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedAttribute("A.nested"))
    val expectedPlan = Project(projectList, UnresolvedRelation(Seq("schema", "table")))
    comparePlans(expectedPlan, logPlan, false)
  }
  

  test("test simple search with one table with two fields projected sorted by one nested field") {
    val context = new CatalystPlanContext
    val logPlan =
      planTransformer.visit(
        plan(pplParser, "source=t | sort A.nested | fields A.nested, B", false),
        context)

    val table = UnresolvedRelation(Seq("t"))
    val projectList = Seq(UnresolvedAttribute("A.nested"), UnresolvedAttribute("B"))
    // Sort by A ascending
    val sortOrder = Seq(SortOrder(UnresolvedAttribute("A.nested"), Ascending))
    val sorted = Sort(sortOrder, true, table)
    val expectedPlan = Project(projectList, sorted)

    comparePlans(expectedPlan, logPlan, false)
  }
  
  test(
    "Search multiple tables - translated into union call - nested fields expected to exist in both tables ") {
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(pplParser, "search source = table1, table2 | fields A.nested1, B.nested1", false),
      context)

    val table1 = UnresolvedRelation(Seq("table1"))
    val table2 = UnresolvedRelation(Seq("table2"))

    val allFields1 = Seq(UnresolvedAttribute("A"), UnresolvedAttribute("B"))
    val allFields2 = Seq(UnresolvedAttribute("A"), UnresolvedAttribute("B"))

    val projectedTable1 = Project(allFields1, table1)
    val projectedTable2 = Project(allFields2, table2)

    val expectedPlan =
      Union(Seq(projectedTable1, projectedTable2), byName = true, allowMissingCol = true)

    comparePlans(expectedPlan, logPlan, false)
  }
}
