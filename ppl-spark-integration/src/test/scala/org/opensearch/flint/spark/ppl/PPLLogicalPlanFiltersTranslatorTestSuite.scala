/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry, TableFunctionRegistry, UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Descending, Divide, EqualTo, Floor, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Like, Literal, NamedExpression, Not, SortOrder, UnixTimestamp}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.Assert.assertEquals
import org.mockito.Mockito.when
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

class PPLLogicalPlanFiltersTranslatorTestSuite
  extends SparkFunSuite
    with Matchers {

  private val planTrnasformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()
  
  test("test simple search with only one table with one field literal filtered ") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser, "source=t a = 1 ", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "source=[t] | where a = 1 | fields + *")
  }

  test("test simple search with only one table with one field literal int equality filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser, "source=t a = 1  | fields a", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "source=[t] | where a = 1 | fields + a")
  }

  test("test simple search with only one table with one field literal string equality filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser,  """source=t a = 'hi'  | fields a""", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = EqualTo(UnresolvedAttribute("a"), Literal("hi"))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
 
    assertEquals(expectedPlan,context.getPlan)
    assertEquals(logPlan, "source=[t] | where a = 'hi' | fields + a")
  }


  test("test simple search with only one table with one field literal string none equality filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser,  """source=t a != 'bye'  | fields a""", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = Not(EqualTo(UnresolvedAttribute("a"), Literal("bye")))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
 
    assertEquals(expectedPlan,context.getPlan)
    assertEquals(logPlan, "source=[t] | where a != 'bye' | fields + a")
  }

  test("test simple search with only one table with one field greater than  filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser, "source=t a > 1  | fields a", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = GreaterThan(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "source=[t] | where a > 1 | fields + a")
  }

  test("test simple search with only one table with one field greater than equal  filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser, "source=t a >= 1  | fields a", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = GreaterThanOrEqual(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "source=[t] | where a >= 1 | fields + a")
  }

  test("test simple search with only one table with one field lower than filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser, "source=t a < 1  | fields a", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = LessThan(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "source=[t] | where a < 1 | fields + a")
  }

  test("test simple search with only one table with one field lower than equal filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser, "source=t a <= 1  | fields a", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = LessThanOrEqual(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "source=[t] | where a <= 1 | fields + a")
  }

  test("test simple search with only one table with one field not equal filtered and one field projected") {
    val context = new CatalystPlanContext
    val logPlan = planTrnasformer.visit(plan(pplParser, "source=t a != 1  | fields a", false), context)

    val table = UnresolvedRelation(Seq("t"))
    val filterExpr = Not(EqualTo(UnresolvedAttribute("a"), Literal(1)))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(expectedPlan, context.getPlan)
    assertEquals(logPlan, "source=[t] | where a != 1 | fields + a")
  }
}