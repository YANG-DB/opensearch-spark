/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.common.antlr.SyntaxCheckException
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedIdentifier, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, EqualTo, GreaterThan, Literal, NamedExpression, Not, SortOrder}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, IdentityTransform, NamedReference, Transform}


class PPLLogicalPlanProjectQueriesTranslatorTestSuite
    extends SparkFunSuite
    with PlanTest
    with LogicalPlanTestUtils
    with Matchers {

  private val planTransformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test project a simple search with only one table using csv ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "project simpleView using csv | source = table | where state != 'California' | fields name"),
      context)

    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("table"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq("simpleView")),
        Seq(),
        Project(Seq(UnresolvedAttribute("name")), filter),
        UnresolvedTableSpec(
          Map.empty,
          Option("CSV"),
          OptionList(Seq()),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = false,
        isAnalyzed = false)
    // Compare the two plans
    comparePlans(logPlan, expectedPlan, checkAnalysis = false)
  }

  test("test project a simple search with only one table using csv and partitioned field ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "project if not exists simpleView using csv partitioned by (age) | source = table | where state != 'California' "),
      context)

    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("table"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq("simpleView")),
        Seq(),
//        Seq(IdentityTransform.apply(FieldReference.apply("age"))),
        Project(Seq(UnresolvedStar(None)), filter),
        UnresolvedTableSpec(
          Map.empty,
          Option("CSV"),
          OptionList(Seq()),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = true,
        isAnalyzed = false)
    // Compare the two plans
    assert(
      compareByString(logPlan) == expectedPlan.toString
    )
  }
  
  test("test project a simple search with only one table using csv and multiple partitioned fields ") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    val logPlan = planTransformer.visit(
      plan(
        pplParser,
        "project if not exists simpleView using csv partitioned by (age, country) | source = table | where state != 'California' "),
      context)

    // Define the expected logical plan
    val relation = UnresolvedRelation(Seq("table"))
    val filter =
      Filter(Not(EqualTo(UnresolvedAttribute("state"), Literal("California"))), relation)
    val expectedPlan: LogicalPlan =
      CreateTableAsSelect(
        UnresolvedIdentifier(Seq("simpleView")),
        Seq(),
//        Seq(IdentityTransform.apply(FieldReference.apply("age")), IdentityTransform.apply(FieldReference.apply("country"))),
        Project(Seq(UnresolvedStar(None)), filter),
        UnresolvedTableSpec(
          Map.empty,
          Option("CSV"),
          OptionList(Seq()),
          Option.empty,
          Option.empty,
          Option.empty,
          external = false),
        Map.empty,
        ignoreIfExists = true,
        isAnalyzed = false)
    // Compare the two plans
    assert(
      compareByString(logPlan) == expectedPlan.toString
    )
  }
}
