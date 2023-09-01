/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.ppl

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar, UnresolvedTable}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Complete, Count}
import org.apache.spark.sql.catalyst.expressions.{Alias, Descending, Divide, EqualTo, GreaterThan, GreaterThanOrEqual, LessThan, Like, Literal, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Limit, Project, Sort, Union}
import org.junit.Assert.assertEquals
import org.opensearch.flint.spark.ppl.PlaneUtils.plan
import org.opensearch.sql.ppl.{CatalystPlanContext, CatalystQueryPlanVisitor}
import org.scalatest.matchers.should.Matchers

class PPLLogicalPlanTranslatorTestSuite
  extends SparkFunSuite
    with Matchers {

  private val planTrnasformer = new CatalystQueryPlanVisitor()
  private val pplParser = new PPLSyntaxParser()

  test("test simple search with only one table and no explicit fields (defaults to all fields)") {
    // if successful build ppl logical plan and translate to catalyst logical plan
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source=table", false), context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, UnresolvedTable(Seq("table"), "source=table", None))
    assertEquals(context.getPlan, expectedPlan)
  }

  test("test simple search with only one table with one field projected") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source=table | fields A", false), context)

    val projectList: Seq[NamedExpression] = Seq(UnresolvedAttribute("A"))
    val expectedPlan = Project(projectList, UnresolvedTable(Seq("table"), "source=table", None))
    assertEquals(context.getPlan, expectedPlan)
  }

  test("test simple search with only one table with one field literal filtered ") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source=t a = 1 ", false), context)

    val table = UnresolvedTable(Seq("t"), "source=t", None)
    val filterExpr = EqualTo(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedStar(None))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(context.getPlan, expectedPlan)
  }

  test("test simple search with only one table with one field literal filtered and one field projected") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source=t a = 1  | fields a", false), context)

    val table = UnresolvedTable(Seq("t"), "source=t", None)
    val filterExpr = EqualTo(UnresolvedAttribute("a"), Literal(1))
    val filterPlan = Filter(filterExpr, table)
    val projectList = Seq(UnresolvedAttribute("a"))
    val expectedPlan = Project(projectList, filterPlan)
    assertEquals(context.getPlan, expectedPlan)
  }


  test("test simple search with only one table with two fields projected") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source=t | fields A, B", false), context)


    val table = UnresolvedTable(Seq("t"), "source=t", None)
    val projectList = Seq(UnresolvedAttribute("A"), UnresolvedAttribute("B"))
    val expectedPlan = Project(projectList, table)
    assertEquals(context.getPlan, expectedPlan)
  }


  test("Search multiple tables - translated into union call") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "search source = table1, table2 ", false), context)


    val table1 = UnresolvedTable(Seq("table1"), "source=table1", None)
    val table2 = UnresolvedTable(Seq("table2"), "source=table2", None)

    val allFields1 = UnresolvedStar(None)
    val allFields2 = UnresolvedStar(None)

    val projectedTable1 = Project(Seq(allFields1), table1)
    val projectedTable2 = Project(Seq(allFields2), table2)

    val expectedPlan = Union(Seq(projectedTable1, projectedTable2))

    assertEquals(context.getPlan, expectedPlan)
  }

  test("Find What are the average prices for different types of properties") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = housing_properties | stats avg(price) by property_type", false), context)
    // equivalent to SELECT property_type, AVG(price) FROM housing_properties GROUP BY property_type
    val table = UnresolvedTable(Seq("housing_properties"), "source=housing_properties", None)

    val avgPrice = Alias(Average(UnresolvedAttribute("price")), "avg(price)")()
    val propertyType = UnresolvedAttribute("property_type")
    val grouped = Aggregate(Seq(propertyType), Seq(propertyType, avgPrice), table)

    val projectList = Seq(
      UnresolvedAttribute("property_type"),
      Alias(Average(UnresolvedAttribute("price")), "avg(price)")()
    )
    val expectedPlan = Project(projectList, grouped)

    assertEquals(context.getPlan, expectedPlan)

  }

  test("Find the top 10 most expensive properties in California, including their addresses, prices, and cities") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = housing_properties | where state = \"CA\" | fields address, price, city | sort - price | head 10", false), context)
    // Equivalent SQL: SELECT address, price, city FROM housing_properties WHERE state = 'CA' ORDER BY price DESC LIMIT 10

    // Constructing the expected Catalyst Logical Plan
    val table = UnresolvedTable(Seq("housing_properties"), "source=housing_properties", None)
    val filter = Filter(EqualTo(UnresolvedAttribute("state"), Literal("CA")), table)
    val projectList = Seq(UnresolvedAttribute("address"), UnresolvedAttribute("price"), UnresolvedAttribute("city"))
    val projected = Project(projectList, filter)
    val sortOrder = SortOrder(UnresolvedAttribute("price"), Descending) :: Nil
    val sorted = Sort(sortOrder, true, projected)
    val limited = Limit(Literal(10), sorted)
    val finalProjectList = Seq(UnresolvedAttribute("address"), UnresolvedAttribute("price"), UnresolvedAttribute("city"))

    val expectedPlan = Project(finalProjectList, limited)

    // Assert that the generated plan is as expected
    assertEquals(context.getPlan, expectedPlan)
  }

  test("Find the average price per unit of land space for properties in different cities") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = housing_properties | where land_space > 0 | eval price_per_land_unit = price / land_space | stats avg(price_per_land_unit) by city", false), context)
    // SQL: SELECT city, AVG(price / land_space) AS avg_price_per_land_unit FROM housing_properties WHERE land_space > 0 GROUP BY city
    val table = UnresolvedTable(Seq("housing_properties"), "source=housing_properties", None)
    val filter = Filter(GreaterThan(UnresolvedAttribute("land_space"), Literal(0)), table)
    val expression = AggregateExpression(
      Average(Divide(UnresolvedAttribute("price"), UnresolvedAttribute("land_space"))),
      mode = Complete,
      isDistinct = false
    )
    val aggregateExpr = Alias(expression, "avg_price_per_land_unit")()
    val groupBy = Aggregate(
      groupingExpressions = Seq(UnresolvedAttribute("city")),
      aggregateExpressions = Seq(aggregateExpr),
      filter)

    val expectedPlan = Project(
      projectList = Seq(
        UnresolvedAttribute("city"),
        UnresolvedAttribute("avg_price_per_land_unit")
      ), groupBy)
    // Continue with your test...
    assertEquals(context.getPlan, expectedPlan)
  }

  test("Find the houses posted in the last month, how many are still for sale") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "search source=housing_properties | where listing_age >= 0 | where listing_age < 30 | stats count() by property_status", false), context)
    // SQL: SELECT property_status, COUNT(*) FROM housing_properties WHERE listing_age >= 0 AND listing_age < 30 GROUP BY property_status;

    val filter = Filter(LessThan(UnresolvedAttribute("listing_age"), Literal(30)),
      Filter(GreaterThanOrEqual(UnresolvedAttribute("listing_age"), Literal(0)),
        UnresolvedTable(Seq("housing_properties"), "source=housing_properties", None)))

    val expression = AggregateExpression(
      Count(Literal(1)),
      mode = Complete,
      isDistinct = false)

    val aggregateExpressions = Seq(
      Alias(expression, "count")()
    )

    val groupByAttributes = Seq(UnresolvedAttribute("property_status"))
    val expectedPlan = Aggregate(groupByAttributes, aggregateExpressions, filter)
    assertEquals(context.getPlan, expectedPlan)
  }
  
  test("Find all the houses listed by agency Compass in  decreasing price order. Also provide only price, address and agency name information.") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = housing_properties | where match( agency_name , \"Compass\" ) | fields address , agency_name , price | sort - price ", false), context)
    // SQL: SELECT address, agency_name, price FROM housing_properties WHERE agency_name LIKE '%Compass%' ORDER BY price DESC

    val projectList = Seq(
      UnresolvedAttribute("address"),
      UnresolvedAttribute("agency_name"),
      UnresolvedAttribute("price")
    )

    val filterCondition = Like(UnresolvedAttribute("agency_name"), Literal("%Compass%"), '\\')
    val filter = Filter(filterCondition, UnresolvedTable(Seq("housing_properties"), "source=housing_properties", None))

    val sortOrder = Seq(SortOrder(UnresolvedAttribute("price"), Descending))
    val sort = Sort(sortOrder, true, filter)

    val expectedPlan = Project(projectList, sort)
    assertEquals(context.getPlan, expectedPlan)

  }
  
  test("Find details of properties owned by Zillow with at least 3 bedrooms and 2 bathrooms") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = housing_properties | where is_owned_by_zillow = 1 and bedroom_number >= 3 and bathroom_number >= 2 | fields address, price, city, listing_age", false), context)
  }

  test("Find which cities in WA state have the largest number of houses for sale") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = housing_properties | where property_status = 'FOR_SALE' and state = 'WA' | stats count() as count by city | sort -count | head", false), context)
  }

  test("Find the top 5 referrers for the '/' path in apache access logs") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = access_logs | where path = \"/\" | top 5 referer", false), context)
  }

  test("Find access paths by status code. How many error responses (status code 400 or higher) are there for each access path in the Apache access logs") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = access_logs | where status >= 400 | stats count() by path, status", false), context)
  }

  test("Find max size of nginx access requests for every 15min") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = access_logs | stats max(size)  by span( request_time , 15m) ", false), context)
  }

  test("Find nginx logs with non 2xx status code and url containing 'products'") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = sso_logs-nginx-* | where match(http.url, 'products') and http.response.status_code >= \"300\"", false), context)
  }

  test("Find What are the details (URL, response status code, timestamp, source address) of events in the nginx logs where the response status code is 400 or higher") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = sso_logs-nginx-* | where http.response.status_code >= \"400\" | fields http.url, http.response.status_code, @timestamp, communication.source.address", false), context)
  }

  test("Find What are the average and max http response sizes, grouped by request method, for access events in the nginx logs") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = sso_logs-nginx-* | where event.name = \"access\" | stats avg(http.response.bytes), max(http.response.bytes) by http.request.method", false), context)
  }

  test("Find flights from which carrier has the longest average delay for flights over 6k miles") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = opensearch_dashboards_sample_data_flights | where DistanceMiles > 6000 | stats avg(FlightDelayMin) by Carrier | sort -`avg(FlightDelayMin)` | head 1", false), context)
  }

  test("Find What's the average ram usage of windows machines over time aggregated by 1 week") {
    val context = new CatalystPlanContext
    planTrnasformer.visit(plan(pplParser, "source = opensearch_dashboards_sample_data_logs | where match(machine.os, 'win') | stats avg(machine.ram) by span(timestamp,1w)", false), context)
  }



  // Add more test cases as needed
}