/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.mv

import java.util.Locale

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.opensearch.flint.core.metadata.FlintMetadata
import org.opensearch.flint.spark.{FlintSpark, FlintSparkIndex, FlintSparkIndexBuilder, FlintSparkIndexOptions}
import org.opensearch.flint.spark.FlintSparkIndex.{generateSchemaJSON, metadataBuilder, StreamingRefresh}
import org.opensearch.flint.spark.FlintSparkIndexOptions.empty
import org.opensearch.flint.spark.function.TumbleFunction
import org.opensearch.flint.spark.mv.FlintSparkMaterializedView.{getFlintIndexName, MV_INDEX_TYPE}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, EventTimeWatermark, LogicalPlan}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.flint.{logicalPlanToDataFrame, qualifyTableName}

/**
 * Flint materialized view in Spark.
 *
 * @param mvName
 *   MV name
 * @param query
 *   source query that generates MV data
 * @param outputSchema
 *   output schema
 * @param options
 *   index options
 */
case class FlintSparkMaterializedView(
    targetIndexName: Option[String],
    mvName: String,
    query: String,
    outputSchema: Map[String, String],
    override val options: FlintSparkIndexOptions = empty)
    extends FlintSparkIndex
    with StreamingRefresh {

  /** TODO: add it to index option */
  private val watermarkDelay = "0 Minute"

  override val kind: String = MV_INDEX_TYPE

  override def name(): String = getFlintIndexName(mvName)

  /**
   * @return
   * Flint target index name - index that already exist or has existing template to be created with
   */
  override def targetName(): String = {
    targetIndexName.getOrElse(name())
  }

  override def metadata(): FlintMetadata = {
    val indexColumnMaps =
      outputSchema.map { case (colName, colType) =>
        Map[String, AnyRef]("columnName" -> colName, "columnType" -> colType).asJava
      }.toArray
    val schemaJson = generateSchemaJSON(outputSchema)

    metadataBuilder(this)
      .name(mvName)
      .source(query)
      .indexedColumns(indexColumnMaps)
      .schema(schemaJson)
      .build()
  }

  override def build(spark: SparkSession, df: Option[DataFrame]): DataFrame = {
    require(df.isEmpty, "materialized view doesn't support reading from other data frame")

    spark.sql(query)
  }

  override def buildStream(spark: SparkSession): DataFrame = {
    val batchPlan = spark.sql(query).queryExecution.logical

    /*
     * Convert unresolved batch plan to streaming plan by:
     *  1.Insert Watermark operator below Aggregate (required by Spark streaming)
     *  2.Set isStreaming flag to true in Relation operator
     */
    val streamingPlan = batchPlan transform {
      case WindowingAggregate(agg, timeCol) =>
        agg.copy(child = watermark(timeCol, watermarkDelay, agg.child))

      case relation: UnresolvedRelation if !relation.isStreaming =>
        relation.copy(isStreaming = true)
    }
    logicalPlanToDataFrame(spark, streamingPlan)
  }

  private def watermark(timeCol: Attribute, delay: String, child: LogicalPlan) = {
    EventTimeWatermark(timeCol, IntervalUtils.fromIntervalString(delay), child)
  }

  /**
   * Extractor that extract event time column out of Aggregate operator.
   */
  private object WindowingAggregate {

    def unapply(agg: Aggregate): Option[(Aggregate, Attribute)] = {
      val winFuncs = agg.groupingExpressions.collect {
        case func: UnresolvedFunction if isWindowingFunction(func) =>
          func
      }

      if (winFuncs.size != 1) {
        throw new IllegalStateException(
          "A windowing function is required for streaming aggregation")
      }

      // Assume first aggregate item must be time column
      val winFunc = winFuncs.head
      val timeCol = winFunc.arguments.head.asInstanceOf[Attribute]
      Some(agg, timeCol)
    }

    private def isWindowingFunction(func: UnresolvedFunction): Boolean = {
      val funcName = func.nameParts.mkString(".").toLowerCase(Locale.ROOT)
      val funcIdent = FunctionIdentifier(funcName)

      // TODO: support other window functions
      funcIdent == TumbleFunction.identifier
    }
  }
}

object FlintSparkMaterializedView {

  /** MV index type name */
  val MV_INDEX_TYPE = "mv"

  /**
   * Get index name following the convention "flint_" + qualified MV name (replace dot with
   * underscore).
   *
   * @param mvName
   *   MV name
   * @return
   *   Flint index name
   */
  def getFlintIndexName(mvName: String): String = {
    require(
      mvName.split("\\.").length >= 3,
      "Qualified materialized view name catalog.database.mv is required")

    s"flint_${mvName.replace(".", "_")}"
  }

  /** Builder class for MV build */
  class Builder(flint: FlintSpark) extends FlintSparkIndexBuilder(flint) {
    private var targetIndexName: String = ""
    private var mvName: String = ""
    private var query: String = ""

    /**
     * Set covering index target name.
     *
     * @param indexName
     * index name
     * @return
     * index builder
     */
    def targetName(indexName: String): Builder = {
      this.targetIndexName = indexName
      this
    }

    /**
     * Set MV name.
     *
     * @param mvName
     *   MV name
     * @return
     *   builder
     */
    def name(mvName: String): Builder = {
      this.mvName = qualifyTableName(flint.spark, mvName)
      this
    }

    /**
     * Set MV query.
     *
     * @param query
     *   MV query
     * @return
     *   builder
     */
    def query(query: String): Builder = {
      this.query = query
      this
    }

    override protected def buildIndex(): FlintSparkIndex = {
      // TODO: change here and FlintDS class to support complex field type in future
      val outputSchema = flint.spark
        .sql(query)
        .schema
        .map { field =>
          field.name -> field.dataType.typeName
        }
        .toMap
      FlintSparkMaterializedView(Option.apply(targetIndexName), mvName, query, outputSchema, indexOptions)
    }
  }
}
