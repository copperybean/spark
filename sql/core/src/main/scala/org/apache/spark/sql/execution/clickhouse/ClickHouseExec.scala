/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.clickhouse

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Final, PartialMerge}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, DataSourceUtils, HadoopFsRelation}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

case class ClickHouseExec(child: SparkPlan)(val clickHouseStageId: Int) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  // This is not strictly needed because the codegen transformation happens after the columnar
  // transformation but just for consistency
  override def supportsColumnar: Boolean = child.supportsColumnar

  override lazy val metrics = Map(
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext,
      ClickHouseExec.PIPELINE_DURATION_METRIC))

  override def nodeName: String = s"ClickHouseExec(${clickHouseStageId})"

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Code generation is not currently supported for columnar output, so just fall back to
    // the interpreted path
    child.executeColumnar()
  }

  override def doExecute(): RDD[InternalRow] = {
    val optParams = transformPlan(child, ClickHouseProcessorParams())
    if (optParams.isDefined && optParams.get.scan.isDefined) {
      val params = optParams.get
      new ClickHouseRDD(
        params.scan.get.sparkSession, params.scan.get.partitions, params)
    } else {
      child.execute()
    }
  }

  private def transformPlan(plan: SparkPlan, clickhouseParams: ClickHouseProcessorParams):
      Option[ClickHouseProcessorParams] = {
    if (plan.children.length > 1) {
      // not supported currently
      None
    } else if (plan.children.length == 1) {
      transformPlan(plan.children.head, clickhouseParams) match {
        case None => None
        case Some(params) => transformOnePlan(plan, params)
      }
    } else {
      transformOnePlan(plan, clickhouseParams)
    }
  }

  private def transformOnePlan(plan: SparkPlan, clickhouseParams: ClickHouseProcessorParams) = {
    plan match {
      case fileScanExec: FileSourceScanExec =>
        val relation = fileScanExec.relation
        val conf = relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
        val broadcastedHadoopConf = relation.sparkSession.sparkContext.broadcast(
          new SerializableConfiguration(conf))
        val params = ClickHouseProcessorScan(
          relation.sparkSession,
          fileScanExec.execute().partitions,
          relation.dataSchema,
          relation.partitionSchema,
          fileScanExec.requiredSchema,
          pushedDownFilters(relation, fileScanExec.dataFilters),
          fileScanExec.output,
          relation.options,
          broadcastedHadoopConf
        )
        Some(clickhouseParams.copy(scan = Some(params)))
      case filterExec: FilterExec =>
        val params = ClickHouseProcessorFilter(filterExec.condition)
        Some(clickhouseParams.copy(filter = Some(params)))
      case projectExec: ProjectExec =>
        val params = ClickHouseProcessorProject(projectExec.projectList)
        Some(clickhouseParams.copy(project = Some(params)))
      case hashAggExec: HashAggregateExec =>
        val params = ClickHouseProcessorAggregate(
          hashAggExec.groupingExpressions,
          hashAggExec.aggregateExpressions,
          hashAggExec.aggregateAttributes,
          hashAggExec.resultExpressions,
          inputAttributesForAgg(hashAggExec.aggregateExpressions)
        )
        Some(clickhouseParams.copy(aggregate = Some(params)))
      case _ => None
    }
  }

  // Copied from FileSourceScanExec::pushedDownFilters
  private def pushedDownFilters(relation: HadoopFsRelation, dataFilters: Seq[Expression]) = {
    val supportNestedPredicatePushdown = DataSourceUtils.supportNestedPredicatePushdown(relation)
    dataFilters.flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown))
  }

  // Copied from BaseAggregateExec::inputAttributes
  private def inputAttributesForAgg(aggregateExpressions: Seq[AggregateExpression]) = {
    val modes = aggregateExpressions.map(_.mode).distinct
    if (modes.contains(Final) || modes.contains(PartialMerge)) {
      val aggAttrs = aggregateExpressions
        .filter(a => a.mode == Final || a.mode == PartialMerge).map(_.aggregateFunction)
        .flatMap(_.inputAggBufferAttributes)
      child.output.dropRight(aggAttrs.length) ++ aggAttrs
    } else {
      child.output
    }

  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean): Unit = {
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      if (printNodeId) "* " else s"*($clickHouseStageId) ",
      false,
      maxFields,
      printNodeId)
  }

  override protected def otherCopyArgs: Seq[AnyRef] = Seq(clickHouseStageId.asInstanceOf[Integer])
}

object ClickHouseExec {
  val PIPELINE_DURATION_METRIC = "duration"
}

/**
 * About the clickhouseStageCounter, please refer to CollapseCodegenStages.
 */
case class CollapseClickHouseStages(
    conf: SQLConf,
    clickhouseStageCounter: AtomicInteger = new AtomicInteger(0))
  extends Rule[SparkPlan] {

  /**
   * Inserts a WholeStageCodegen on top of those that support codegen.
   */
  private def insertClickHouseExec(plan: SparkPlan): SparkPlan = {
    plan match {
      case plan: ShuffleExchangeExec =>
        plan.withNewChildren(plan.children.map(
          ClickHouseExec(_)(clickhouseStageCounter.incrementAndGet())
        ))
      case other =>
        other.withNewChildren(other.children.map(insertClickHouseExec))
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.clickhouseEnabled) {
      insertClickHouseExec(plan)
    } else {
      plan
    }
  }
}
