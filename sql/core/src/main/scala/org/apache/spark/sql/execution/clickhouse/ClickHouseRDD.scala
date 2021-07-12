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

import scala.language.postfixOps
import scala.sys.process._

import org.apache.spark.{Partition => RDDPartition}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Add, And, Attribute, AttributeReference, BinaryOperator, Cast, Expression, GenericInternalRow, IsNotNull, LessThan, Literal, NamedExpression, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType, StringType, StructType}
import org.apache.spark.util.SerializableConfiguration

class ClickHouseRDD(
    @transient private val sparkSession: SparkSession,
    @transient val rddPartitions: Seq[RDDPartition],
    val processorParams: ClickHouseProcessorParams)
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  val clickHouseBinPath: String = sparkSession.sessionState.conf.clickhouseBinPath

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      var idx = 0

      override def hasNext: Boolean = {
        idx == 0
      }
      override def next(): InternalRow = {
        idx += 1
        val schema = StructType.fromAttributes(
          processorParams.aggregate.get.resultExpressions.map(_.toAttribute))
        val row = new GenericInternalRow(1)
        row.update(0, callClickHouse(split).trim.toLong)
        val toUnsafe = UnsafeProjection.create(schema)
        toUnsafe(row)
      }
    }
  }

  private def callClickHouse(split: RDDPartition) = {
    val files = split.asInstanceOf[FilePartition].files
    assert(files.length == 1, "only one file in split is supported currently")
    val agg = parseAgg()
    val filter = parseFilter()
    val path = files(0).filePath.stripPrefix("file://")
    val schema = processorParams.scan.get.dataSchema.fields.map(field => {
      val clickhouseType = field.dataType match {
        case IntegerType => "Nullable(Int32)"
        case StringType => "Nullable(String)"
        case _: DecimalType => "Nullable(Float32)"
        case FloatType => "Nullable(Float32)"
      }
      s"${field.name} $clickhouseType"
    }).mkString(", ")
    val cmd = Seq(
      clickHouseBinPath,
      "--local",
      "--query",
      s"select $agg from file('$path', 'Parquet', '$schema') $filter"
    )
    cmd .!!
  }

  private def parseFilter() = {
    if (processorParams.filter.isDefined) {
      val conditions = parseExpression(processorParams.filter.get.condition)
      s"where $conditions"
    } else {
      ""
    }
  }

  private def parseAgg() = {
    val aggExpressions = processorParams.aggregate.get.aggregateExpressions
    assert(aggExpressions.length == 1, "only one agg function supported currently")
    aggExpressions.head.aggregateFunction match {
      case sumFun: Sum =>
        val exp = parseExpression(sumFun.child)
        s"sum($exp)"
      case _ => throw new Exception("only sum aggregate function supported currently")
    }
  }

  private def parseExpression(exp: Expression): String = exp match {
    case cast: Cast =>
      parseExpression(cast.child)
    case b: BinaryOperator =>
      val left = parseExpression(b.left)
      val right = parseExpression(b.right)
      parseBinaryOperation(b, left, right)
    case ref: AttributeReference =>
      ref.name
    case l: Literal =>
      l.value.toString
    case isNotNull: IsNotNull =>
      val child = parseExpression(isNotNull.child)
      s"isNotNull($child)"
    case _ =>
      throw new Exception(s"unsupported expression $exp")
  }

  private def parseBinaryOperation(bin: BinaryOperator, left: String, right: String) = bin match {
    case _: Add =>
      s"($left + $right)"
    case _: LessThan =>
      s"($left < $right)"
    case _: And =>
      s"($left and $right)"
    case _ =>
      throw new Exception(s"unsupported expression $bin")
  }

  override protected def getPartitions: Array[RDDPartition] = rddPartitions.toArray
}

case class ClickHouseProcessorScan(
  sparkSession: SparkSession,
  partitions: Seq[RDDPartition],
  dataSchema: StructType,
  partitionSchema: StructType,
  requiredSchema: StructType,
  filters: Seq[Filter],
  output: Seq[Attribute],
  options: Map[String, String],
  hadoopConf: Broadcast[SerializableConfiguration]
)

case class ClickHouseProcessorFilter(
  condition: Expression
)

case class ClickHouseProcessorProject(
  projectList: Seq[NamedExpression]
)

case class ClickHouseProcessorAggregate(
  groupingExpressions: Seq[NamedExpression],
  aggregateExpressions: Seq[AggregateExpression],
  aggregateAttributes: Seq[Attribute],
  resultExpressions: Seq[NamedExpression],
  inputAttributes: Seq[Attribute]
)

case class ClickHouseProcessorParams(
  scan: Option[ClickHouseProcessorScan] = None,
  filter: Option[ClickHouseProcessorFilter] = None,
  project: Option[ClickHouseProcessorProject] = None,
  aggregate: Option[ClickHouseProcessorAggregate] = None
)
