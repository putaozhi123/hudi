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

package org.apache.spark.sql.adapter

import org.apache.hudi.Spark3RowSerDe
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.spark.SPARK_VERSION
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.{HoodieAvroSchemaConverters, HoodieSparkAvroSchemaConverters}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, InterpretedPredicate, Predicate}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{HoodieCatalystPlansUtils, HoodieSpark3CatalystPlanUtils, Row, SparkSession}

import scala.util.control.NonFatal

/**
 * Base implementation of [[SparkAdapter]] for Spark 3.x branch
 */
abstract class BaseSpark3Adapter extends SparkAdapter with Logging {

  override def createSparkRowSerDe(encoder: ExpressionEncoder[Row]): SparkRowSerDe = {
    new Spark3RowSerDe(encoder)
  }

  override def getAvroSchemaConverters: HoodieAvroSchemaConverters = HoodieSparkAvroSchemaConverters

  override def createSparkParsePartitionUtil(conf: SQLConf): SparkParsePartitionUtil = {
    new Spark3ParsePartitionUtil(conf)
  }

  override def parseMultipartIdentifier(parser: ParserInterface, sqlText: String): Seq[String] = {
    parser.parseMultipartIdentifier(sqlText)
  }

  /**
   * Combine [[PartitionedFile]] to [[FilePartition]] according to `maxSplitBytes`.
   */
  override def getFilePartitions(
      sparkSession: SparkSession,
      partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {
    FilePartition.getFilePartitions(sparkSession, partitionedFiles, maxSplitBytes)
  }

  override def isHoodieTable(table: LogicalPlan, spark: SparkSession): Boolean = {
    unfoldSubqueryAliases(table) match {
      case LogicalRelation(_, _, Some(table), _) => isHoodieTable(table)
      case relation: UnresolvedRelation =>
        try {
          isHoodieTable(getCatalystPlanUtils.toTableIdentifier(relation), spark)
        } catch {
          case NonFatal(e) =>
            logWarning("Failed to determine whether the table is a hoodie table", e)
            false
        }
      case DataSourceV2Relation(table: Table, _, _, _, _) => isHoodieTable(table.properties())
      case _=> false
    }
  }

  override def createExtendedSparkParser: Option[(SparkSession, ParserInterface) => ParserInterface] = {
    // since spark3.2.1 support datasourceV2, so we need to a new SqlParser to deal DDL statment
    if (SPARK_VERSION.startsWith("3.1")) {
      val loadClassName = "org.apache.spark.sql.parser.HoodieSpark312ExtendedSqlParser"
      Some {
        (spark: SparkSession, delegate: ParserInterface) => {
          val clazz = Class.forName(loadClassName, true, Thread.currentThread().getContextClassLoader)
          val ctor = clazz.getConstructors.head
          ctor.newInstance(spark, delegate).asInstanceOf[ParserInterface]
        }
      }
    } else {
      None
    }
  }

  override def createInterpretedPredicate(e: Expression): InterpretedPredicate = {
    Predicate.createInterpreted(e)
  }
}
