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

package org.apache.spark.sql.catalyst.catalog

import java.util.Date

import com.google.common.base.Objects

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{CatalystConf, FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Cast, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.StructType


/**
 * A function defined in the catalog.
 *
 * @param identifier name of the function
 * @param className fully qualified class name, e.g. "org.apache.spark.util.MyFunc"
 * @param resources resource types and Uris used by the function
 */
case class CatalogFunction(
    identifier: FunctionIdentifier,
    className: String,
    resources: Seq[FunctionResource])


/**
 * Storage format, used to describe how a partition or a table is stored.
 */
case class CatalogStorageFormat(
    // TODO(ekl) consider storing this field as java.net.URI for type safety. Note that this must
    // be converted to/from a hadoop Path object using new Path(new URI(locationUri)) and
    // path.toUri respectively before use as a filesystem path due to URI char escaping.
    locationUri: Option[String],
    inputFormat: Option[String],
    outputFormat: Option[String],
    serde: Option[String],
    compressed: Boolean,
    properties: Map[String, String]) {

  override def toString: String = {
    val serdePropsToString = CatalogUtils.maskCredentials(properties) match {
      case props if props.isEmpty => ""
      case props => "Properties: " + props.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
    }
    val output =
      Seq(locationUri.map("Location: " + _).getOrElse(""),
        inputFormat.map("InputFormat: " + _).getOrElse(""),
        outputFormat.map("OutputFormat: " + _).getOrElse(""),
        if (compressed) "Compressed" else "",
        serde.map("Serde: " + _).getOrElse(""),
        serdePropsToString)
    output.filter(_.nonEmpty).mkString("Storage(", ", ", ")")
  }

}

object CatalogStorageFormat {
  /** Empty storage format for default values and copies. */
  val empty = CatalogStorageFormat(locationUri = None, inputFormat = None,
    outputFormat = None, serde = None, compressed = false, properties = Map.empty)
}

/**
 * A partition (Hive style) defined in the catalog.
 *
 * @param spec partition spec values indexed by column name
 * @param storage storage format of the partition
 * @param parameters some parameters for the partition, for example, stats.
 */
case class CatalogTablePartition(
    spec: CatalogTypes.TablePartitionSpec,
    storage: CatalogStorageFormat,
    parameters: Map[String, String] = Map.empty) {

  override def toString: String = {
    val specString = spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
    val output =
      Seq(
        s"Partition Values: [$specString]",
        s"$storage",
        s"Partition Parameters:{${parameters.map(p => p._1 + "=" + p._2).mkString(", ")}}")

    output.filter(_.nonEmpty).mkString("CatalogPartition(\n\t", "\n\t", ")")
  }

  /** Return the partition location, assuming it is specified. */
  def location: String = storage.locationUri.getOrElse {
    val specString = spec.map { case (k, v) => s"$k=$v" }.mkString(", ")
    throw new AnalysisException(s"Partition [$specString] did not specify locationUri")
  }

  /**
   * Given the partition schema, returns a row with that schema holding the partition values.
   */
  def toRow(partitionSchema: StructType): InternalRow = {
    InternalRow.fromSeq(partitionSchema.map { field =>
      // TODO: use correct timezone for partition values.
      Cast(Literal(spec(field.name)), field.dataType,
        Option(DateTimeUtils.defaultTimeZone().getID)).eval()
    })
  }
}


/**
 * A container for bucketing information.
 * Bucketing is a technology for decomposing data sets into more manageable parts, and the number
 * of buckets is fixed so it does not fluctuate with data.
 *
 * @param numBuckets number of buckets.
 * @param bucketColumnNames the names of the columns that used to generate the bucket id.
 * @param sortColumnNames the names of the columns that used to sort data in each bucket.
 */
case class BucketSpec(
    numBuckets: Int,
    bucketColumnNames: Seq[String],
    sortColumnNames: Seq[String]) {
  if (numBuckets <= 0 || numBuckets >= 100000) {
    throw new AnalysisException(
      s"Number of buckets should be greater than 0 but less than 100000. Got `$numBuckets`")
  }

  override def toString: String = {
    val bucketString = s"bucket columns: [${bucketColumnNames.mkString(", ")}]"
    val sortString = if (sortColumnNames.nonEmpty) {
      s", sort columns: [${sortColumnNames.mkString(", ")}]"
    } else {
      ""
    }
    s"$numBuckets buckets, $bucketString$sortString"
  }
}

/**
 * A table defined in the catalog.
 *
 * Note that Hive's metastore also tracks skewed columns. We should consider adding that in the
 * future once we have a better understanding of how we want to handle skewed columns.
 *
 * @param provider the name of the data source provider for this table, e.g. parquet, json, etc.
 *                 Can be None if this table is a View, should be "hive" for hive serde tables.
 * @param unsupportedFeatures is a list of string descriptions of features that are used by the
 *        underlying table but not supported by Spark SQL yet.
 * @param tracksPartitionsInCatalog whether this table's partition metadata is stored in the
 *                                  catalog. If false, it is inferred automatically based on file
 *                                  structure.
 */
case class CatalogTable(
    identifier: TableIdentifier,
    tableType: CatalogTableType,
    storage: CatalogStorageFormat,
    schema: StructType,
    provider: Option[String] = None,
    partitionColumnNames: Seq[String] = Seq.empty,
    bucketSpec: Option[BucketSpec] = None,
    owner: String = "",
    createTime: Long = System.currentTimeMillis,
    lastAccessTime: Long = -1,
    properties: Map[String, String] = Map.empty,
    stats: Option[CatalogStatistics] = None,
    viewText: Option[String] = None,
    comment: Option[String] = None,
    unsupportedFeatures: Seq[String] = Seq.empty,
    tracksPartitionsInCatalog: Boolean = false) {

  import CatalogTable._

  /**
   * schema of this table's partition columns
   */
  def partitionSchema: StructType = {
    val partitionFields = schema.takeRight(partitionColumnNames.length)
    assert(partitionFields.map(_.name) == partitionColumnNames)

    StructType(partitionFields)
  }

  /**
   * schema of this table's data columns
   */
  def dataSchema: StructType = {
    val dataFields = schema.dropRight(partitionColumnNames.length)
    StructType(dataFields)
  }

  /** Return the database this table was specified to belong to, assuming it exists. */
  def database: String = identifier.database.getOrElse {
    throw new AnalysisException(s"table $identifier did not specify database")
  }

  /** Return the table location, assuming it is specified. */
  def location: String = storage.locationUri.getOrElse {
    throw new AnalysisException(s"table $identifier did not specify locationUri")
  }

  /** Return the fully qualified name of this table, assuming the database was specified. */
  def qualifiedName: String = identifier.unquotedString

  /**
   * Return the default database name we use to resolve a view, should be None if the CatalogTable
   * is not a View or created by older versions of Spark(before 2.2.0).
   */
  def viewDefaultDatabase: Option[String] = properties.get(VIEW_DEFAULT_DATABASE)

  /**
   * Return the output column names of the query that creates a view, the column names are used to
   * resolve a view, should be empty if the CatalogTable is not a View or created by older versions
   * of Spark(before 2.2.0).
   */
  def viewQueryColumnNames: Seq[String] = {
    for {
      numCols <- properties.get(VIEW_QUERY_OUTPUT_NUM_COLUMNS).toSeq
      index <- 0 until numCols.toInt
    } yield properties.getOrElse(
      s"$VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX$index",
      throw new AnalysisException("Corrupted view query output column names in catalog: " +
        s"$numCols parts expected, but part $index is missing.")
    )
  }

  /** Syntactic sugar to update a field in `storage`. */
  def withNewStorage(
      locationUri: Option[String] = storage.locationUri,
      inputFormat: Option[String] = storage.inputFormat,
      outputFormat: Option[String] = storage.outputFormat,
      compressed: Boolean = false,
      serde: Option[String] = storage.serde,
      properties: Map[String, String] = storage.properties): CatalogTable = {
    copy(storage = CatalogStorageFormat(
      locationUri, inputFormat, outputFormat, serde, compressed, properties))
  }

  override def toString: String = {
    val tableProperties = properties.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
    val partitionColumns = partitionColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
    val bucketStrings = bucketSpec match {
      case Some(BucketSpec(numBuckets, bucketColumnNames, sortColumnNames)) =>
        val bucketColumnsString = bucketColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
        val sortColumnsString = sortColumnNames.map(quoteIdentifier).mkString("[", ", ", "]")
        Seq(
          s"Num Buckets: $numBuckets",
          if (bucketColumnNames.nonEmpty) s"Bucket Columns: $bucketColumnsString" else "",
          if (sortColumnNames.nonEmpty) s"Sort Columns: $sortColumnsString" else ""
        )

      case _ => Nil
    }

    val output =
      Seq(s"Table: ${identifier.quotedString}",
        if (owner.nonEmpty) s"Owner: $owner" else "",
        s"Created: ${new Date(createTime).toString}",
        s"Last Access: ${new Date(lastAccessTime).toString}",
        s"Type: ${tableType.name}",
        if (schema.nonEmpty) s"Schema: ${schema.mkString("[", ", ", "]")}" else "",
        if (provider.isDefined) s"Provider: ${provider.get}" else "",
        if (partitionColumnNames.nonEmpty) s"Partition Columns: $partitionColumns" else ""
      ) ++ bucketStrings ++ Seq(
        viewText.map("View: " + _).getOrElse(""),
        comment.map("Comment: " + _).getOrElse(""),
        if (properties.nonEmpty) s"Properties: $tableProperties" else "",
        if (stats.isDefined) s"Statistics: ${stats.get.simpleString}" else "",
        s"$storage",
        if (tracksPartitionsInCatalog) "Partition Provider: Catalog" else "")

    output.filter(_.nonEmpty).mkString("CatalogTable(\n\t", "\n\t", ")")
  }
}

object CatalogTable {
  val VIEW_DEFAULT_DATABASE = "view.default.database"
  val VIEW_QUERY_OUTPUT_PREFIX = "view.query.out."
  val VIEW_QUERY_OUTPUT_NUM_COLUMNS = VIEW_QUERY_OUTPUT_PREFIX + "numCols"
  val VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX = VIEW_QUERY_OUTPUT_PREFIX + "col."
}

/**
 * This class of statistics is used in [[CatalogTable]] to interact with metastore.
 * We define this new class instead of directly using [[Statistics]] here because there are no
 * concepts of attributes or broadcast hint in catalog.
 */
case class CatalogStatistics(
    sizeInBytes: BigInt,
    rowCount: Option[BigInt] = None,
    colStats: Map[String, ColumnStat] = Map.empty) {

  /**
   * Convert [[CatalogStatistics]] to [[Statistics]], and match column stats to attributes based
   * on column names.
   */
  def toPlanStats(planOutput: Seq[Attribute]): Statistics = {
    val matched = planOutput.flatMap(a => colStats.get(a.name).map(a -> _))
    Statistics(sizeInBytes = sizeInBytes, rowCount = rowCount,
      attributeStats = AttributeMap(matched))
  }

  /** Readable string representation for the CatalogStatistics. */
  def simpleString: String = {
    val rowCountString = if (rowCount.isDefined) s", ${rowCount.get} rows" else ""
    s"$sizeInBytes bytes$rowCountString"
  }
}


case class CatalogTableType private(name: String)
object CatalogTableType {
  val EXTERNAL = new CatalogTableType("EXTERNAL")
  val MANAGED = new CatalogTableType("MANAGED")
  val VIEW = new CatalogTableType("VIEW")
}


/**
 * A database defined in the catalog.
 */
case class CatalogDatabase(
    name: String,
    description: String,
    locationUri: String,
    properties: Map[String, String])


object CatalogTypes {
  /**
   * Specifications of a table partition. Mapping column name to column value.
   */
  type TablePartitionSpec = Map[String, String]
}


/**
 * A [[LogicalPlan]] that represents a table.
 */
case class CatalogRelation(
    tableMeta: CatalogTable,
    dataCols: Seq[Attribute],
    partitionCols: Seq[Attribute]) extends LeafNode with MultiInstanceRelation {
  assert(tableMeta.identifier.database.isDefined)
  assert(tableMeta.partitionSchema.sameType(partitionCols.toStructType))
  assert(tableMeta.dataSchema.sameType(dataCols.toStructType))

  // The partition column should always appear after data columns.
  override def output: Seq[Attribute] = dataCols ++ partitionCols

  def isPartitioned: Boolean = partitionCols.nonEmpty

  override def equals(relation: Any): Boolean = relation match {
    case other: CatalogRelation => tableMeta == other.tableMeta && output == other.output
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(tableMeta.identifier, output)
  }

  /** Only compare table identifier. */
  override lazy val cleanArgs: Seq[Any] = Seq(tableMeta.identifier)

  override def computeStats(conf: CatalystConf): Statistics = {
    // For data source tables, we will create a `LogicalRelation` and won't call this method, for
    // hive serde tables, we will always generate a statistics.
    // TODO: unify the table stats generation.
    tableMeta.stats.map(_.toPlanStats(output)).getOrElse {
      throw new IllegalStateException("table stats must be specified.")
    }
  }

  override def newInstance(): LogicalPlan = copy(
    dataCols = dataCols.map(_.newInstance()),
    partitionCols = partitionCols.map(_.newInstance()))
}
