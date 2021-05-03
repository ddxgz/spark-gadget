package com.github.ddxgz.spark.gadget.datautils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}

abstract class DataSource(val sourceType: String)

trait RootPather {
  def rootPath(): String
}

trait DfLoader {
  def load(tableName: String): DataFrame
}

trait DfSaver {
  def save(
      df: DataFrame,
      tableName: String,
      saveMode: String
  ): Unit
}

abstract class DatabaseDataSource(
    override val sourceType: String
) extends DataSource(sourceType) {

  override def toString(): String = s"[$sourceType]"
}

abstract class FileDataSource(
    override val sourceType: String,
    val pathPrefix: Option[String] = None
    // val defaultFormat: String
) extends DataSource(sourceType)
    with RootPather {

  def composePath(left: String, right: String): String =
    left.stripSuffix("/") + s"/${right.stripPrefix("/")}"

  def path(to: String): String = composePath(rootPath(), to)

  def file(path: String): String = this.path(to = path)

  // def delta(path:String):DataFrame
  // def parquet(path:String): DataFrame

  override def toString(): String = s"[$sourceType] $rootPath"
}

/** DataSource to Azure Data Lake Storage Gen2
  *
  * @param sourceType
  * @param blob
  * @param container
  * @param pathPrefix
  * @param secretScope
  * @param secretKey
  */
case class DataSourceDbfs(
    override val sourceType: String = "DBFS",
    override val pathPrefix: Option[String] = None
) extends FileDataSource(sourceType, pathPrefix)
    with RootPather {

  val dbfsRootPath = "dbfs:/FileStore"

  val rootPath = pathPrefix match {
    case Some(prefix) => composePath(dbfsRootPath, prefix)
    case None         => s"$dbfsRootPath"
  }

}

/** DataSource to Azure Blob Storage.
  *
  * @param sourceType
  * @param blob
  * @param container
  * @param pathPrefix
  * @param secretScope
  * @param secretKey
  * @param defaultFormat
  * @param mountNow if to mount the blob to Databrocks at `/mnt/\*`
  */
case class DataSourceAzBlob(
    override val sourceType: String = "Azure Blob",
    val blob: String,
    val container: String,
    override val pathPrefix: Option[String] = None,
    val secretScope: String,
    val secretKey: String,
    var defaultFormat: String = "parquet",
    val mountNow: Boolean = true
) extends FileDataSource(sourceType, pathPrefix)
    with RootPather {

  val mountSource =
    s"wasbs://$container@$blob.blob.core.windows.net"

  val mountPath = s"/mnt/$blob-$container"

  val rootPath = pathPrefix match {
    case Some(prefix) => composePath(s"dbfs://$mountPath", prefix)
    case None         => s"dbfs://$mountPath"
  }
}

/** DataSource to Azure Data Lake Storage Gen2
  *
  * @param sourceType
  * @param blob
  * @param container
  * @param pathPrefix
  * @param secretScope
  * @param secretKey
  */
case class DataSourceAdls2(
    override val sourceType: String = "ADLS Gen2",
    blob: String,
    container: String,
    override val pathPrefix: Option[String] = None,
    secretScope: String,
    secretKey: String
) extends FileDataSource(sourceType, pathPrefix)
    with RootPather {

  val abfssRootPath =
    s"abfss://$container@$blob.dfs.core.windows.net"

  val rootPath = pathPrefix match {
    case Some(prefix) => composePath(abfssRootPath, prefix)
    case None         => s"$abfssRootPath"
  }

}

/** A DataSource for the source type of `Azure Synapse via JDBC`.
  *
  *  @constructor create a new DataSourceJdbc with JDBC URL and temp dir.
  *  @param jdbcUrl the database's JDBC URL
  *  @param tempDir the temporary directory in a blob storage
  */
case class DataSourceAzSynapse(
    override val sourceType: String = "Azure Synapse via JDBC",
    val jdbcUrl: String,
    val tempDir: String,
    val spark: SparkSession
) extends DatabaseDataSource(sourceType)
    with DfLoader
    with DfSaver {

  val defaultReadOptions = Map[String, String](
    "url" -> jdbcUrl,
    "tempDir" -> tempDir,
    "forwardSparkAzureStorageCredentials" -> "true"
  )

  val formatSqldw = "com.databricks.spark.sqldw"

  val rwTypes = Vector[String](
    "dbTable",
    "query"
  )

  def read(tableOrExpr: String, rwType: String): DataFrame = {
    assert(rwTypes.contains(rwType))
    val options = defaultReadOptions + (rwType -> tableOrExpr)
    spark.read
      .format(formatSqldw)
      .options(options)
      .load()
  }

  def load(tableName: String): DataFrame = read(tableName, "dbTable")

  def query(expr: String): DataFrame = read(expr, "query")

  def save(
      df: DataFrame,
      tableName: String,
      saveMode: String
  ): Unit = {
    val options = defaultReadOptions + ("dbTable" -> tableName)
    df.write
      .format(formatSqldw)
      .options(options)
      .mode(saveMode)
      .save()
  }
}
