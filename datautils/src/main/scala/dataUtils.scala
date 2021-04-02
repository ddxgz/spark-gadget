package datautils

// import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}

abstract class DataSource(val sourceType: String)

trait RootPather {
  def rootPath(): String
}

trait DfLoader {
  def load(): DataFrame
}

trait DfSaver {
  def save(): DataFrame
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

  def file(path: String): String = { rootPath.stripSuffix("/") + s"/$path" }

  override def toString(): String = s"[$sourceType] $rootPath"
}

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
    case Some(prefix) => s"dbfs://$mountPath/$prefix"
    case None         => s"dbfs://$mountPath"
  }

}

case class DataSourceJdbc(
    override val sourceType: String = "JDBC",
    val jdbcUrl: String,
    val tempDir: String
) extends DatabaseDataSource(sourceType) {

  val defaultReadOptions = Map[String, String](
    "url" -> jdbcUrl,
    "tempDir" -> tempDir,
    "forwardSparkAzureStorageCredentials" -> "true"
  )

  val formatSqldw = "com.databricks.spark.sqldw"

  val rwTypes = Array[String](
    "dbTable",
    "query"
  )
}
