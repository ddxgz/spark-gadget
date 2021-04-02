package datautils

// import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}

abstract class DataSource(sourceType: String, defaultFormat: String)

trait RootPather {
  def rootPath(): String
}

trait DfLoader {
  def load(): DataFrame
}

trait DfSaver {
  def save(): DataFrame
}

abstract class FileDataSource(
    val sourceType: String,
    val pathPrefix: Option[String] = None,
    val defaultFormat: String
) extends DataSource(sourceType, defaultFormat)
    with RootPather {

  def file(path: String): String = { rootPath.stripSuffix("/") + s"/$path" }

  override def toString(): String = s"[$sourceType] $rootPath"
}

case class DataSourceAzBlob(
    val sourceType: String = "Azure Blob",
    val blob: String,
    val container: String,
    val pathPrefix: Option[String] = None,
    val secretScope: String,
    val secretKey: String,
    val defaultFormat: String = "parquet",
    val mountNow: Boolean = true
) extends FileDataSource(sourceType, pathPrefix, defaultFormat)
    with RootPather {

  val mountSource =
    s"wasbs://$container@$blob.blob.core.windows.net"

  val mountPath = s"/mnt/$blob-$container"

  val rootPath = pathPrefix match {
    case Some(prefix) => s"dbfs://$mountPath/$prefix"
    case None         => s"dbfs://$mountPath"
  }

}
