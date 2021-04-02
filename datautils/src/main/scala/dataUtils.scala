package cake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}
// import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

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
// trait DataSourceBlob {
//   def rootPath: String
// }

abstract class FileDataSource(sourceType: String, defaultFormat: String)
    extends DataSource(sourceType, defaultFormat)
    with RootPather {
  def file(path: String): String = { rootPath.stripSuffix("/") + s"/$path" }
}

case class DataSourceBlob(
    blob: String,
    container: String,
    pathPrefix: Option[String] = None,
    secretScope: String,
    secretKey: String,
    mountNow: Boolean = true
) extends FileDataSource(sourceType, defaultFormat)
    with RootPather {

  val mountSource =
    s"wasbs://$container@$blob.blob.core.windows.net"

  val mountPath = s"/mnt/$blob-$container"

  val rootPath = pathPrefix match {
    case Some(prefix) => s"dbfs://$mountPath/$prefix"
    case None         => s"dbfs://$mountPath"
  }

  override def toString(): String = rootPath

}
