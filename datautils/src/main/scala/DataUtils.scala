package datautils

import org.apache.spark.sql.SparkSession
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

class DataUtils(secretScope: String, spark: SparkSession) {

  def getDataSourceAdls2BySecret(
      blobNameSecretKey: String,
      container: String,
      pathPrefix: Option[String] = None,
      secretScope: String = secretScope,
      secretKey: String
  ): DataSourceAdls2 = {
    val blob = dbutils.secrets.get(scope = secretScope, key = blobNameSecretKey)

    getDataSourceAdls2(blob, container, pathPrefix, secretScope, secretKey)
  }

  def getDataSourceAdls2(
      blob: String,
      container: String,
      pathPrefix: Option[String] = None,
      secretScope: String = secretScope,
      secretKey: String
  ): DataSourceAdls2 = {

    val storageKeySource =
      dbutils.secrets.get(scope = secretScope, key = secretKey)

    spark.conf.set(
      s"fs.azure.account.key.$blob.dfs.core.windows.net",
      storageKeySource
    )

    DataSourceAdls2(
      blob = blob,
      container = container,
      pathPrefix = pathPrefix,
      secretScope = secretScope,
      secretKey = secretKey
    )
  }

  def getDataSourceJdbc(
      jdbcUrlKey: String,
      tempDir: String
  ): DataSourceJdbc = {

    val jdbcUrl = dbutils.secrets.get(scope = secretScope, key = jdbcUrlKey)
    DataSourceJdbc(jdbcUrl = jdbcUrl, tempDir = tempDir, spark = spark)
  }

  def getDataSourceJdbc(
      name: String,
      port: Int,
      database: String,
      userKey: String,
      passwordKey: String,
      tempDir: String
  ): DataSourceJdbc = {
    val jdbcUrl =
      DataUtils.getJdbcUrl(
        name,
        port,
        database,
        secretScope,
        userKey,
        passwordKey
      )
    DataSourceJdbc(jdbcUrl = jdbcUrl, tempDir = tempDir, spark = spark)
  }

  // A wrap to get blob name by using key vault
  def getDataSourceBlobBySecret(
      blobNameSecretKey: String,
      container: String,
      pathPrefix: Option[String] = None,
      secretScope: String = secretScope,
      secretKey: String,
      mountNow: Boolean = true
  ): DataSourceAzBlob = {

    val blob =
      dbutils.secrets.get(scope = secretScope, key = blobNameSecretKey)

    new DataSourceAzBlob(
      blob = blob,
      container = container,
      pathPrefix = pathPrefix,
      secretScope = secretScope,
      secretKey = secretKey,
      mountNow = mountNow
    )
  }

  def getDataSourceBlob(
      blob: String,
      container: String,
      pathPrefix: Option[String] = None,
      secretScope: String = secretScope,
      secretKey: String,
      mountNow: Boolean = true
  ): DataSourceAzBlob = {
    new DataSourceAzBlob(
      blob = blob,
      container = container,
      pathPrefix = pathPrefix,
      secretScope = secretScope,
      secretKey = secretKey,
      mountNow = mountNow
    )
  }
}

object DataUtils {
  def apply(
      secretScope: String,
      spark: SparkSession
  ): DataUtils = {
    val du = new DataUtils(secretScope, spark)
    return du
  }

// def DataSourceJdbc()

  //sql-dw-001-pw
// jdbc:sqlserver://group-bi-dev-we-sql-001.database.windows.net:1433;database=group-bi-dev-we-sqldw-001;user=SPAdminSQLDW001@group-bi-dev-we-sql-001;password=sof2342343##HHaskjkjw;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;
  def getJdbcUrl(
      name: String,
      port: Int,
      database: String,
      secretScope: String,
      userKey: String,
      passwordKey: String
  ): String = {

    val dw = name
    val jdbcPort = port
    val hostname = s"$dw.database.windows.net"
    // val database = "group-bi-dev-we-sqldw-001"
    val user =
      dbutils.secrets.get(scope = secretScope, key = userKey)
    val pw =
      dbutils.secrets.get(scope = secretScope, key = passwordKey)

    s"jdbc:sqlserver://${hostname}:${jdbcPort};database=${database};user=${user}@${dw};password=${pw};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
  }

  def connectionString(
      storageAccount: String,
      secretScope: String,
      storageAccountKeyName: String
  ): String = {
    val storageKey =
      dbutils.secrets.get(scope = secretScope, key = storageAccountKeyName)

    s"DefaultEndpointsProtocol=https;AccountName=${storageAccount};AccountKey=${storageKey};EndpointSuffix=core.windows.net"
  }

  def mountAnyway(
      mountSource: String,
      mountPath: String,
      storageAccount: String,
      storageKey: String
  ): String = {
    // for a given path can only have one mount point
    val mounted = dbutils.fs.mounts().filter(_.mountPoint == mountPath)
    var msg = s"${mountPath}"

    // if mounted path is the target return
    if (mounted.length == 1 && mounted(0).mountPoint == mountPath) {
      if (mounted(0).source == mountSource) {
        return msg + s" already mounted source ${mountSource}"
      }
      msg += s"already mounted a different source ${mounted(0).source}, will unmount and remount the new source"
      dbutils.fs.unmount(mountPath)
    }

    dbutils.fs.mount(
      source = mountSource,
      mountPoint = mountPath,
      extraConfigs = Map(
        s"fs.azure.account.key.${storageAccount}.blob.core.windows.net" -> storageKey
      )
    )

    msg += s"\nMounted ${mountPath} to source ${mountSource}"
    return msg
  }

}
