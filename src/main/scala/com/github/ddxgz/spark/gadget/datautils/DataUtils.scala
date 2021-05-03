package com.github.ddxgz.spark.gadget.datautils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
// import com.databricks.dbutils_v1.DBUtilsV1
// import com.databricks.service.DBUtils

class DataUtils(secretScope: String, spark: SparkSession) {
  // val dbutils = DataUtils.dbutils

  def getDataSourceDbfs(
      pathPrefix: Option[String] = None
  ): DataSourceDbfs = {

    DataSourceDbfs(pathPrefix = pathPrefix)
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

  def getDataSourceAzSynapse(
      jdbcUrlKey: String,
      tempDir: String
  ): DataSourceDbfs = {

    val jdbcUrl = dbutils.secrets.get(scope = secretScope, key = jdbcUrlKey)
    DataSourceDbfs(jdbcUrl = jdbcUrl, tempDir = tempDir, spark = spark)
  }

  def getDataSourceAzSynapse(
      name: String,
      port: Int,
      database: String,
      userKey: String,
      passwordKey: String,
      tempDir: String
  ): DataSourceDbfs = {
    val jdbcUrl =
      DataUtils.getJdbcUrl(
        name,
        port,
        database,
        secretScope,
        userKey,
        passwordKey
      )
    DataSourceDbfs(jdbcUrl = jdbcUrl, tempDir = tempDir, spark = spark)
  }

  def getDataSourceAzBlob(
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

    getDataSourceAzBlob(
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
  // val dbutils = com.databricks.service.DBUtils

  // var dbutils = com.databricks.dbutils_v1.DBUtilsHolder.dbutils

  def apply(
      secretScope: String,
      spark: SparkSession
      // dbutilsin: DBUtilsV1
  ): DataUtils = {
    // val dbutils = com.databricks.service.DBUtils
    // dbutils = dbutilsin
    val du = new DataUtils(secretScope, spark)
    return du
  }

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

  def compareSchema(
      schemaActual: StructType,
      schemaExpected: StructType
  ): Unit = {
    assert(
      schemaActual.fields.sortBy(_.name).deep == schemaExpected.fields
        .sortBy(_.name)
        .deep
    )
  }
}
