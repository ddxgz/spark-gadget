package com.github.ddxgz.spark.gadget.datautils

import org.scalatest.FunSpec

class DataSourceTest extends FunSpec {

  describe("DataSource::DataSourceAdls2") {

    // val blob = "blob"
    // val container = "container"
    // val pathPrefix = Some("pathto")

    // val ds = DataSourceAdls2(
    //   blob = blob,
    //   container = container,
    //   pathPrefix = pathPrefix,
    //   secretScope = "secretScope",
    //   secretKey = "secretKey"
    // )
    it("should compose correct rootPath") {

      val blob = "blob"
      val container = "container"
      val pathPrefix = Some("pathto")

      val ds = DataSourceAdls2(
        blob = blob,
        container = container,
        pathPrefix = pathPrefix,
        secretScope = "secretScope",
        secretKey = "secretKey"
      )
      assert(ds.rootPath == ds.abfssRootPath + "/pathto")

      val ds2 = DataSourceAdls2(
        blob = blob,
        container = container,
        pathPrefix = Some("/pathto"),
        secretScope = "secretScope",
        secretKey = "secretKey"
      )
      assert(ds.rootPath == ds.abfssRootPath + "/pathto")
    }

    it("should compose correct file path") {
      val blob = "blob"
      val container = "container"
      val pathPrefix = Some("pathto")

      val ds = DataSourceAdls2(
        blob = blob,
        container = container,
        pathPrefix = pathPrefix,
        secretScope = "secretScope",
        secretKey = "secretKey"
      )
      assert(ds.file("/abc/") == ds.rootPath + "/abc/")
      assert(ds.file("abc/") == ds.rootPath + "/abc/")
    }
  }

}
