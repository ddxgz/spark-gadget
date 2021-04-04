package etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame, Row, Column, functions => F}
import org.scalatest.FunSpec

class CommonTransformsTest extends FunSpec {
  val spark = SparkSession.builder().master("local").getOrCreate()

  import spark.implicits._

  describe("CommonTransforms::renameColumns") {

    val df = Seq(
      (1, "a"),
      (2, "b")
    ).toDF("col1", "col2")

    it("should rename the columns") {

      val newNames = Seq(
        ("col1", "newcol1"),
        ("col2", "newcol2")
      )

      assert(
        df.transform(CommonTransforms.renameColumns(newNames))
          .columns(0) == "newcol1"
      )
      assert(
        df.transform(CommonTransforms.renameColumns(newNames))
          .columns(1) == "newcol2"
      )
    }

    it("should reorder the columns") {

      val newNames = Seq(
        ("col2", "newcol2"),
        ("col1", "newcol1")
      )

      assert(
        df.transform(CommonTransforms.renameColumns(newNames))
          .columns(0) == "newcol2"
      )
      assert(
        df.transform(CommonTransforms.renameColumns(newNames))
          .columns(1) == "newcol1"
      )
    }
  }
}
