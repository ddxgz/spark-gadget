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

      val dfRenamed = df.transform(CommonTransforms.renameColumns(newNames))

      assert(dfRenamed.columns(0) == "newcol1")
      assert(dfRenamed.columns(1) == "newcol2")
    }

    it("should reorder the columns") {

      val newNames = Seq(
        ("col2", "newcol2"),
        ("col1", "newcol1")
      )

      val dfRenamed = df.transform(CommonTransforms.renameColumns(newNames))

      assert(dfRenamed.columns(0) == "newcol2")
      assert(dfRenamed.columns(1) == "newcol1")
    }

  }
  describe("CommonTransforms::utcToOslo") {

    val df = Seq(
      (1, "2020-01-01T23:00:00"),
      (2, "2020-01-01T13:00:00")
    ).toDF("id", "ts_str")
      .withColumn("ts1", F.to_timestamp($"ts_str"))

    it("should changed timezone") {
      val df2 = df.transform(CommonTransforms.utcToOslo(Seq("ts1")))
      assert(
        df2.withColumn("str_oslo", F.date_format($"ts1", "yyyy-MM-dd")) == df
      )
    }
  }
}
