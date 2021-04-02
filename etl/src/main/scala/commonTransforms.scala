package etl

import org.apache.spark.sql.{Dataset, DataFrame, Column, functions => F}

object commonTransforms {

  def renameColumns(
      namePairs: Map[String, String]
  )(df: DataFrame): DataFrame = df
    .select(namePairs.keys.toSeq.map { oldName =>
      F.col(oldName)
        .alias(namePairs.getOrElse(oldName, oldName))
    }: _*)

  def renameColumns(
      namePairs: Array[(String, String)]
  )(df: DataFrame): DataFrame = df
    .select(namePairs.map { case (oldName, newName) =>
      F.col(oldName)
        .alias(newName)
    }: _*)

  def replaceNulls(cols: Seq[String], as: Any, prefixFilled: String = "")(
      df: DataFrame
  ): DataFrame = {
    cols.foldLeft(df)((df, c) =>
      df.withColumn(
        s"$prefixFilled$c",
        F.when(F.col(c).isNull, as).otherwise(F.col(c))
      )
    )
  }

  def nullToZero(cols: Seq[String], prefixFilled: String = "")(
      df: DataFrame
  ): DataFrame = {
    replaceNulls(cols, 0, prefixFilled)(df)
  }

  def nullToMinus1(cols: Seq[String], prefixFilled: String = "")(
      df: DataFrame
  ): DataFrame = {
    replaceNulls(cols, -1, prefixFilled)(df)
  }

  def nullToND(cols: Seq[String], prefixFilled: String = "")(
      df: DataFrame
  ): DataFrame = {
    replaceNulls(cols, "N/D", prefixFilled)(df)
  }

  def nullToMinus2(cols: Seq[String], prefixFilled: String = "")(
      df: DataFrame
  ): DataFrame = {
    replaceNulls(cols, -2, prefixFilled)(df)
  }

  def nullToNA(cols: Seq[String], prefixFilled: String = "")(
      df: DataFrame
  ): DataFrame = {
    replaceNulls(cols, "N/A", prefixFilled)(df)
  }

  def addMonotonicId(colName: String, performCoalesce: Boolean = true)(
      df: DataFrame
  ): DataFrame = {
    val df_ = if (performCoalesce) df.coalesce(1) else df
    df_.withColumn(colName, F.monotonically_increasing_id() + 1)
  }

  def timestampToID(cols: Seq[String], format: String = "yyyyMMdd")(
      df: DataFrame
  ): DataFrame = {
    cols.foldLeft(df) { (tdf, col) =>
      tdf.withColumn(
        col,
        F.when(
          F.col(col).isNotNull,
          F.date_format(F.col(col), format).cast("int")
        ).otherwise(-1)
      )
    }
  }

  def utcToOslo(cols: Seq[String])(df: DataFrame): DataFrame = {
    df.transform(utcToTimezone("Europe/Oslo", cols))
  }

  def utcToTimezone(tzStr: String, cols: Seq[String])(
      df: DataFrame
  ): DataFrame = {
    cols.foldLeft(df) { (tdf, col) =>
      tdf.withColumn(col, F.from_utc_timestamp(F.col(col), tzStr))
    }
  }
}
