package com.github.ddxgz.spark.gadget.etl

import org.apache.spark.sql.{Dataset, DataFrame, Column, functions => F}

/** A handler for accessing various of Spark DataFrame handy transformations. */
object CommonTransforms {

  /** Rename columns by passing a map.
    *
    * The order is not guarantted due to the nature of map.
    *
    * @param namePairs
    * @param df
    * @return
    */
  def renameColumns(
      namePairs: Map[String, String]
  )(df: DataFrame): DataFrame = df
    .select(namePairs.keys.toVector.map { oldName =>
      F.col(oldName)
        .alias(namePairs.getOrElse(oldName, oldName))
    }: _*)

  /** Rename Columns by referring to tuples, from tuple._1 to tuple._2 */
  def renameColumns(
      namePairs: (String, String)*
  )(df: DataFrame): DataFrame = df
    .select(namePairs.map { case (oldName, newName) =>
      F.col(oldName)
        .alias(newName)
    }: _*)

  /** Replace nulls in a Seq of columns to the specified value. */
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

  def nullToZero(cols: String*)(
      df: DataFrame
  ): DataFrame = {
    replaceNulls(cols, 0, "")(df)
  }

  def nullToMinus1(cols: String*)(
      df: DataFrame
  ): DataFrame = {
    replaceNulls(cols, -1, "")(df)
  }

  def nullToND(cols: String*)(
      df: DataFrame
  ): DataFrame = {
    replaceNulls(cols, "N/D", "")(df)
  }

  def nullToMinus2(cols: String*)(
      df: DataFrame
  ): DataFrame = {
    replaceNulls(cols, -2, "")(df)
  }

  def nullToNA(cols: String*)(
      df: DataFrame
  ): DataFrame = {
    replaceNulls(cols, "N/A", "")(df)
  }

  /** Add a column by using monotonically_increasing_id().
    *
    * Perform coalesce(1) (default True) if the dataset is small.
    */
  def addMonotonicId(colName: String, performCoalesce: Boolean = true)(
      df: DataFrame
  ): DataFrame = {
    val df_ = if (performCoalesce) df.coalesce(1) else df
    df_.withColumn(colName, F.monotonically_increasing_id() + 1)
  }

  /** Add Date ID column from a Seq of timestamp columns as Integer. The default
    * format is `yyyyMMdd`.
    */
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

  /** Change timezone of a Seq of timestamp columns from UTC to Oslo.
    */
  def utcToOslo(cols: String*)(df: DataFrame): DataFrame = {
    // def utcToOslo(cols: Seq[String])(df: DataFrame): DataFrame = {
    df.transform(utcToTimezone("Europe/Oslo", cols))
  }

  /** Change timezone of a Seq of timestamp columns from UTC to the specified
    *  timezone by `tzStr` in format like "Europe/Oslo".
    */
  def utcToTimezone(tzStr: String, cols: Seq[String])(
      df: DataFrame
  ): DataFrame = {
    cols.foldLeft(df) { (tdf, col) =>
      tdf.withColumn(col, F.from_utc_timestamp(F.col(col), tzStr))
    }
  }
}
