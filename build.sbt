name := "SparkGadget"

version := "0.1.0"

ThisBuild / scalaVersion := "2.12.12"

// val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"

lazy val etl = (project in file("etl"))
  .settings(
    name := "ETL",
    libraryDependencies ++= Seq(
      //   "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.1.1", //% "provided",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
    )
  )

lazy val datautils = (project in file("datautils"))
  .settings(
    name := "DataUtils",
    libraryDependencies ++= Seq(
      //   "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
    )
  )
