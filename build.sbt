// name := "spark-gadget"
// organization := "com.github.ddxgz"

ThisBuild / scalaVersion := "2.12.12"

lazy val gadget = (project in file("."))
  .settings(
    // name := "",
    name := "spark-gadget",
    organization := "com.github.ddxgz",
    version := "0.1.2",
    libraryDependencies ++= Seq(
      //   "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",
      "com.databricks" % "dbutils-api_2.11" % "0.0.4" % "provided",
      "joda-time" % "joda-time" % "2.10.5" % "provided",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"
    )
  )

// lazy val datautils = (project in file("datautils"))
//   .settings(
//     name := "DataUtils",
//     libraryDependencies ++= Seq(
//       //   "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
//       "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",
//       "com.databricks" % "dbutils-api_2.11" % "0.0.4",
//       "org.scalatest" %% "scalatest" % "3.0.5" % "test"
//     )
//   )
