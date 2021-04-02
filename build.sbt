// name := "NibeProto"

// version := "1.0"

ThisBuild / scalaVersion := "2.12.12"
// ThisBuild / organization := "se.reinsight"

// val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"

// lazy val nibeDatabricks = (project in file("."))
//   .aggregate(nibeProtobuf)
//   .dependsOn(nibeProtobuf)
//   .settings(
//     name := "NibeDatabricks",
//     libraryDependencies ++= Seq(
//       "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
//       "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided",
//       "com.thesamet.scalapb" %% "sparksql-scalapb" % "0.11.0-RC1",
//       "org.scalatest" %% "scalatest" % "3.0.5" % "test"
//     )
//   )

lazy val datautils = (project in file("datautils"))
  .settings(
    name := "DataUtils",
    libraryDependencies ++= Seq(
      //   "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided"
    )
  )
