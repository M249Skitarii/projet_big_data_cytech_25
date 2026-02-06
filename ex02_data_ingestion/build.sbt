ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "ex02_data_ingestion",
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.5" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.5" % "provided",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4"
)
resolvers ++= Seq(
  "Apache Releases" at "https://repository.apache.org/content/repositories/releases/"
)
Compile / mainClass := Some("SparkApp")
allowUnsafeScalaLibUpgrade := true