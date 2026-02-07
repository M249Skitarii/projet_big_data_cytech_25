ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "ex02_data_ingestion",
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.5" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.5" % "provided",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "software.amazon.awssdk" % "s3" % "2.17.100",
  "software.amazon.awssdk" % "aws-core" % "2.17.100",
  "software.amazon.awssdk" % "auth" % "2.17.100",
  "software.amazon.awssdk" % "regions" % "2.17.100"
)
resolvers ++= Seq(
  "Apache Releases" at "https://repository.apache.org/content/repositories/releases/"
)
Compile / mainClass := Some("SparkApp")
allowUnsafeScalaLibUpgrade := true