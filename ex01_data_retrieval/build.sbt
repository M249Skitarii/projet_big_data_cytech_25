ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"
val awsVersion = "2.20.0"
lazy val root = (project in file("."))
  .settings(
    name := "ex01_data_retrieval",
  )
libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"      % awsVersion,
  "software.amazon.awssdk" % "auth"    % awsVersion,
  "software.amazon.awssdk" % "regions" % awsVersion,
)