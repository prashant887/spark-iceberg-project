ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "spark-iceberg-project"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0"
libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.4" % "1.4.2"
libraryDependencies += "software.amazon.awssdk" % "bundle" % "2.21.37"
libraryDependencies += "org.apache.iceberg" % "iceberg-aws" % "1.4.2"
libraryDependencies += "org.apache.iceberg" % "iceberg-core" % "1.4.2"
libraryDependencies += "org.apache.iceberg" % "iceberg-spark" % "1.4.2"
libraryDependencies += "software.amazon.awssdk" % "url-connection-client" % "2.21.37"