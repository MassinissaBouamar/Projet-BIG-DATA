ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "ex01_data_retrieval"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "4.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.2.0"
// Source: https://mvnrepository.com/artifact/org.apache.spark/spark-hadoop-cloud
libraryDependencies += "org.apache.spark" %% "spark-hadoop-cloud" % "4.2.0"
