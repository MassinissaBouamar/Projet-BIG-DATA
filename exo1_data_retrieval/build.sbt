ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

// Le template Spark 4.2.0 dépend de scala-library 2.13.18. On conserve ces
// dépendances à part : la question 3 utilise HTTP/S3 et reste en Scala 2.13.17.
lazy val SparkTemplate = config("sparkTemplate").hide

lazy val root = (project in file("."))
  .configs(SparkTemplate)
  .settings(
    name := "ex01_data_retrieval"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "4.2.0" % SparkTemplate
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.2.0" % SparkTemplate
// Source: https://mvnrepository.com/artifact/org.apache.spark/spark-hadoop-cloud
libraryDependencies += "org.apache.spark" %% "spark-hadoop-cloud" % "4.2.0" % SparkTemplate

// Client S3 natif : le transfert ne nécessite pas de session Spark.
libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3" % "2.31.54",
  "software.amazon.awssdk" % "url-connection-client" % "2.31.54",
  "org.slf4j" % "slf4j-nop" % "1.7.36",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.mockito" % "mockito-core" % "5.17.0" % Test
)

Compile / mainClass := Some("cytech.retrieval.Main")
Compile / run / fork := true
Test / fork := true
Compile / run / javaOptions ++= Seq("-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
Test / javaOptions ++= Seq("-Dfile.encoding=UTF-8", "-Dstdout.encoding=UTF-8", "-Dstderr.encoding=UTF-8")
