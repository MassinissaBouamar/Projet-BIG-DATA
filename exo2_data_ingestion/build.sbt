ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "exo2_data_ingestion"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "4.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.2.0"
// Source: https://mvnrepository.com/artifact/org.apache.spark/spark-hadoop-cloud
libraryDependencies += "org.apache.spark" %% "spark-hadoop-cloud" % "4.2.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
libraryDependencies += "org.postgresql" % "postgresql" % "42.7.14"
 // Executer Spark dans une JVM separee de SBT
Compile / run / fork := true

// Autoriser les acces Java necessaires a Spark
Compile / run / javaOptions ++= Seq(
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)
