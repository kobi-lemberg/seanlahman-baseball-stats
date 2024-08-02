import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "seanlahman-baseball-stats",
  )

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % Versions.`config`,
  "org.apache.spark" %% "spark-sql" %  Versions.`spark` ,
  "mysql" % "mysql-connector-java" % Versions.`mysql`,
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % Versions.`testcontainers-scala` % Test,
  "com.dimafeng" %% "testcontainers-scala-mysql" % Versions.`testcontainers-scala` % Test
)


Universal / mappings ++= Seq(
  (Compile / resourceDirectory).value / "application.conf" -> "conf/application.conf",
  (Compile / resourceDirectory).value / "log4j.xml" -> "conf/log4j.xml",
)