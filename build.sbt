import play.sbt.PlayImport._

scalaVersion := "2.11.11"
lazy val sparkVersion = "2.0.2"

name         := "vehicle-routing-challenge"
organization := "com.matteoguarnerio"
version      := "0.0.1"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

libraryDependencies ++= Seq(
  filters,
  "org.apache.spark"            %%  "spark-core"            % sparkVersion,
  "org.apache.spark"            %%  "spark-streaming"       % sparkVersion,
  "org.apache.spark"            %%  "spark-sql"             % sparkVersion,
  "joda-time"                   %   "joda-time"             % "2.9.9"
)
