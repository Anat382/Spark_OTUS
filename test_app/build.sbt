ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.7"

lazy val root = (project in file("."))
  .settings(
    name := "test_app"
  )

val sparkVersion = "3.3.2"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  //  "org.apache.spark" %% "spark-core" % sparkVersion
)

dependencyOverrides += "log4j" %% "log4j" % "1.2.17"
