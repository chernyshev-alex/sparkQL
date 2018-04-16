import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.acme.chern",
      scalaVersion := "2.11.11",
      version      := "0.1"
    )),
    name := "apl-bd",   
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0",
    libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"
  )
