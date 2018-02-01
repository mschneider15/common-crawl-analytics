lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "edu.usma",
      scalaVersion := "2.11.12",
      version      := "0.0.1-SNAPSHOT"
    )),
    name := "cc",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.2.1"
    )
  )
