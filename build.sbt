lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "edu.usma",
      scalaVersion := "2.11.12",
      version      := "0.0.1-SNAPSHOT"
    )),
    name := "cc",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
      "org.apache.spark" % "spark-streaming_2.11" % "2.3.0",
      "org.jwat" % "jwat-warc" % "1.1.0",
      "com.martinkl.warc" % "warc-hadoop" % "0.1.0"
    )
  )
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
