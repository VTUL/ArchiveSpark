import sbt.Keys._

lazy val commonSettings = Seq(
  name := "archivespark-benchmarking",
  organization := "de.l3s",
  version := "2.0.0",
  scalaVersion := "2.10.5",
  fork := true
)

lazy val archivesparkBenchmarking = (project in file(".")).
  settings(commonSettings: _*).dependsOn(file("../..")).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.6.2" % "provided" excludeAll(
        ExclusionRule(organization = "org.apache.hadoop"),
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule(organization = "com.google.guava")),
      "org.apache.hadoop" % "hadoop-client" % "2.5.0" % "provided",
      "org.apache.hbase" % "hbase-shaded" % "1.2.0",
      "org.apache.hbase" % "hbase-shaded-client" % "1.2.0",
      "org.apache.hbase" % "hbase-shaded-server" % "1.2.0"
    )
  )

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}