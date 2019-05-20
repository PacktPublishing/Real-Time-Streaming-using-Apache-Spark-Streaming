name := "spark-streaming"

lazy val commonSettings = Seq(
  version := "0.1",
  organization := "com.tomekl007",
  resolvers ++= Seq(
    "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
    "releases" at "http://oss.sonatype.org/content/repositories/releases",
    "maven central" at "http://central.maven.org/maven2/"
  ),
  scalacOptions in Compile ++= Seq(
    "-encoding", "UTF-8",
    "-target:jvm-1.7",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xlog-reflective-calls",
    "-Xlint"),
  javacOptions in Compile ++= Seq(
    "-source", "1.8",
    "-target", "1.8",
    "-Xlint:unchecked",
    "-Xlint:deprecation"),
  scalaVersion := "2.10.5",
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "2.2.5" % "test",
    "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
    "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1",
    "org.apache.spark" %% "spark-streaming" % "1.6.1",
    "org.apache.kafka" % "kafka-clients" % "0.8.2.1",
    "org.apache.kafka" %% "kafka" % "0.8.2.0",
    "commons-logging" % "commons-logging" % "1.2",
    "net.ceedubs" %% "ficus" % "1.0.1"
  ) map (_.excludeAll(
    ExclusionRule(organization = "org.slf4j"),
    ExclusionRule(organization = "log4j"),
    ExclusionRule(organization = "javax.servlet")
  )),
  libraryDependencies ++= Seq(("org.slf4j" % "slf4j-log4j12" % "1.7.10")
    .excludeAll(ExclusionRule(organization = "log4j"))),
  libraryDependencies += "log4j" % "log4j" % "1.2.16" % "test",

  assemblyMergeStrategy in assembly := {
    case PathList(xs@_*) if xs.last == "pom.xml" || xs.last == "pom.properties" =>
      MergeStrategy.rename
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)

lazy val root = (project in file(".")).aggregate("spark-streaming")


lazy val `spark-streaming` = project.in(file("spark-streaming"))
  .settings(commonSettings: _*)


