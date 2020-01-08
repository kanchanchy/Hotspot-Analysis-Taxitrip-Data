import sbt.Keys.{libraryDependencies, scalaVersion, version}


lazy val root = (project in file(".")).
  settings(
    name := "Hotspot-Part2",

    version := "0.1.0",

    scalaVersion := "2.11.11",

    organization  := "github.com/kanchanchy",

    publishMavenStyle := true,

    mainClass := Some("sparksql.Entrance")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.specs2" %% "specs2-core" % "2.4.16" % "test",
  "org.specs2" %% "specs2-junit" % "2.4.16" % "test"
)