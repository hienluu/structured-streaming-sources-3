ThisBuild / scalaVersion := "2.12.7"
ThisBuild / organization := "org.structured-streaming-sources"

def sparkVersion = "3.1.2"

lazy val streaming_sources = (project in file("streaming-sources"))
  .enablePlugins(SbtPlugin)
  .settings(
    version := "0.0.1",
    name := "structured-streaming-sources",
    libraryDependencies ++= Seq (
        "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
        "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
        "org.twitter4j" % "twitter4j-stream" % "4.0.6"
    )
  )


lazy val example_sources = (project in file("examples")).dependsOn(streaming_sources)
  .settings(
    version := "0.0.1",
    name := "structured-streaming-examples",
    libraryDependencies ++= Seq (
        "org.apache.spark" %% "spark-core" % sparkVersion ,
        "org.apache.spark" %% "spark-streaming" % sparkVersion,
        "org.apache.spark" %% "spark-sql" % sparkVersion,
        "org.apache.spark" %% "spark-catalyst" % sparkVersion,
        "org.twitter4j" % "twitter4j-stream" % "4.0.6"
    )
  )




