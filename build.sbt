name := "dota-etl"

Compile / run / mainClass := Option("dota.etl.DotaETL")

version := "0.1"

scalaVersion := "2.12.14"

logLevel := Level.Info

// Library versions
val akkaVersion = "2.6.15"
val akkaHttpVersion = "10.2.6"
val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  // http requests
  "com.typesafe.play" %% "play-ahc-ws-standalone" % "2.1.3",
  // Play JSON
  "com.typesafe.play" %% "play-json" % "2.9.2",
  // scala logging
  "ch.qos.logback" % "logback-classic" % "1.2.5" % Test,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  // scalatest
  "org.scalactic" %% "scalactic" % "3.2.9",
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  // spark
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // spark testing
  "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % "test"
)

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")