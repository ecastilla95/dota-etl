name := "dota-etl"

Compile / run / mainClass := Option("DotaETL")

version := "0.1"

scalaVersion := "2.12.14"

// Library versions
val akkaVersion = "2.6.15"
val akkaHttpVersion = "10.2.6"
val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  // akka actors
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  // akka streams
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  // akka http
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  // scalatest
  "org.scalactic" %% "scalactic" % "3.2.9",
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",
  // spark
  "org.apache.spark" %% "spark-core" % sparkVersion
)