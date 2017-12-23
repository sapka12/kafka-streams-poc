name := "kafka-streams-poc"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.0.0"
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka" % "1.0.0" % "test"
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka-streams" % "0.10.0"

