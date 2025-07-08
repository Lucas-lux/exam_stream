name := "smart-meters-streaming"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.3.2" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
  "org.apache.kafka" % "kafka-clients" % "3.3.1",
  "com.typesafe" % "config" % "1.4.2"
)

Compile / mainClass := Some("kafka.SmartMeterKafkaProducer")
assembly / mainClass := Some("kafka.SmartMeterKafkaProducer")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}
