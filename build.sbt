ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "smart-meter-streaming",
    libraryDependencies ++= Seq(
      // Spark Core et SQL
      "org.apache.spark" %% "spark-core" % "3.3.2",
      "org.apache.spark" %% "spark-sql" % "3.3.2",
      "org.apache.spark" %% "spark-streaming" % "3.3.2",
      
      // Kafka
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
      "org.apache.kafka" % "kafka-clients" % "3.3.0",
      
      // Configuration
      "com.typesafe" % "config" % "1.4.2",
      
      // JSON
      "org.json4s" %% "json4s-jackson" % "4.0.6",
      
      // Logging
      "ch.qos.logback" % "logback-classic" % "1.4.6",
      
      // Test
      "org.scalatest" %% "scalatest" % "3.2.15" % Test
    ),
    
    // Options de compilation
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint"
    ),
    
    // Options Java pour résoudre les problèmes de modules
    javaOptions ++= Seq(
      "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens", "java.base/java.nio=ALL-UNNAMED",
      "--add-opens", "java.base/java.lang=ALL-UNNAMED"
    ),
    
    // Résolution des conflits de dépendances
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.4",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.13.4"
    ),
    
    // Configuration pour l'assembly
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList("reference.conf")    => MergeStrategy.concat
      case PathList("application.conf")  => MergeStrategy.concat
      case x => MergeStrategy.first
    }
  )
