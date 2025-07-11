package kafka

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SmartMeterKafkaProducer {

  // Charger la configuration
  private val config            = ConfigFactory.load()
  private val bootstrapServers  = config.getString("kafka.bootstrap.servers")
  private val metersTopic       = config.getString("kafka.topic.meters")
  private val sleepIntervalMs   = config.getLong("kafka.producer.interval.ms")
  private val halfHourlyDataDir = config.getString("paths.meters.halfhourly")
  private val checkpointDir     = config.getString("paths.checkpoint")

  // Initialiser SparkSession
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("SmartMeterKafkaProducer")
    .master("local[*]")
    .getOrCreate()

  // Compteur de messages global
  private var messageCounter = 0

  def start(): Unit = {
    import spark.implicits._

    // Schéma demi-horaire
    val halfHourlySchema = new StructType()
      .add("LCLid", StringType)
      .add("tstp", StringType)
      .add("energy(kWh/hh)", DoubleType)

    // Lecture en streaming des fichiers demi-horaires
    val inputStream = spark.readStream
      .schema(halfHourlySchema)
      .option("header", "true")
      .option("maxFilesPerTrigger", 1)
      .csv(halfHourlyDataDir)
      .withColumn("tstp", to_timestamp($"tstp", "yyyy-MM-dd HH:mm:ss.SSSSSSSS")) // Correction du parsing timestamp

    val kafkaStream = inputStream
      .withColumn("key", $"LCLid".cast(StringType))
      .withColumn("value", to_json(struct(inputStream.columns.map(col): _*)))

    val startTime = System.currentTimeMillis()

    val query = kafkaStream
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .foreachBatch { (df: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
        val currentTime = LocalDateTime.now()
        val timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")
        val currentTimeStr = currentTime.format(timeFormatter)
        
        // Calculer le temps écoulé depuis le début
        val elapsedTimeS = (System.currentTimeMillis() - startTime) / 1000
        
        // Afficher les données en temps réel dans le terminal avec le format demandé
        println(s"\n🔄 Batch $batchId - Envoi de ${df.count()} messages vers Kafka:")
        println("=" * 120)
        
        df.select("key", "value").limit(10).collect().foreach { row =>
          val key = row.getString(0)
          val value = row.getString(1)
          
          // Parser le JSON pour extraire les informations
          val json = org.json4s.jackson.JsonMethods.parse(value)
          import org.json4s._
          implicit val formats = DefaultFormats
          
          val energy = (json \ "energy(kWh/hh)").extract[Double]
          val tstp = (json \ "tstp").extract[String]
          
          messageCounter += 1
          
          // Format demandé : 🕒 16:19:47 | 📊 Message #119601 | 🏠 MAC000322 | ⚡ 0.178 kWh | 📅 2014-02-19T04:00:00.000+01:00 | ⏱️  9s
          println(s"🕒 $currentTimeStr | 📊 Message #$messageCounter | 🏠 $key | ⚡ ${energy} kWh | 📅 $tstp | ⏱️  ${elapsedTimeS}s")
        }
        
        // Envoyer vers Kafka
        df.write
          .format("kafka")
          .option("kafka.bootstrap.servers", bootstrapServers)
          .option("topic", metersTopic)
          .save()
        
        println("=" * 120)
        println(s"✅ Batch $batchId envoyé vers topic '$metersTopic' - Total messages: $messageCounter")
        println()
      }
      .option("checkpointLocation", checkpointDir)
      .trigger(Trigger.ProcessingTime(s"${sleepIntervalMs} milliseconds"))
      .outputMode("append")
      .start()

    println(s"🚀 Streaming démarré vers Kafka topic '$metersTopic'")
    println(s"📂 Source: $halfHourlyDataDir")
    println(s"⏱️  Intervalle: $sleepIntervalMs ms")
    println(s"📡 Kafka: $bootstrapServers")
    println("=" * 120)

    query.awaitTermination() // bloque le thread principal jusqu'à que le flux soit terminé
  }

  def main(args: Array[String]): Unit = {
    // Ajuster niveau de log
    spark.sparkContext.setLogLevel("WARN")
    // Démarrer le flux
    start()
    // On ne ferme pas spark ici, car awaitTermination bloque
  }
}
