package kafka

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

/**
 * Producteur Kafka qui simule un flux de données en temps réel
 * en envoyant des données historiques à intervalles réguliers
 * Utilise Spark Structured Streaming pour lire et traiter les données
 */
object SmartMeterKafkaProducer {

  // Charger la configuration
  private val config            = ConfigFactory.load()
  private val bootstrapServers  = config.getString("kafka.bootstrap.servers")
  private val metersTopic       = config.getString("kafka.topic.meters")
  private val sleepIntervalMs   = config.getLong("kafka.producer.interval.ms")
  private val halfHourlyDataDir = config.getString("paths.meters.halfhourly")
  private val dailyDataDir      = config.getString("paths.meters.daily")
  private val checkpointDir     = config.getString("paths.checkpoint")

  // Initialiser SparkSession
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("SmartMeterKafkaProducer")
    .master("local[*]")
    .getOrCreate()

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

    val query = kafkaStream
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", metersTopic)
      .option("checkpointLocation", checkpointDir)
      .option("kafka.partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner") // -> Meilleur répartion des données pour traitement en //
      .trigger(Trigger.ProcessingTime(s"${sleepIntervalMs} milliseconds"))
      .outputMode("append")
      .start()

    query.awaitTermination() // bloque le thread principal jusqu'à que le flux soit terminé (garde le programme actif pour que le stream puisse continuer)

    println(s"Streaming démarré vers Kafka topic '$metersTopic' (fichiers: $halfHourlyDataDir, interval: $sleepIntervalMs ms)")

    // Traitement batch du daily_dataset (affichage simple)
    val dailySchema = new StructType()
      .add("LCLid", StringType)
      .add("day", StringType)
      .add("energy_median", DoubleType)
      .add("energy_mean", DoubleType)
      .add("energy_max", DoubleType)
      .add("energy_count", IntegerType)
      .add("energy_std", DoubleType)
      .add("energy_sum", DoubleType)
      .add("energy_min", DoubleType)

    val dailyDF = spark.read
      .schema(dailySchema)
      .option("header", "true")
      .csv(dailyDataDir)

    println("Exemple de données du daily_dataset :")
    dailyDF.show(5, truncate = false)

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    // Ajuster niveau de log
    spark.sparkContext.setLogLevel("WARN")
    // Démarrer le flux
    start()
    // On ne ferme pas spark ici, car awaitTermination bloque
  }
}
