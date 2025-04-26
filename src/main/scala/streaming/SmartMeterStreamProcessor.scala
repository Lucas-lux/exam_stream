package streaming

import com.typesafe.config.{Config, ConfigFactory}
import models.{HalfHourlyReading, HouseholdInfo, WeatherData}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types._

/**
 * Processeur de flux qui consomme les données de Kafka
 * et les traite en temps réel avec Spark Structured Streaming
 */
object SmartMeterStreamProcessor {
  
  // Charger la configuration
  val config: Config = ConfigFactory.load()
  val bootstrapServers: String = config.getString("kafka.bootstrap.servers")
  val metersTopic: String = config.getString("kafka.topic.meters")
  val appName: String = config.getString("spark.app.name")
  val sparkMaster: String = config.getString("spark.master")
  val checkpointDir: String = config.getString("spark.checkpoint.dir")
  val householdsInfoPath: String = config.getString("paths.households.info")
  val weatherHourlyPath: String = config.getString("paths.weather.hourly")

  // Schéma pour les données des compteurs
  val meterSchema = new StructType()
    .add("meterid", StringType)
    .add("datetime", StringType)
    .add("energy", DoubleType)

  // Initialiser SparkSession
  lazy val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .master(sparkMaster)
    .config("spark.sql.streaming.checkpointLocation", checkpointDir)
    .getOrCreate()

  import spark.implicits._

  /**
   * Démarrer le processeur de flux
   */
  def start(): Unit = {
    println("Démarrage du processeur de flux...")

    // Configurer le niveau de log
    spark.sparkContext.setLogLevel("WARN")
    
    // Charger les données de référence statiques
    val householdInfoDF = loadHouseholdsInfo()
    val weatherDataDF = loadWeatherData()
    
    // Créer le flux depuis Kafka
    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", metersTopic)
      .option("startingOffsets", "earliest")
      .load()

    // Extraire et convertir les valeurs JSON de Kafka
    val meterReadingsDF = kafkaStream
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .select(
        col("key").as("meterid"),
        from_json(col("value"), meterSchema).as("data")
      )
      .select("meterid", "data.*")

    // Conversion timestamp
    val parsedReadingsDF = meterReadingsDF
      .withColumn("timestamp", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("hour", hour(col("timestamp")))
      .withColumn("date", to_date(col("timestamp")))

    // Joindre avec les données de ménage
    val enrichedWithHouseholdsDF = parsedReadingsDF
      .join(householdInfoDF, "meterid")

    // Joindre avec les données météo sur l'heure la plus proche 
    val fullyEnrichedDF = enrichedWithHouseholdsDF
      .join(
        weatherDataDF,
        enrichedWithHouseholdsDF("date") === weatherDataDF("date") &&
        enrichedWithHouseholdsDF("hour") === weatherDataDF("hour"),
        "left"
      )
      .drop(weatherDataDF("date"))
      .drop(weatherDataDF("hour"))

    // Calculer des agrégations en temps réel
    
    // 1. Consommation moyenne par groupe ACORN (fenêtre de 30 minutes)
    val acornGroupConsumptionDF = fullyEnrichedDF
      .withWatermark("timestamp", "30 minutes")
      .groupBy(
        window(col("timestamp"), "30 minutes"),
        col("acornGroup")
      )
      .agg(
        avg("energy").as("avg_energy"),
        count("*").as("readings_count")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("acornGroup"),
        col("avg_energy"),
        col("readings_count")
      )

    // 2. Consommation totale par ménage (fenêtre de 1 heure)
    val householdTotalDF = fullyEnrichedDF
      .withWatermark("timestamp", "1 hour")
      .groupBy(
        window(col("timestamp"), "1 hour"),
        col("meterid")
      )
      .agg(
        sum("energy").as("total_energy"),
        avg("temperature").as("avg_temperature")
      )
      .select(
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("meterid"),
        col("total_energy"),
        col("avg_temperature")
      )

    // Détecter les pics de consommation (avec fenêtre glissante)
    val consumptionThreshold = 10.0 // à ajuster selon vos données
    val anomaliesDF = fullyEnrichedDF
      .withWatermark("timestamp", "30 minutes")
      .groupBy(
        window(col("timestamp"), "30 minutes", "10 minutes"),
        col("meterid")
      )
      .agg(
        avg("energy").as("avg_energy")
      )
      .filter(col("avg_energy") > consumptionThreshold)
      .select(
        col("window.start").as("anomaly_start"),
        col("window.end").as("anomaly_end"),
        col("meterid"),
        col("avg_energy")
      )

    // Démarrer les requêtes streaming pour écrire les résultats
    
    // Sortie pour consommation par groupe ACORN
    val acornGroupQuery = acornGroupConsumptionDF
      .writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", false)
      .option("numRows", 20)
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    // Sortie pour consommation totale par ménage
    val householdTotalQuery = householdTotalDF
      .writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", false)
      .option("numRows", 20)
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    // Sortie pour les anomalies détectées
    val anomaliesQuery = anomaliesDF
      .writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", false)
      .option("numRows", 20)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Attendre que les requêtes soient terminées
    spark.streams.awaitAnyTermination()
  }

  /**
   * Charger les informations des ménages
   */
  def loadHouseholdsInfo(): DataFrame = {
    println(s"Chargement des données de ménage depuis $householdsInfoPath")
    
    try {
      spark.read
        .option("header", true)
        .option("inferSchema", true)
        .csv(householdsInfoPath)
    } catch {
      case e: Exception =>
        println(s"Erreur lors du chargement des informations des ménages: ${e.getMessage}")
        
        // Créer un DataFrame vide avec le schéma attendu en cas d'erreur
        spark.createDataFrame(
          spark.sparkContext.emptyRDD[HouseholdInfo],
          classOf[HouseholdInfo]
        )
    }
  }

  /**
   * Charger les données météo
   */
  def loadWeatherData(): DataFrame = {
    println(s"Chargement des données météo depuis $weatherHourlyPath")
    
    try {
      val weatherDF = spark.read
        .option("header", true)
        .option("inferSchema", true)
        .csv(weatherHourlyPath)
        
      // Extraire date et heure à partir du timestamp
      weatherDF
        .withColumn("timestamp", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("date", to_date(col("timestamp")))
        .withColumn("hour", hour(col("timestamp")))
    } catch {
      case e: Exception =>
        println(s"Erreur lors du chargement des données météo: ${e.getMessage}")
        
        // Créer un DataFrame vide avec le schéma attendu en cas d'erreur
        val emptyWeatherRDD = spark.sparkContext.emptyRDD[WeatherData]
        val weatherDF = spark.createDataFrame(emptyWeatherRDD, classOf[WeatherData])
        
        weatherDF
          .withColumn("timestamp", to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss"))
          .withColumn("date", to_date(col("timestamp")))
          .withColumn("hour", hour(col("timestamp")))
    }
  }

  def main(args: Array[String]): Unit = {
    start()
  }
} 