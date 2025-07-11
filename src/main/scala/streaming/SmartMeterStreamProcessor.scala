package streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import models.SmartMeterSchemas.meterSchema

object SmartMeterStreamProcessorSparkOnly {

  def main(args: Array[String]): Unit = {
    // 1. Initialisation de SparkSession
    val spark = SparkSession.builder()
      .appName("SmartMeterStreamProcessorSparkOnly")
      .getOrCreate()
    import spark.implicits._

    // 2. Chargement des paramètres depuis spark.conf
    val bootstrapServers    = spark.conf.get("spark.kafka.bootstrap.servers")
    val metersTopic         = spark.conf.get("spark.kafka.topic.meters")
    val householdsInfoPath  = spark.conf.get("spark.paths.households.info")
    val weatherHourlyPath   = spark.conf.get("spark.paths.weather.hourly")
    val checkpointDir       = spark.conf.get("spark.checkpoint.dir")
    val triggerIntervalMs   = spark.conf.get("spark.stream.interval.ms").toLong
    val halfHourlyDataDir   = "data/halfourlydataset"

    spark.sparkContext.setLogLevel("WARN")
    println(s"Configures: kafka=$bootstrapServers/$metersTopic," +
            s" households=$householdsInfoPath," +
            s" weather=$weatherHourlyPath," +
            s" checkpoint=$checkpointDir," +
            s" trigger=$triggerIntervalMs ms")

    // 3. Schéma du JSON Kafka
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

    // 4. Lecture des données statiques en batch
    val householdInfoDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(householdsInfoPath)
      .withColumnRenamed("id", "meterid") // adapter si besoin

    val weatherDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(weatherHourlyPath)
      .withColumn("timestamp", to_timestamp($"datetime", "yyyy-MM-dd HH:mm:ss"))
      .withColumn("date",    to_date($"timestamp"))
      .withColumn("hour",    hour($"timestamp"))
      .drop("datetime")

    // 5. Lecture du flux Kafka
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", metersTopic)
      .option("startingOffsets", "earliest")
      .load()

    // 6. Parsing JSON et structuration
    val readings = rawStream
      .selectExpr("CAST(key AS STRING) as LCLid_key", "CAST(value AS STRING) as json")
      .select(from_json($"json", meterSchema).as("data"))
      .select("data.*")
      .withColumn("timestamp", to_timestamp($"tstp", "yyyy-MM-dd HH:mm:ss.SSSSSSSS"))
      .withColumn("energy", $"energy(kWh/hh)")
      .withColumn("meterid", $"LCLid")

    // 7. Enrichissement par jointure batch (broadcast)
    val enriched = readings
      .join(broadcast(householdInfoDF), "meterid")
      .join(broadcast(weatherDF),
            Seq("date", "hour"),
            "left")

    // 8. Agrégations & détection d'anomalies

    // 8.1 Consommation moyenne par groupe ACORN (30mn)
    val acornAgg = enriched
      .withWatermark("timestamp", "30 minutes")
      .groupBy(
        window($"timestamp", "30 minutes"),
        $"acornGroup"
      )
      .agg(
        avg("energy").alias("avg_energy"),
        count("*").alias("readings_count")
      )
      .select(
        $"window.start".alias("window_start"),
        $"window.end".alias("window_end"),
        $"acornGroup",
        $"avg_energy",
        $"readings_count"
      )

    // 8.2 Consommation totale par ménage (1h)
    val householdAgg = enriched
      .withWatermark("timestamp", "1 hour")
      .groupBy(
        window($"timestamp", "1 hour"),
        $"meterid"
      )
      .agg(
        sum("energy").alias("total_energy"),
        avg("temperature").alias("avg_temperature")
      )
      .select(
        $"window.start".alias("window_start"),
        $"window.end".alias("window_end"),
        $"meterid",
        $"total_energy",
        $"avg_temperature"
      )

    // 8.3 Détection de pics (seuil de consommation élevée)
    val threshold = 2.0
    val anomalies = readings.filter($"energy" > threshold)

    // 9. Écriture des résultats en console
    def writeConsole(df: DataFrame, name: String, trigMs: Long) =
      df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(Trigger.ProcessingTime(s"${trigMs} milliseconds"))
        .queryName(name)
        .start()

    val q1 = writeConsole(acornAgg,     "acornGroup",    triggerIntervalMs)
    val q2 = writeConsole(householdAgg, "householdTotal", triggerIntervalMs * 2)
    val q3 = writeConsole(anomalies,    "anomalies",     triggerIntervalMs / 3)

    // Traitement batch du daily_dataset (affichage simple)
    val dailyDataDir = "data/daily_dataset"
    val dailyDF = spark.read
      .schema(dailySchema)
      .option("header", "true")
      .csv(dailyDataDir)

    println("Exemple de données du daily_dataset :")
    dailyDF.show(5, truncate = false)

    // 10. Lancement et attente
    spark.streams.awaitAnyTermination()
  }
}
