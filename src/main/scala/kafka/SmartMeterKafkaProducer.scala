package kafka

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.Properties
import java.nio.file.{Files, Paths}
import java.io.File

/**
 * Producteur Kafka qui simule un flux de données en temps réel
 * en envoyant des données historiques à intervalles réguliers
 * Utilise Spark pour lire et traiter les données
 */
object SmartMeterKafkaProducer {
  
  // Charger la configuration
  val config: Config = ConfigFactory.load()
  val bootstrapServers: String = config.getString("kafka.bootstrap.servers")
  val metersTopic: String = config.getString("kafka.topic.meters")
  val sleepInterval: Long = config.getLong("kafka.producer.interval.ms")
  val halfHourlyDataPath: String = config.getString("paths.meters.halfhourly")

  // Initialiser SparkSession
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("SmartMeterKafkaProducer")
    .master("local[*]")
    .getOrCreate()
  
  // Configuration du producteur Kafka
  def createProducerProps(): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props
  }
  
  /**
   * Démarrer le producteur qui simule le flux de données en temps réel
   */
  def start(): Unit = {
    println(s"Démarrage du producteur Kafka pour envoyer des données au topic $metersTopic...")
    
    // Configurer le niveau de log
    spark.sparkContext.setLogLevel("WARN")
    
    val producer = new KafkaProducer[String, String](createProducerProps())
    
    try {
      // Parcourir les blocs de données (block_0.csv, block_1.csv, etc.)
      val dataDir = new File(halfHourlyDataPath)
      if (dataDir.exists() && dataDir.isDirectory()) {
        val blockFiles = dataDir.listFiles()
          .filter(file => file.getName.endsWith(".csv"))
          .sortBy(_.getName)
          .map(_.getAbsolutePath)
          
        println(s"Fichiers trouvés: ${blockFiles.mkString(", ")}")
        
        // Traiter chaque fichier un par un
        blockFiles.foreach { file =>
          println(s"Traitement du fichier: $file")
          processFileWithSpark(file, producer)
        }
      } else {
        println(s"Le répertoire $halfHourlyDataPath n'existe pas ou n'est pas un répertoire")
      }
    } finally {
      producer.close()
      // Ne pas arrêter Spark ici si d'autres composants peuvent l'utiliser
    }
  }
  
  /**
   * Traite un fichier CSV avec Spark et envoie chaque ligne au topic Kafka
   */
  def processFileWithSpark(filePath: String, producer: KafkaProducer[String, String]): Unit = {
    // Lire le fichier CSV avec Spark
    val df = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(filePath)
    
    // Convertir chaque ligne en JSON et l'envoyer à Kafka
    val rows = df.collect()
    val columns = df.columns
    
    rows.foreach { row =>
      // Extraire le meterid (première colonne, généralement)
      val meterid = row.getAs[String](0)
      
      // Construire un message JSON à partir de la ligne
      val jsonMessage = buildJsonMessageFromRow(columns, row)
      
      // Créer et envoyer un enregistrement Kafka
      val record = new ProducerRecord[String, String](metersTopic, meterid, jsonMessage)
      producer.send(record)
      
      // Attendre l'intervalle défini pour simuler des données en temps réel
      Thread.sleep(sleepInterval)
    }
  }
  
  /**
   * Construit un message JSON à partir d'une ligne Spark Row
   */
  def buildJsonMessageFromRow(columns: Array[String], row: org.apache.spark.sql.Row): String = {
    val pairs = columns.zipWithIndex.map { case (colName, idx) => 
      val value = if (row.isNullAt(idx)) "null" else {
        row.get(idx) match {
          case n: Number => n.toString
          case d: java.sql.Date => s""""${d.toString}""""
          case t: java.sql.Timestamp => s""""${t.toString}""""
          case s: String => s""""$s""""
          case x => s""""$x""""
        }
      }
      s""""$colName": $value"""
    }
    
    s"{${pairs.mkString(", ")}}"
  }
  
  def main(args: Array[String]): Unit = {
    start()
    // Fermer Spark à la fin de l'exécution autonome
    spark.stop()
  }
} 