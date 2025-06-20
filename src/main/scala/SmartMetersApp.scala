import kafka.SmartMeterKafkaProducer
import streaming.SmartMeterStreamProcessorSparkOnly


object SmartMetersApp {
  
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: SmartMetersApp [producer|consumer|both]")
      println("  producer - Lance uniquement le producteur Kafka")
      println("  consumer - Lance uniquement le consommateur Spark Streaming")
      println("  both     - Lance les deux en parallèle")
      System.exit(1)
    }
    
    val mode = args(0).toLowerCase
    
    mode match {
      case "producer" =>
        println("Démarrage du producteur Kafka...")
        SmartMeterKafkaProducer.start()
        
      case "consumer" =>
        println("Démarrage du consommateur Spark Streaming...")
        SmartMeterStreamProcessorSparkOnly.main(Array())
        
      case "both" =>
        println("Démarrage du producteur Kafka et du consommateur Spark Streaming en parallèle...")
        
        // Démarrer le producteur dans un thread séparé
        val producerThread = new Thread {
          override def run(): Unit = {
            SmartMeterKafkaProducer.start()
          }
        }
        
        // Démarrer le thread du producteur
        producerThread.start()
        
        // Démarrer le consommateur dans le thread principal
        SmartMeterStreamProcessorSparkOnly.main(Array())
        
      case _ =>
        println(s"Mode non reconnu: $mode")
        println("Usage: SmartMetersApp [producer|consumer|both]")
        System.exit(1)
    }
  }
} 