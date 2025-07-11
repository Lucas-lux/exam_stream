package models

import org.apache.spark.sql.types._

/**
 * Modèle représentant les données de consommation demi-horaires
 */
case class HalfHourlyReading(
  meterid: String,
  datetime: String,
  energy: Double,
  acornGroup: Option[String] = None,
  temperature: Option[Double] = None,
  humidity: Option[Double] = None
)

/**
 * Modèle représentant les données de consommation journalières
 */
case class DailyReading(
  meterid: String,
  day: String,
  energy: Double,
  acornGroup: Option[String] = None,
  temperature: Option[Double] = None,
  humidity: Option[Double] = None
)

/**
 * Informations sur les ménages
 */
case class HouseholdInfo(
  meterid: String,
  acorn: String,
  acornGroup: String
)

/**
 * Modèle pour les données météorologiques
 */
case class WeatherData(
  datetime: String, 
  temperature: Double,
  humidity: Double,
  precipIntensity: Double,
  visibility: Double
)

object SmartMeterSchemas {
  val meterSchema = new StructType()
    .add("LCLid", StringType)
    .add("tstp", StringType)
    .add("energy(kWh/hh)", DoubleType)
} 