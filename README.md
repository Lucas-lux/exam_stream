# Application Smart Meters Streaming

Cette application utilise Spark et Kafka pour analyser en temps réel les données de compteurs intelligents.

## Architecture

```
+----------------------+
| Smart Meters Dataset |
+----------------------+
          |
+---------------------+------------------------+
|                                              |
(1) Batch Processing                      (2) Streaming
|                                              |
+------------------------------+              +-------------------------------+
| Daily & Half-hourly datasets |              | Real-time emulation of stream |
+------------------------------+              |   via Kafka Producer (Scala)  |
|                                   +-------------------------------+
| (ex: daily_dataset.zip, etc.)                          |
v                                                        v
+-------------------------------+                +-------------------------------+
| Spark Scala Batch Processing  |<---------------| Kafka Topics (Real-Time Flow) |
| (Load, clean, aggregate data) |                +-------------------------------+
+-------------------------------+                                |
|                                      v
+----------------------------------+     +-------------------------------+
| Aggregated Views & Features CSV |<---->| Spark Structured Streaming    |
+----------------------------------+     | (clean, join, aggregate data) |
|                               +-------------------------------+
+----------------------+               |
|               v
+--------------+----------------+
| Output: Real-Time Dashboards  |
|        via Power BI           |
+-------------------------------+
```

## Prérequis

- Java 8+
- Scala 2.12+
- SBT (Scala Build Tool)
- Apache Spark 3.3.x
- Apache Kafka 3.3.x

## Installation

1. Cloner ce dépôt :
   ```
   git clone <repository-url>
   cd smart-meters-streaming
   ```

2. Télécharger le dataset Smart Meters et le décompresser dans le répertoire `data/` :
   - Les fichiers de consommation demi-horaires doivent être dans `data/halfhourly_dataset/`
   - Les fichiers de consommation journaliers doivent être dans `data/daily_dataset/`
   - Les autres fichiers (informations des ménages, données météo) doivent être à la racine du répertoire `data/`

3. Démarrer Kafka (si ce n'est pas déjà fait) :
   ```
   # Dans un terminal, démarrer ZooKeeper
   bin/zookeeper-server-start.sh config/zookeeper.properties
   
   # Dans un autre terminal, démarrer Kafka
   bin/kafka-server-start.sh config/server.properties
   ```

## Utilisation

L'application peut être exécutée dans trois modes différents :

1. **Mode producteur** : simule un flux de données en temps réel en envoyant les données historiques à Kafka
   ```
   ./run.sh producer
   ```

2. **Mode consommateur** : traite les flux de données provenant de Kafka avec Spark Structured Streaming
   ```
   ./run.sh consumer
   ```

3. **Mode combiné** : lance à la fois le producteur et le consommateur
   ```
   ./run.sh both
   ```

## Détails d'implémentation

### Producteur Kafka
Le producteur lit les fichiers CSV du dataset de compteurs intelligents et envoie les données ligne par ligne à un topic Kafka, en simulant un flux de données en temps réel avec un délai configurable.

### Consommateur Spark Streaming
Le consommateur utilise Spark Structured Streaming pour :
- Lire les données du topic Kafka
- Joindre les données avec les informations des ménages et les données météo
- Calculer des agrégations en temps réel (consommation moyenne par groupe ACORN, consommation totale par ménage)
- Détecter les anomalies (pics de consommation)
- Afficher les résultats en console (ou les écrire dans des sinks configurables pour une intégration avec Power BI) 