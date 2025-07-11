# 🚀 Smart Meters - Système de Streaming Automatique

Système de streaming temps réel pour données de compteurs intelligents avec **Apache Kafka**, **Spark Structured Streaming** et **Python**.

## ⚡ **Démarrage Ultra-Rapide**

```bash
# Une seule commande pour tout lancer !
./run-producer.sh
```

C'est tout ! Le système démarre automatiquement :
- ✅ **Kafka** (avec ZooKeeper)
- ✅ **Producteur Scala/Spark** 
- ✅ **Monitoring Python temps réel**

## 📊 **Ce que Vous Obtiendrez**

Le système affiche en temps réel dans votre terminal :
```
🚀 SMART METERS - KAFKA MONITOR
📡 Kafka: localhost:9092
📊 Topic: smart-meters

🕒 15:24:24 | 📊 Message #1234 | 🏠 MAC000154 | ⚡ 0.567 kWh | 📅 2013-03-26T12:00:00 | ⏱️ 45s
🕒 15:24:24 | 📊 Message #1235 | 🏠 MAC003422 | ⚡ 0.032 kWh | 📅 2013-06-17T22:00:00 | ⏱️ 45s
📈 STATS: 1240 messages en 45s (27.6 msg/s)
```

## 🏗️ **Architecture**

```
📂 data/halfhourly_dataset/ → 🚀 Producteur Scala → 📡 Kafka → 📺 Monitoring Python
```

## 📋 **Prérequis**

Sur **macOS** avec Homebrew :
```bash
# Installer les outils nécessaires
brew install openjdk@11 scala sbt apache-spark kafka python

# Installer les dépendances Python
pip install -r requirements.txt
```

## 📁 **Structure du Projet**

```
exam_stream/
├── run-producer.sh              # 🚀 Script principal (tout-en-un)
├── main_kafka.py               # 📺 Monitoring temps réel
├── data/halfhourly_dataset/    # 📊 Données Smart Meters
├── src/main/scala/             # ⚡ Code Scala/Spark
│   ├── kafka/SmartMeterKafkaProducer.scala
│   └── models/SmartMeterModels.scala
├── build.sbt                   # 🔧 Configuration SBT
└── README.md                   # 📖 Ce fichier
```

## 🎯 **Fonctionnalités**

- **🔥 Streaming temps réel** : 5000+ messages/seconde
- **📊 Données réalistes** : Vrais compteurs britanniques (2012-2014)
- **🎨 Affichage coloré** : Interface terminal avec emojis
- **📈 Statistiques live** : Débit, compteurs, durée
- **🛡️ Gestion d'erreurs** : Redémarrage automatique
- **🧹 Arrêt propre** : Ctrl+C nettoie tout

## ⚙️ **Comment Ça Marche**

1. **Kafka** se lance automatiquement (si pas déjà démarré)
2. **Topic `smart-meters`** est créé
3. **Producteur Scala** lit les CSV et stream vers Kafka
4. **Monitoring Python** affiche les données en temps réel

## 🛠️ **Dépannage**

### Kafka ne démarre pas ?
```bash
# Vérifier les processus
brew services list | grep kafka

# Redémarrer si nécessaire  
brew services restart kafka
brew services restart zookeeper
```

### Erreur de compilation ?
```bash
# Nettoyer et recompiler
sbt clean compile
```

### Aucune donnée ?
```bash
# Vérifier manuellement
kafka-console-consumer --bootstrap-server localhost:9092 --topic smart-meters
```

## 🚦 **Commandes Utiles**

```bash
# Démarrer le système
./run-producer.sh

# Arrêter proprement (dans le terminal qui affiche les données)
Ctrl+C

# Vérifier les processus
ps aux | grep -E "(kafka|spark|zookeeper)"

# Nettoyer les processus (si besoin)
pkill -f "kafka"
pkill -f "spark-submit"
```

## 📈 **Performances Attendues**

- **Débit** : 2000-6000 messages/seconde
- **Latence** : < 50ms
- **Mémoire** : ~500MB (Spark + Kafka)
- **Variété** : 10+ compteurs différents
- **Données** : Consommations 0.0-2.0 kWh

## 🎓 **Contexte Éducatif**

Projet développé pour le cours **4IABD2 - Spark Streaming**. 

**Objectifs pédagogiques :**
- Maîtriser Apache Kafka
- Implémenter Spark Structured Streaming  
- Créer un pipeline temps réel complet
- Utiliser Scala et Python ensemble

## 💡 **Astuce Pro**

Pour une démonstration impressionnante, lancez deux terminaux :
1. `./run-producer.sh` (affichage des données)
2. `kafka-console-consumer --bootstrap-server localhost:9092 --topic smart-meters` (données brutes)

---

**Made with ❤️ and ☕ for Real-Time Data Processing** 