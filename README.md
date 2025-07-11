# 🌟 Smart Meter Streaming Dashboard

Système de streaming temps réel pour analyser les données de compteurs intelligents avec Apache Kafka, Spark et Streamlit.

## 🎯 Architecture du Système

```
📊 Données CSV → 🔄 Producer Kafka → 📡 Topic Kafka → 🐍 Consumer Streamlit → 📈 Dashboard
```

## 🚀 Lancement Automatique (Recommandé)

### Option 1 : Script automatique
```bash
./start-system.sh
```

Le script va automatiquement :
- ✅ Démarrer Kafka et Zookeeper
- ✅ Compiler le projet Scala
- ✅ Installer les dépendances Python
- ✅ Lancer le producer Kafka
- ✅ Démarrer l'application Streamlit

### Option 2 : Lancement manuel étape par étape

#### Étape 1 : Démarrer Kafka
```bash
docker-compose up -d
```

#### Étape 2 : Compiler le projet
```bash
sbt clean compile
```

#### Étape 3 : Installer les dépendances Python
```bash
pip install -r requirements.txt
```

#### Étape 4 : Démarrer le producer Kafka
```bash
sbt "runMain kafka.SmartMeterKafkaProducer"
```

#### Étape 5 : Dans un nouveau terminal, démarrer Streamlit
```bash
streamlit run "src/main kafka.py"
```

## 🔧 Configuration

### Kafka
- **Bootstrap servers** : `localhost:9092`
- **Topic** : `smart-meters`
- **Intervalle du producer** : 1 seconde

### Streamlit
- **Port** : 8501
- **URL** : http://localhost:8501

## 📂 Structure des Données

### Données d'entrée
- **Dossier** : `data/halfourlydataset/`
- **Format** : CSV avec colonnes :
  - `LCLid` : Identifiant du compteur
  - `tstp` : Timestamp
  - `energy(kWh/hh)` : Consommation énergétique

### Données de sortie Kafka
```json
{
  "LCLid": "MAC000002",
  "tstp": "2013-01-01 00:30:00",
  "energy(kWh/hh)": 0.748
}
```

## 🎨 Fonctionnalités du Dashboard

### 📊 Métriques temps réel
- Consommation totale
- Nombre de compteurs actifs
- Consommation moyenne
- Détection d'anomalies

### 📈 Visualisations
- Graphiques de consommation en temps réel
- Distribution par compteur
- Détection d'anomalies
- Indicateurs de performance

### 🔄 Mise à jour automatique
- Actualisation toutes les 5 secondes
- Indicateur de connexion temps réel
- Cache intelligent des données

## 🛠️ Dépendances

### Scala/Spark
- Spark 3.3.2
- Kafka 3.3.0
- Scala 2.12.17

### Python
- streamlit 1.28.1
- pandas 2.1.1
- plotly 5.17.0
- kafka-python 2.0.2

## 🐳 Docker Services

### Zookeeper
- Port : 2181

### Kafka
- Port : 9092
- Réplication : 1

## 🚦 Vérification du Système

### 1. Vérifier Kafka
```bash
docker-compose ps
```

### 2. Vérifier les topics Kafka
```bash
docker exec -it $(docker-compose ps -q kafka) kafka-topics --list --bootstrap-server localhost:9092
```

### 3. Vérifier les messages Kafka
```bash
docker exec -it $(docker-compose ps -q kafka) kafka-console-consumer --bootstrap-server localhost:9092 --topic smart-meters --from-beginning
```

## 🆘 Résolution des Problèmes

### Problème : Kafka ne démarre pas
**Solution** : Vérifier que Docker est actif et que les ports 2181 et 9092 sont libres

### Problème : Producer ne trouve pas les données
**Solution** : Vérifier que le dossier `data/halfourlydataset/` contient les fichiers CSV

### Problème : Streamlit ne reçoit pas de données
**Solution** : 
1. Vérifier que le producer Kafka fonctionne
2. Vérifier la connexion Kafka dans l'interface Streamlit
3. Redémarrer le système complet

### Problème : Erreur de compilation Scala
**Solution** : Nettoyer et recompiler
```bash
sbt clean compile
```

## 🔧 Commandes Utiles

### Arrêter le système
```bash
# Arrêter Docker
docker-compose down

# Arrêter tous les processus SBT
pkill -f sbt

# Arrêter Streamlit
pkill -f streamlit
```

### Nettoyer les checkpoints
```bash
rm -rf data/checkpoint/*
```

### Voir les logs Kafka
```bash
docker-compose logs kafka
```

## 🎯 Prochaines Étapes

1. **Démarrer le système** : `./start-system.sh`
2. **Ouvrir le dashboard** : http://localhost:8501
3. **Surveiller les données** en temps réel
4. **Explorer les métriques** et visualisations

## 📞 Support

En cas de problème, vérifiez :
- ✅ Docker est actif
- ✅ Les ports 2181, 9092 et 8501 sont libres
- ✅ Java 11+ est installé
- ✅ Python 3.8+ est installé
- ✅ Les données CSV sont présentes

---

🎉 **Prêt à analyser vos données de compteurs intelligents en temps réel !** 