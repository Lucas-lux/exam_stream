# 🚀 Guide d'Utilisation - Smart Meters Streaming

## 📋 Prérequis

Assurez-vous d'avoir installé :
- ✅ Java 11+
- ✅ Scala 2.12+
- ✅ SBT (Scala Build Tool)
- ✅ Kafka (via Homebrew)
- ✅ Python 3.8+
- ✅ Streamlit

## 🎯 Démarrage Rapide

### **Option 1 : Démarrage automatique (Recommandé)**

```bash
# Démarrer tout le système
./launch-smart-meters.sh start

# Accéder au dashboard
open http://localhost:8501
```

### **Option 2 : Commandes séparées**

```bash
# Vérifier l'état des services
./launch-smart-meters.sh status

# Voir les logs
./launch-smart-meters.sh logs

# Arrêter le système
./launch-smart-meters.sh stop

# Redémarrer le système
./launch-smart-meters.sh restart
```

## 🏗️ Architecture du Système

```
📊 Données CSV 
    ↓
🔄 Producer Kafka (Scala)
    ↓
📡 Topic: smart-meters
    ↓
🐍 Dashboard Streamlit
```

## 🌐 Accès aux Services

- **Dashboard Streamlit** : http://localhost:8501
- **Kafka** : localhost:9092
- **Zookeeper** : localhost:2181

## 📊 Fonctionnalités du Dashboard

### **Onglet Temps Réel**
- Métriques en temps réel
- Graphiques de consommation
- Indicateurs de statut

### **Onglet Consommation**
- Analyse des tendances
- Distribution des consommations
- Top consommateurs

### **Onglet Anomalies**
- Détection automatique
- Seuils configurables
- Alertes visuelles

## 🔧 Résolution des Problèmes

### **Problème : Kafka ne démarre pas**
```bash
# Nettoyer et redémarrer
./launch-smart-meters.sh stop
./launch-smart-meters.sh start
```

### **Problème : Pas de données dans Streamlit**
```bash
# Vérifier les logs
./launch-smart-meters.sh logs

# Vérifier le statut
./launch-smart-meters.sh status
```

### **Problème : Erreur de compilation**
```bash
# Nettoyer et recompiler
sbt clean compile
```

## 📈 Données Traitées

Le système traite les données de compteurs intelligents :
- **Format** : CSV avec colonnes LCLid, tstp, energy(kWh/hh)
- **Source** : `data/halfourlydataset/`
- **Fréquence** : Toutes les 2 secondes
- **Volume** : ~100 messages par batch

## 🎛️ Configuration

### **Producer Kafka**
- Intervalle : 2 secondes
- Batch size : 16 KB
- Timeout : 30 secondes

### **Streamlit**
- Port : 8501
- Actualisation : 10 secondes
- Cache : 10 secondes

## 🚦 Commandes Utiles

```bash
# Voir les topics Kafka
kafka-topics --bootstrap-server localhost:9092 --list

# Voir les messages en temps réel
kafka-console-consumer --bootstrap-server localhost:9092 --topic smart-meters

# Vérifier les ports
lsof -i :8501  # Streamlit
lsof -i :9092  # Kafka
lsof -i :2181  # Zookeeper
```

## 🎯 Prochaines Étapes

1. **Démarrer le système** : `./launch-smart-meters.sh start`
2. **Ouvrir le dashboard** : http://localhost:8501
3. **Explorer les données** en temps réel
4. **Analyser les métriques** et anomalies

---

🎉 **Votre système de streaming est maintenant opérationnel !** 