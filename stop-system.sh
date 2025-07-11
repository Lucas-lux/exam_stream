#!/bin/bash

echo "🛑 Arrêt du système Smart Meter Streaming"
echo "========================================="

# Arrêter Streamlit
echo "📱 Arrêt de Streamlit..."
pkill -f streamlit || echo "Streamlit n'était pas actif"

# Arrêter le producer Kafka (SBT)
echo "🔄 Arrêt du producer Kafka..."
pkill -f "SmartMeterKafkaProducer" || echo "Producer Kafka n'était pas actif"
pkill -f sbt || echo "SBT n'était pas actif"

# Arrêter Docker (Kafka + Zookeeper)
echo "🐳 Arrêt de Docker (Kafka + Zookeeper)..."
docker-compose down

# Nettoyer les checkpoints si nécessaire
echo "🧹 Nettoyage des checkpoints..."
rm -rf data/checkpoint/* 2>/dev/null || echo "Pas de checkpoints à nettoyer"

echo "✅ Système arrêté avec succès !"
echo "💡 Pour redémarrer, utilisez: ./start-system.sh" 