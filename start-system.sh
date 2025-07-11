#!/bin/bash

echo "🚀 Démarrage du système Smart Meter Streaming"
echo "=============================================="

# Fonction pour vérifier si un processus existe
check_process() {
    pgrep -f "$1" > /dev/null
}

# Fonction pour attendre qu'un service soit prêt
wait_for_service() {
    local service=$1
    local port=$2
    echo "⏳ Attente du démarrage de $service sur le port $port..."
    
    while ! nc -z localhost $port; do
        sleep 1
    done
    echo "✅ $service est prêt !"
}

# Étape 1: Démarrer Kafka et Zookeeper
echo "1️⃣ Démarrage de Kafka et Zookeeper..."
docker-compose up -d

# Attendre que Kafka soit prêt
wait_for_service "Kafka" 9092

# Étape 2: Compiler le projet Scala
echo "2️⃣ Compilation du projet Scala..."
sbt clean compile

# Étape 3: Installer les dépendances Python
echo "3️⃣ Installation des dépendances Python..."
pip install -r requirements.txt

# Étape 4: Démarrer le producer Kafka en arrière-plan
echo "4️⃣ Démarrage du producer Kafka..."
sbt "runMain kafka.SmartMeterKafkaProducer" &
PRODUCER_PID=$!

# Attendre un peu que le producer se lance
sleep 5

# Étape 5: Démarrer Streamlit
echo "5️⃣ Démarrage de l'application Streamlit..."
echo "🌐 L'application sera accessible sur http://localhost:8501"
streamlit run "src/main kafka.py" --server.port 8501

# Nettoyage à la fin
cleanup() {
    echo "🛑 Arrêt du système..."
    kill $PRODUCER_PID 2>/dev/null
    docker-compose down
    exit 0
}

trap cleanup SIGINT SIGTERM

# Garder le script actif
wait 