#!/bin/bash

# ====================================================================
# 🚀 SMART METERS - PRODUCTEUR KAFKA TEMPS RÉEL
# ====================================================================

set -e  # Arrêt en cas d'erreur

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Fonctions utilitaires
print_header() {
    echo -e "${BLUE}=====================================================================${NC}"
    echo -e "${BLUE}🚀 SMART METERS - PRODUCTEUR KAFKA TEMPS RÉEL${NC}"
    echo -e "${BLUE}=====================================================================${NC}"
}

print_step() {
    echo -e "${GREEN}[ÉTAPE]${NC} $1"
}

print_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERREUR]${NC} $1"
}

# Vérifier que SBT est installé
check_sbt() {
    if ! command -v sbt &> /dev/null; then
        print_error "SBT n'est pas installé. Veuillez l'installer pour continuer."
        exit 1
    fi
}

# Vérifier que Java est installé
check_java() {
    if ! command -v java &> /dev/null; then
        print_error "Java n'est pas installé. Veuillez l'installer pour continuer."
        exit 1
    fi
}

# Vérifier que Scala est installé
check_scala() {
    if ! command -v scala &> /dev/null; then
        print_error "Scala n'est pas installé. Veuillez l'installer pour continuer."
        exit 1
    fi
}

# Vérifier si Kafka est en cours d'exécution
check_kafka() {
    print_step "Vérification de Kafka..."
    
    if nc -z localhost 9092 2>/dev/null; then
        print_info "✅ Kafka est en cours d'exécution sur le port 9092"
    else
        print_info "⚠️ Kafka ne semble pas être en cours d'exécution sur le port 9092"
        print_info "🚀 Tentative de démarrage automatique de Kafka..."
        
        # Vérifier si Kafka est installé via Homebrew
        if command -v kafka-server-start &> /dev/null; then
            print_info "📦 Démarrage de Kafka via Homebrew..."
            
            # Démarrer ZooKeeper en arrière-plan
            nohup zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties > kafka_zookeeper.log 2>&1 &
            sleep 3
            
            # Démarrer Kafka en arrière-plan
            nohup kafka-server-start /opt/homebrew/etc/kafka/server.properties > kafka_server.log 2>&1 &
            sleep 8
            
            # Vérifier que Kafka est bien démarré
            if nc -z localhost 9092 2>/dev/null; then
                print_info "✅ Kafka démarré avec succès"
                
                # Créer le topic s'il n'existe pas
                kafka-topics --bootstrap-server localhost:9092 --create --topic smart-meters --partitions 3 --replication-factor 1 --if-not-exists > /dev/null 2>&1 || true
                print_info "✅ Topic 'smart-meters' créé/vérifié"
            else
                print_error "❌ Échec du démarrage automatique de Kafka"
                print_error "Veuillez démarrer Kafka manuellement avant d'exécuter cette application"
                exit 1
            fi
        else
            print_error "❌ Kafka n'est pas installé. Veuillez l'installer avec:"
            print_error "brew install kafka"
            exit 1
        fi
    fi
}

# Créer le répertoire de données s'il n'existe pas
create_data_dir() {
    print_step "Vérification des répertoires de données..."
    
    mkdir -p data
    mkdir -p data/halfhourly_dataset
    mkdir -p data/daily_dataset
    
    if [ ! -f "data/informations_households.csv" ]; then
        print_info "⚠️ Les fichiers de données semblent manquer dans data/"
        print_info "💡 Assurez-vous d'avoir le dataset Smart Meters dans le répertoire 'data/'"
    else
        print_info "✅ Fichiers de données trouvés"
    fi
}

# Compiler le projet
compile_project() {
    print_step "Compilation du projet Scala..."
    
    sbt compile
    
    if [ $? -ne 0 ]; then
        print_error "❌ Erreur de compilation. Abandon."
        exit 1
    fi
    
    print_info "✅ Compilation réussie"
}

# Créer le JAR avec assembly
create_jar() {
    print_step "Création du JAR avec assembly..."
    
    sbt assembly
    
    if [ $? -ne 0 ]; then
        print_error "❌ Erreur lors de la création du JAR. Abandon."
        exit 1
    fi
    
    print_info "✅ JAR créé avec succès"
}

# Exécuter le producteur avec spark-submit
run_producer() {
    print_step "Lancement du producteur Kafka avec Spark..."

    JAR_PATH="./target/scala-2.12/smart-meter-streaming-assembly-0.1.0-SNAPSHOT.jar"

    if [ ! -f "$JAR_PATH" ]; then
        print_error "❌ Le fichier JAR $JAR_PATH est introuvable"
        print_info "🔧 Tentative de création du JAR..."
        create_jar
        
        if [ ! -f "$JAR_PATH" ]; then
            print_error "❌ Impossible de créer le JAR"
            exit 1
        fi
    fi

    print_info "🚀 Démarrage du producteur Kafka en arrière-plan..."
    
    # Lancer le producteur en arrière-plan
    nohup spark-submit \
        --class kafka.SmartMeterKafkaProducer \
        --master local[*] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.kafka:kafka-clients:3.3.1 \
        "$JAR_PATH" > producer_streaming.log 2>&1 &
    
    PRODUCER_PID=$!
    echo $PRODUCER_PID > .producer_pid
    
    print_info "✅ Producteur Scala démarré (PID: $PRODUCER_PID)"
    print_info "⏳ Attente du démarrage et des premières données (15s)..."
    sleep 15
    
    print_info "🔍 Vérification des données..."
    # Vérifier que les données arrivent (sans timeout, compatible macOS)
    if command -v kafka-console-consumer &> /dev/null; then
        kafka-console-consumer --bootstrap-server localhost:9092 --topic smart-meters --from-beginning --max-messages 1 > /dev/null 2>&1 &
        CONSUMER_PID=$!
        sleep 3
        kill $CONSUMER_PID 2>/dev/null || true
        wait $CONSUMER_PID 2>/dev/null || true
        print_info "✅ Test de vérification effectué"
    fi
    
    # Vérifier que main_kafka.py existe
    if [ ! -f "main_kafka.py" ]; then
        print_error "❌ main_kafka.py introuvable"
        print_info "🔄 Le producteur continue en arrière-plan, vous pouvez lancer le dashboard manuellement"
        print_info "📋 Pour arrêter le producteur : kill $(cat .producer_pid)"
        print_info "🌐 Pour lancer le dashboard : streamlit run main_kafka.py"
        return
    fi
    
    print_info "🌐 Lancement du dashboard Streamlit..."
    print_info "📊 Le dashboard sera accessible sur : http://localhost:8501"
    print_info "🛑 Appuyez sur Ctrl+C pour arrêter le système complet"
    echo ""
    
    # Installer les dépendances si nécessaire
    if [ -f "requirements.txt" ]; then
        print_info "📦 Installation des dépendances..."
        pip install -r requirements.txt --quiet || true
    fi
    
    # Lancer le dashboard Streamlit en premier plan
    streamlit run main_kafka.py --server.headless true
}

# Fonction de nettoyage
cleanup() {
    print_info "🛑 Arrêt du système en cours..."
    
    # Arrêter le producteur Scala
    if [ -f ".producer_pid" ]; then
        PRODUCER_PID=$(cat .producer_pid)
        print_info "🔄 Arrêt du producteur Scala (PID: $PRODUCER_PID)..."
        kill $PRODUCER_PID 2>/dev/null || true
        rm -f .producer_pid
        print_info "✅ Producteur Scala arrêté"
    fi
    
    # Arrêter tous les processus Spark liés
    pkill -f "spark-submit" 2>/dev/null || true
    pkill -f "SmartMeterKafkaProducer" 2>/dev/null || true
    
    print_info "✅ Système arrêté proprement"
    exit 0
}

# Configuration du signal d'arrêt
trap cleanup SIGINT SIGTERM

# Fonction principale
main() {
    print_header
    
    check_sbt
    check_java
    check_scala
    check_kafka
    create_data_dir
    compile_project
    run_producer
}

# Appel de la fonction principale
main "$@"
