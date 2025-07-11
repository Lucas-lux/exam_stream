#!/bin/bash

# =============================================================================
# 🚀 Script de Lancement Smart Meters Streaming - Solution Complète
# =============================================================================

set -e  # Arrêter en cas d'erreur

# Couleurs pour les logs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Fonction de log coloré
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Variables
KAFKA_HOME="/opt/homebrew/etc/kafka"
KAFKA_BIN="/opt/homebrew/bin"
ZOOKEEPER_PORT=2181
KAFKA_PORT=9092
TOPIC_NAME="smart-meters"
STREAMLIT_PORT=8501

# Vérifier les prérequis
check_prerequisites() {
    log_info "🔍 Vérification des prérequis..."
    
    # Vérifier Java
    if ! command -v java &> /dev/null; then
        log_error "Java n'est pas installé"
        exit 1
    fi
    
    # Vérifier Scala
    if ! command -v scala &> /dev/null; then
        log_error "Scala n'est pas installé"
        exit 1
    fi
    
    # Vérifier SBT
    if ! command -v sbt &> /dev/null; then
        log_error "SBT n'est pas installé"
        exit 1
    fi
    
    # Vérifier Kafka
    if ! command -v kafka-server-start &> /dev/null; then
        log_error "Kafka n'est pas installé ou pas dans le PATH"
        exit 1
    fi
    
    # Vérifier Python et Streamlit
    if ! command -v streamlit &> /dev/null; then
        log_error "Streamlit n'est pas installé"
        exit 1
    fi
    
    # Vérifier les données
    if [ ! -d "data/halfourlydataset" ] || [ -z "$(ls -A data/halfourlydataset)" ]; then
        log_error "Dossier data/halfourlydataset vide ou inexistant"
        exit 1
    fi
    
    log_success "✅ Tous les prérequis sont satisfaits"
}

# Nettoyer les processus existants
cleanup() {
    log_info "🧹 Nettoyage des processus existants..."
    
    # Arrêter les processus Kafka/Zookeeper
    pkill -f kafka-server-start || true
    pkill -f zookeeper-server-start || true
    pkill -f streamlit || true
    pkill -f sbt || true
    
    # Nettoyer les logs
    rm -rf /tmp/kafka-logs /tmp/zookeeper 2>/dev/null || true
    
    log_success "✅ Nettoyage terminé"
}

# Démarrer Zookeeper
start_zookeeper() {
    log_info "🐘 Démarrage de Zookeeper..."
    
    nohup zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties > /tmp/zookeeper.log 2>&1 &
    
    # Attendre que Zookeeper soit prêt
    for i in {1..30}; do
        if nc -z localhost $ZOOKEEPER_PORT 2>/dev/null; then
            log_success "✅ Zookeeper démarré sur le port $ZOOKEEPER_PORT"
            return 0
        fi
        sleep 1
    done
    
    log_error "❌ Impossible de démarrer Zookeeper"
    exit 1
}

# Démarrer Kafka
start_kafka() {
    log_info "📡 Démarrage de Kafka..."
    
    nohup kafka-server-start /opt/homebrew/etc/kafka/server.properties > /tmp/kafka.log 2>&1 &
    
    # Attendre que Kafka soit prêt
    for i in {1..30}; do
        if nc -z localhost $KAFKA_PORT 2>/dev/null; then
            log_success "✅ Kafka démarré sur le port $KAFKA_PORT"
            return 0
        fi
        sleep 1
    done
    
    log_error "❌ Impossible de démarrer Kafka"
    exit 1
}

# Créer le topic Kafka
create_topic() {
    log_info "📝 Création du topic Kafka..."
    
    # Supprimer le topic s'il existe
    kafka-topics --bootstrap-server localhost:$KAFKA_PORT --delete --topic $TOPIC_NAME 2>/dev/null || true
    
    # Créer le topic
    kafka-topics --create \
        --bootstrap-server localhost:$KAFKA_PORT \
        --topic $TOPIC_NAME \
        --partitions 3 \
        --replication-factor 1
    
    log_success "✅ Topic '$TOPIC_NAME' créé"
}

# Compiler le projet Scala
compile_project() {
    log_info "🔨 Compilation du projet Scala..."
    
    sbt clean compile
    
    log_success "✅ Compilation terminée"
}

# Démarrer le producteur Kafka
start_producer() {
    log_info "🔄 Démarrage du producteur Kafka..."
    
    nohup sbt "runMain kafka.SmartMeterKafkaProducer" > /tmp/producer.log 2>&1 &
    PRODUCER_PID=$!
    
    log_success "✅ Producteur démarré (PID: $PRODUCER_PID)"
}

# Démarrer Streamlit
start_streamlit() {
    log_info "📊 Démarrage de Streamlit..."
    
    nohup streamlit run "src/main kafka.py" --server.port $STREAMLIT_PORT --server.headless true > /tmp/streamlit.log 2>&1 &
    STREAMLIT_PID=$!
    
    log_success "✅ Streamlit démarré (PID: $STREAMLIT_PID)"
    log_info "🌐 Dashboard accessible sur: http://localhost:$STREAMLIT_PORT"
}

# Vérifier le statut des services
check_status() {
    log_info "🔍 Vérification du statut des services..."
    
    # Zookeeper
    if nc -z localhost $ZOOKEEPER_PORT 2>/dev/null; then
        log_success "✅ Zookeeper: EN LIGNE"
    else
        log_error "❌ Zookeeper: HORS LIGNE"
    fi
    
    # Kafka
    if nc -z localhost $KAFKA_PORT 2>/dev/null; then
        log_success "✅ Kafka: EN LIGNE"
    else
        log_error "❌ Kafka: HORS LIGNE"
    fi
    
    # Streamlit
    if nc -z localhost $STREAMLIT_PORT 2>/dev/null; then
        log_success "✅ Streamlit: EN LIGNE"
    else
        log_error "❌ Streamlit: HORS LIGNE"
    fi
}

# Afficher les logs
show_logs() {
    log_info "📋 Affichage des logs..."
    
    echo "=== Logs Zookeeper ==="
    tail -20 /tmp/zookeeper.log 2>/dev/null || echo "Pas de logs Zookeeper"
    
    echo -e "\n=== Logs Kafka ==="
    tail -20 /tmp/kafka.log 2>/dev/null || echo "Pas de logs Kafka"
    
    echo -e "\n=== Logs Producteur ==="
    tail -20 /tmp/producer.log 2>/dev/null || echo "Pas de logs Producteur"
    
    echo -e "\n=== Logs Streamlit ==="
    tail -20 /tmp/streamlit.log 2>/dev/null || echo "Pas de logs Streamlit"
}

# Arrêter tous les services
stop_services() {
    log_info "🛑 Arrêt des services..."
    
    pkill -f streamlit || true
    pkill -f sbt || true
    pkill -f kafka-server-start || true
    pkill -f zookeeper-server-start || true
    
    log_success "✅ Tous les services arrêtés"
}

# Fonction principale
main() {
    case "${1:-start}" in
        start)
            log_info "🚀 Démarrage du système Smart Meters"
            check_prerequisites
            cleanup
            start_zookeeper
            start_kafka
            sleep 2
            create_topic
            compile_project
            start_producer
            sleep 3
            start_streamlit
            sleep 2
            check_status
            
            log_success "🎉 Système Smart Meters démarré avec succès!"
            log_info "🌐 Dashboard: http://localhost:$STREAMLIT_PORT"
            log_info "📡 Kafka: localhost:$KAFKA_PORT"
            log_info "🔍 Utilisez '$0 status' pour vérifier l'état"
            log_info "🛑 Utilisez '$0 stop' pour arrêter"
            ;;
        
        stop)
            stop_services
            ;;
        
        status)
            check_status
            ;;
        
        logs)
            show_logs
            ;;
        
        restart)
            stop_services
            sleep 2
            $0 start
            ;;
        
        *)
            echo "Usage: $0 {start|stop|status|logs|restart}"
            echo "  start   - Démarrer tous les services"
            echo "  stop    - Arrêter tous les services"
            echo "  status  - Vérifier l'état des services"
            echo "  logs    - Afficher les logs"
            echo "  restart - Redémarrer tous les services"
            exit 1
            ;;
    esac
}

# Gestion des signaux pour nettoyage
trap 'log_warning "Interruption détectée, nettoyage..."; stop_services; exit 0' INT TERM

# Exécution
main "$@" 