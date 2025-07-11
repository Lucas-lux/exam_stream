#!/bin/bash

# =============================================================================
# 🚀 Script de Lancement Simplifié - Smart Meters Streaming
# =============================================================================

set -e

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Variables
KAFKA_PORT=9092
TOPIC_NAME="smart-meters"
STREAMLIT_PORT=8501

# Nettoyer les processus existants
cleanup() {
    log_info "🧹 Nettoyage des processus existants..."
    
    pkill -f kafka-server-start || true
    pkill -f streamlit || true
    pkill -f sbt || true
    
    # Nettoyer les logs
    rm -rf /tmp/kafka-logs /tmp/zookeeper 2>/dev/null || true
    
    log_success "✅ Nettoyage terminé"
}

# Démarrer Kafka en mode KRaft (sans Zookeeper)
start_kafka() {
    log_info "📡 Démarrage de Kafka..."
    
    # Créer le fichier de configuration KRaft
    cat > /tmp/kraft-server.properties << EOF
# Configuration Kafka KRaft
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/tmp/kafka-logs
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
EOF
    
    # Initialiser le stockage KRaft
    kafka-storage format -t $(kafka-storage random-uuid) -c /tmp/kraft-server.properties --ignore-formatted 2>/dev/null || true
    
    # Démarrer Kafka
    nohup kafka-server-start /tmp/kraft-server.properties > /tmp/kafka.log 2>&1 &
    
    # Attendre que Kafka soit prêt
    for i in {1..30}; do
        if nc -z localhost $KAFKA_PORT 2>/dev/null; then
            log_success "✅ Kafka démarré sur le port $KAFKA_PORT"
            return 0
        fi
        sleep 1
    done
    
    log_error "❌ Impossible de démarrer Kafka"
    return 1
}

# Créer le topic Kafka
create_topic() {
    log_info "📝 Création du topic Kafka..."
    
    # Attendre un peu que Kafka soit complètement prêt
    sleep 3
    
    # Supprimer le topic s'il existe
    kafka-topics --bootstrap-server localhost:$KAFKA_PORT --delete --topic $TOPIC_NAME 2>/dev/null || true
    
    # Créer le topic
    kafka-topics --create \
        --bootstrap-server localhost:$KAFKA_PORT \
        --topic $TOPIC_NAME \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists
    
    log_success "✅ Topic '$TOPIC_NAME' créé"
}

# Compiler le projet
compile_project() {
    log_info "🔨 Compilation du projet..."
    
    sbt clean compile
    
    log_success "✅ Compilation terminée"
}

# Démarrer le producteur
start_producer() {
    log_info "🔄 Démarrage du producteur Kafka..."
    
    nohup sbt "runMain kafka.SmartMeterKafkaProducer" > /tmp/producer.log 2>&1 &
    
    log_success "✅ Producteur démarré"
}

# Démarrer Streamlit
start_streamlit() {
    log_info "📊 Démarrage de Streamlit..."
    
    nohup streamlit run "src/main kafka.py" --server.port $STREAMLIT_PORT --server.headless true > /tmp/streamlit.log 2>&1 &
    
    log_success "✅ Streamlit démarré"
    log_info "🌐 Dashboard accessible sur: http://localhost:$STREAMLIT_PORT"
}

# Vérifier le statut
check_status() {
    log_info "🔍 Vérification du statut..."
    
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

# Arrêter les services
stop_services() {
    log_info "🛑 Arrêt des services..."
    
    pkill -f streamlit || true
    pkill -f sbt || true
    pkill -f kafka-server-start || true
    
    log_success "✅ Services arrêtés"
}

# Afficher les logs
show_logs() {
    log_info "📋 Logs des services:"
    
    echo "=== Logs Kafka ==="
    tail -20 /tmp/kafka.log 2>/dev/null || echo "Pas de logs Kafka"
    
    echo -e "\n=== Logs Producteur ==="
    tail -20 /tmp/producer.log 2>/dev/null || echo "Pas de logs Producteur"
    
    echo -e "\n=== Logs Streamlit ==="
    tail -20 /tmp/streamlit.log 2>/dev/null || echo "Pas de logs Streamlit"
}

# Test simple de production/consommation
test_kafka() {
    log_info "🧪 Test de Kafka..."
    
    # Envoyer un message de test
    echo "Test message $(date)" | kafka-console-producer --bootstrap-server localhost:$KAFKA_PORT --topic $TOPIC_NAME
    
    # Lire le message
    timeout 5 kafka-console-consumer --bootstrap-server localhost:$KAFKA_PORT --topic $TOPIC_NAME --from-beginning --max-messages 1
    
    log_success "✅ Test Kafka réussi"
}

# Fonction principale
main() {
    case "${1:-start}" in
        start)
            log_info "🚀 Démarrage du système Smart Meters (mode simplifié)"
            
            cleanup
            
            if start_kafka; then
                create_topic
                compile_project
                start_producer
                sleep 5
                start_streamlit
                sleep 2
                check_status
                
                log_success "🎉 Système démarré avec succès!"
                log_info "🌐 Dashboard: http://localhost:$STREAMLIT_PORT"
                log_info "📊 Utilisez 'curl localhost:$STREAMLIT_PORT' pour tester"
            else
                log_error "❌ Échec du démarrage de Kafka"
                exit 1
            fi
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
        
        test)
            test_kafka
            ;;
        
        *)
            echo "Usage: $0 {start|stop|status|logs|test}"
            exit 1
            ;;
    esac
}

# Gestion des interruptions
trap 'log_warning "Interruption..."; stop_services; exit 0' INT TERM

# Exécution
main "$@" 