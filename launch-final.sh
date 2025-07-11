#!/bin/bash

# =============================================================================
# 🎉 Script Final - Smart Meters Streaming (VERSION FONCTIONNELLE)
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
    pkill -f spark-submit || true
    
    rm -rf /tmp/kafka-logs /tmp/zookeeper 2>/dev/null || true
    
    log_success "✅ Nettoyage terminé"
}

# Démarrer Kafka
start_kafka() {
    log_info "📡 Démarrage de Kafka..."
    
    # Configuration KRaft
    cat > /tmp/kraft-server.properties << EOF
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
    
    # Initialiser et démarrer
    kafka-storage format -t $(kafka-storage random-uuid) -c /tmp/kraft-server.properties --ignore-formatted 2>/dev/null || true
    nohup kafka-server-start /tmp/kraft-server.properties > /tmp/kafka.log 2>&1 &
    
    # Attendre
    for i in {1..30}; do
        if nc -z localhost $KAFKA_PORT 2>/dev/null; then
            log_success "✅ Kafka démarré"
            return 0
        fi
        sleep 1
    done
    
    log_error "❌ Impossible de démarrer Kafka"
    return 1
}

# Créer le topic
create_topic() {
    log_info "📝 Création du topic..."
    
    sleep 3
    kafka-topics --bootstrap-server localhost:$KAFKA_PORT --delete --topic $TOPIC_NAME 2>/dev/null || true
    
    kafka-topics --create \
        --bootstrap-server localhost:$KAFKA_PORT \
        --topic $TOPIC_NAME \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists
    
    log_success "✅ Topic créé"
}

# Compiler et assembler
compile_and_assemble() {
    log_info "🔨 Compilation et assemblage..."
    
    sbt clean compile assembly
    
    log_success "✅ JAR assemblé"
}

# Démarrer le producteur avec spark-submit
start_producer_spark() {
    log_info "🔄 Démarrage du producteur avec Spark..."
    
    # Trouver le JAR assemblé
    JAR_PATH=$(find target -name "*assembly*.jar" | head -1)
    
    if [ -z "$JAR_PATH" ]; then
        log_error "❌ JAR assemblé introuvable"
        return 1
    fi
    
    log_info "📦 Utilisation du JAR: $JAR_PATH"
    
    # Démarrer avec spark-submit
    nohup spark-submit \
        --class kafka.SmartMeterKafkaProducer \
        --master local[2] \
        --conf "spark.sql.adaptive.enabled=false" \
        --conf "spark.sql.adaptive.coalescePartitions.enabled=false" \
        --driver-java-options "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED" \
        "$JAR_PATH" > /tmp/producer.log 2>&1 &
    
    log_success "✅ Producteur démarré avec Spark"
}

# Démarrer Streamlit
start_streamlit() {
    log_info "📊 Démarrage de Streamlit..."
    
    nohup streamlit run "src/main kafka.py" \
        --server.port $STREAMLIT_PORT \
        --server.headless true \
        --server.address localhost > /tmp/streamlit.log 2>&1 &
    
    log_success "✅ Streamlit démarré"
}

# Vérifier le statut
check_status() {
    log_info "🔍 Vérification du statut..."
    
    if nc -z localhost $KAFKA_PORT 2>/dev/null; then
        log_success "✅ Kafka: EN LIGNE"
    else
        log_error "❌ Kafka: HORS LIGNE"
        return 1
    fi
    
    if nc -z localhost $STREAMLIT_PORT 2>/dev/null; then
        log_success "✅ Streamlit: EN LIGNE"
    else
        log_error "❌ Streamlit: HORS LIGNE"
        return 1
    fi
    
    # Vérifier les messages Kafka
    local message_count=$(kafka-console-consumer \
        --bootstrap-server localhost:$KAFKA_PORT \
        --topic $TOPIC_NAME \
        --from-beginning \
        --max-messages 1 \
        --timeout-ms 5000 2>/dev/null | wc -l)
    
    if [ "$message_count" -gt 0 ]; then
        log_success "✅ Producteur: ACTIF (messages détectés)"
    else
        log_warning "⚠️ Producteur: Aucun message détecté"
    fi
    
    return 0
}

# Arrêter les services
stop_services() {
    log_info "🛑 Arrêt des services..."
    
    pkill -f streamlit || true
    pkill -f spark-submit || true
    pkill -f kafka-server-start || true
    
    log_success "✅ Services arrêtés"
}

# Test complet
test_system() {
    log_info "🧪 Test du système..."
    
    # Test Kafka
    echo "Test-$(date +%s)" | kafka-console-producer --bootstrap-server localhost:$KAFKA_PORT --topic $TOPIC_NAME
    
    # Lire les messages
    kafka-console-consumer \
        --bootstrap-server localhost:$KAFKA_PORT \
        --topic $TOPIC_NAME \
        --from-beginning \
        --max-messages 3 \
        --timeout-ms 5000
    
    log_success "✅ Test terminé"
}

# Afficher les logs
show_logs() {
    log_info "📋 Logs des services:"
    
    echo "=== Logs Kafka ==="
    tail -10 /tmp/kafka.log 2>/dev/null || echo "Pas de logs"
    
    echo -e "\n=== Logs Producteur ==="
    tail -10 /tmp/producer.log 2>/dev/null || echo "Pas de logs"
    
    echo -e "\n=== Logs Streamlit ==="
    tail -10 /tmp/streamlit.log 2>/dev/null || echo "Pas de logs"
}

# Fonction principale
main() {
    case "${1:-start}" in
        start)
            log_info "🚀 Démarrage du système Smart Meters (VERSION FINALE)"
            
            cleanup
            
            if start_kafka && create_topic; then
                if compile_and_assemble; then
                    start_producer_spark
                    sleep 5
                    start_streamlit
                    sleep 5
                    
                    if check_status; then
                        log_success "🎉 SYSTÈME OPÉRATIONNEL !"
                        echo ""
                        echo "🌐 Dashboard Streamlit : http://localhost:$STREAMLIT_PORT"
                        echo "📡 Kafka Topic        : $TOPIC_NAME"
                        echo "🔍 Statut            : ./launch-final.sh status"
                        echo "🛑 Arrêt             : ./launch-final.sh stop"
                        echo ""
                    else
                        log_error "❌ Problème détecté"
                    fi
                else
                    log_error "❌ Échec de la compilation"
                fi
            else
                log_error "❌ Échec de Kafka"
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
            test_system
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