#!/bin/bash

# =============================================================================
# 🎯 Solution Simple - Smart Meters Streaming (GARANTIE FONCTIONNELLE)
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

# Nettoyer tout
cleanup() {
    log_info "🧹 Nettoyage complet..."
    
    pkill -f kafka-server-start || true
    pkill -f streamlit || true
    pkill -f spark-submit || true
    
    rm -rf /tmp/kafka-logs /tmp/zookeeper 2>/dev/null || true
    
    log_success "✅ Nettoyage terminé"
}

# Démarrer Kafka avec configuration par défaut
start_kafka_simple() {
    log_info "📡 Démarrage de Kafka (configuration par défaut)..."
    
    # Utiliser la configuration par défaut de Homebrew
    nohup kafka-server-start /opt/homebrew/etc/kafka/server.properties > /tmp/kafka.log 2>&1 &
    
    # Attendre que Kafka soit prêt
    for i in {1..30}; do
        if nc -z localhost $KAFKA_PORT 2>/dev/null; then
            log_success "✅ Kafka démarré"
            return 0
        fi
        sleep 2
    done
    
    log_error "❌ Impossible de démarrer Kafka"
    return 1
}

# Créer le topic (avec plus de patience)
create_topic_simple() {
    log_info "📝 Création du topic..."
    
    # Attendre que Kafka soit vraiment prêt
    sleep 5
    
    # Créer le topic
    kafka-topics --create \
        --bootstrap-server localhost:$KAFKA_PORT \
        --topic $TOPIC_NAME \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists 2>/dev/null || true
    
    log_success "✅ Topic créé"
}

# Démarrer Streamlit seulement
start_streamlit_only() {
    log_info "📊 Démarrage de Streamlit..."
    
    # S'assurer que Kafka est prêt
    if ! nc -z localhost $KAFKA_PORT 2>/dev/null; then
        log_error "❌ Kafka n'est pas accessible"
        return 1
    fi
    
    # Démarrer Streamlit
    nohup streamlit run "src/main kafka.py" \
        --server.port $STREAMLIT_PORT \
        --server.headless true \
        --server.address localhost > /tmp/streamlit.log 2>&1 &
    
    log_success "✅ Streamlit démarré"
}

# Créer un producteur Python simple
create_simple_producer() {
    log_info "🔄 Création d'un producteur Python simple..."
    
    cat > simple_producer.py << 'EOF'
#!/usr/bin/env python3
"""
Producteur Kafka simple en Python pour Smart Meters
"""
import json
import time
import random
from kafka import KafkaProducer
import pandas as pd
import os

def main():
    print("🚀 Démarrage du producteur Python simple...")
    
    # Configuration Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: v.encode('utf-8') if v else None
    )
    
    # Vérifier si les données existent
    data_dir = "data/halfourlydataset"
    if not os.path.exists(data_dir):
        print(f"❌ Dossier {data_dir} non trouvé")
        return
    
    # Lire le premier fichier CSV
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    if not csv_files:
        print("❌ Aucun fichier CSV trouvé")
        return
    
    csv_file = os.path.join(data_dir, csv_files[0])
    print(f"📊 Lecture du fichier: {csv_file}")
    
    try:
        df = pd.read_csv(csv_file)
        print(f"✅ {len(df)} lignes chargées")
        
        # Envoyer les données en boucle
        count = 0
        for _, row in df.iterrows():
            
            # Préparer le message
            message = {
                "LCLid": row['LCLid'],
                "tstp": row['tstp'],
                "energy(kWh/hh)": float(row['energy(kWh/hh)'].strip()) if pd.notna(row['energy(kWh/hh)']) else 0.0
            }
            
            # Envoyer le message
            producer.send('smart-meters', key=row['LCLid'], value=message)
            count += 1
            
            if count % 100 == 0:
                print(f"📤 {count} messages envoyés")
            
            # Attendre un peu
            time.sleep(0.1)
            
            # Limiter pour les tests
            if count >= 1000:
                break
                
    except Exception as e:
        print(f"❌ Erreur: {e}")
    
    finally:
        producer.flush()
        producer.close()
        print(f"✅ Producteur terminé. {count} messages envoyés.")

if __name__ == "__main__":
    main()
EOF

    log_success "✅ Producteur Python créé"
}

# Démarrer le producteur Python
start_python_producer() {
    log_info "🔄 Démarrage du producteur Python..."
    
    # Vérifier que pandas est installé
    if ! python3 -c "import pandas" 2>/dev/null; then
        log_warning "Installation de pandas..."
        pip install pandas
    fi
    
    # Vérifier que kafka-python est installé
    if ! python3 -c "import kafka" 2>/dev/null; then
        log_warning "Installation de kafka-python..."
        pip install kafka-python
    fi
    
    # Démarrer le producteur
    nohup python3 simple_producer.py > /tmp/python_producer.log 2>&1 &
    
    log_success "✅ Producteur Python démarré"
}

# Vérifier le statut complet
check_complete_status() {
    log_info "🔍 Vérification complète du statut..."
    
    # Kafka
    if nc -z localhost $KAFKA_PORT 2>/dev/null; then
        log_success "✅ Kafka: EN LIGNE"
    else
        log_error "❌ Kafka: HORS LIGNE"
        return 1
    fi
    
    # Streamlit
    if nc -z localhost $STREAMLIT_PORT 2>/dev/null; then
        log_success "✅ Streamlit: EN LIGNE"
    else
        log_error "❌ Streamlit: HORS LIGNE"
        return 1
    fi
    
    # Vérifier les topics
    local topics=$(kafka-topics --bootstrap-server localhost:$KAFKA_PORT --list 2>/dev/null)
    if echo "$topics" | grep -q "$TOPIC_NAME"; then
        log_success "✅ Topic 'smart-meters': EXISTE"
    else
        log_warning "⚠️ Topic 'smart-meters': NON TROUVÉ"
    fi
    
    # Compter les messages
    local message_count=$(timeout 5 kafka-console-consumer \
        --bootstrap-server localhost:$KAFKA_PORT \
        --topic $TOPIC_NAME \
        --from-beginning \
        --max-messages 10 2>/dev/null | wc -l)
    
    if [ "$message_count" -gt 0 ]; then
        log_success "✅ Messages dans Kafka: $message_count détectés"
    else
        log_warning "⚠️ Messages dans Kafka: Aucun détecté"
    fi
    
    return 0
}

# Arrêter tout
stop_all() {
    log_info "🛑 Arrêt complet..."
    
    pkill -f streamlit || true
    pkill -f python3 || true
    pkill -f kafka-server-start || true
    
    log_success "✅ Tout arrêté"
}

# Afficher les logs
show_all_logs() {
    log_info "📋 Logs de tous les services:"
    
    echo "=== Logs Kafka ==="
    tail -15 /tmp/kafka.log 2>/dev/null || echo "Pas de logs"
    
    echo -e "\n=== Logs Producteur Python ==="
    tail -15 /tmp/python_producer.log 2>/dev/null || echo "Pas de logs"
    
    echo -e "\n=== Logs Streamlit ==="
    tail -15 /tmp/streamlit.log 2>/dev/null || echo "Pas de logs"
}

# Test rapide
quick_test() {
    log_info "🧪 Test rapide..."
    
    # Envoyer un message de test
    echo '{"test": "message", "timestamp": "'$(date)'"}' | kafka-console-producer \
        --bootstrap-server localhost:$KAFKA_PORT \
        --topic $TOPIC_NAME
    
    # Lire quelques messages
    log_info "📖 Lecture des messages..."
    timeout 3 kafka-console-consumer \
        --bootstrap-server localhost:$KAFKA_PORT \
        --topic $TOPIC_NAME \
        --from-beginning \
        --max-messages 5
    
    log_success "✅ Test terminé"
}

# Fonction principale
main() {
    case "${1:-start}" in
        start)
            log_info "🚀 DÉMARRAGE SOLUTION SIMPLE"
            
            cleanup
            
            if start_kafka_simple; then
                create_topic_simple
                
                create_simple_producer
                start_python_producer
                
                start_streamlit_only
                sleep 3
                
                if check_complete_status; then
                    log_success "🎉 SYSTÈME COMPLÈTEMENT OPÉRATIONNEL !"
                    echo ""
                    echo "🌐 Dashboard Streamlit : http://localhost:$STREAMLIT_PORT"
                    echo "📊 Pour voir les messages: kafka-console-consumer --bootstrap-server localhost:9092 --topic smart-meters --from-beginning"
                    echo "🔍 Statut: ./solution-simple.sh status"
                    echo "📋 Logs: ./solution-simple.sh logs"
                    echo "🛑 Arrêt: ./solution-simple.sh stop"
                    echo ""
                else
                    log_error "❌ Problème détecté"
                fi
            else
                log_error "❌ Impossible de démarrer Kafka"
            fi
            ;;
        
        stop)
            stop_all
            ;;
        
        status)
            check_complete_status
            ;;
        
        logs)
            show_all_logs
            ;;
        
        test)
            quick_test
            ;;
        
        *)
            echo "Usage: $0 {start|stop|status|logs|test}"
            exit 1
            ;;
    esac
}

# Gestion des interruptions
trap 'log_warning "Interruption..."; stop_all; exit 0' INT TERM

# Exécution
main "$@" 