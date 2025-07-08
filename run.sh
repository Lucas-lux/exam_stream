#!/bin/bash

# Script pour compiler et exécuter l'application Smart Meters Streaming

# Vérifier que SBT est installé
if ! command -v sbt &> /dev/null
then
    echo "SBT n'est pas installé. Veuillez l'installer pour continuer."
    exit 1
fi

# Vérifier que Java est installé
if ! command -v java &> /dev/null
then
    echo "Java n'est pas installé. Veuillez l'installer pour continuer."
    exit 1
fi

# Vérifier que Scala est installé
if ! command -v scala &> /dev/null
then
    echo "Scala n'est pas installé. Veuillez l'installer pour continuer."
    exit 1
fi

# Vérifier si Kafka est en cours d'exécution
check_kafka() {
    if nc -z localhost 9092 2>/dev/null; then
        echo "Kafka semble être en cours d'exécution sur le port 9092."
    else
        echo "ATTENTION: Kafka ne semble pas être en cours d'exécution sur le port 9092."
        echo "Veuillez démarrer Kafka avant d'exécuter cette application."
        echo "Vous pouvez installer et démarrer Kafka avec les commandes suivantes:"
        echo "---------------------------------------------------------------------"
        echo "# Télécharger Kafka"
        echo "wget https://downloads.apache.org/kafka/3.3.1/kafka_2.13-3.3.1.tgz"
        echo "tar -xzf kafka_2.13-3.3.1.tgz"
        echo "cd kafka_2.13-3.3.1"
        echo ""
        echo "# Démarrer ZooKeeper (dans un terminal séparé)"
        echo "bin/zookeeper-server-start.sh config/zookeeper.properties"
        echo ""
        echo "# Démarrer Kafka (dans un autre terminal séparé)"
        echo "bin/kafka-server-start.sh config/server.properties"
        echo "---------------------------------------------------------------------"
        
        read -p "Voulez-vous continuer quand même? (o/n): " continue_anyway
        if [ "$continue_anyway" != "o" ]; then
            exit 1
        fi
    fi
}

# Créer le répertoire de données s'il n'existe pas
create_data_dir() {
    mkdir -p data
    mkdir -p data/halfhourly_dataset
    mkdir -p data/daily_dataset
    
    if [ ! -f "data/informations_households.csv" ]; then
        echo "ATTENTION: Les fichiers de données semblent manquer."
        echo "Veuillez télécharger le dataset Smart Meters et le décompresser dans le répertoire 'data/'."
    fi
}

# Compiler le projet
compile_project() {
    echo "Compilation du projet..."
    sbt clean compile
    
    if [ $? -ne 0 ]; then
        echo "Erreur de compilation. Abandon."
        exit 1
    fi
}

# Assembler le jar
assemble_jar() {
    echo "Assemblage du jar..."
    sbt assembly
    
    if [ $? -ne 0 ]; then
        echo "Erreur lors de l'assemblage du jar. Abandon."
        exit 1
    fi
}

# Exécuter l'application
run_app() {
    MODE=$1
    
    if [ -z "$MODE" ]; then
        echo "Usage: $0 [producer|consumer|both]"
        echo "  producer - Lance uniquement le producteur Kafka"
        echo "  consumer - Lance uniquement le consommateur Spark Streaming"
        echo "  both     - Lance les deux en parallèle"
        exit 1
    fi
    
    echo "Exécution de l'application en mode $MODE..."
    
    # Exécuter avec spark-submit pour le mode consumer ou both
    if [ "$MODE" == "consumer" ] || [ "$MODE" == "both" ]; then
        spark-submit \
            --class SmartMetersApp \
            --master local[*] \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
            target/scala-2.12/smart-meters-streaming-assembly-1.0.jar $MODE
    else
        # Pour le mode producteur, utiliser scala directement
        scala -cp target/scala-2.12/smart-meters-streaming-assembly-1.0.jar SmartMetersApp $MODE
    fi
}

# Fonction principale
main() {
    check_kafka
    create_data_dir
    compile_project
    assemble_jar
    run_app $1
}

# Appel de la fonction principale avec le premier argument
main $1 