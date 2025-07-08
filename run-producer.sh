#!/bin/bash

# Script pour compiler et exécuter uniquement le producteur Kafka

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
    sbt compile
    
    if [ $? -ne 0 ]; then
        echo "Erreur de compilation. Abandon."
        exit 1
    fi
}

# Exécuter uniquement le producteur avec spark-submit
run_producer() {
    echo "Exécution du producteur Kafka avec Spark..."

    JAR_PATH="./target/scala-2.12/smart-meters-streaming-assembly-1.0.jar"

    if [ ! -f "$JAR_PATH" ]; then
        echo "Erreur : le fichier JAR $JAR_PATH est introuvable. Avez-vous bien exécuté 'sbt assembly' ?"
        exit 1
    fi

    spark-submit \
        --class kafka.SmartMeterKafkaProducer \
        --master local[*] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.kafka:kafka-clients:3.3.1 \
        "$JAR_PATH"
}

# Fonction principale
main() {
    check_kafka
    create_data_dir
    compile_project
    run_producer
}

# Appel de la fonction principale
main
