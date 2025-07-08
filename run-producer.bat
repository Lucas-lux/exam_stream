    @echo off
REM Script pour compiler et exécuter uniquement le producteur Kafka sous Windows

echo Vérification des prérequis...

REM Vérifier que Java est installé
java -version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo Java n'est pas installé ou n'est pas dans le PATH. Veuillez l'installer pour continuer.
    exit /b 1
)

REM Vérifier que SBT est installé
sbt -version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo SBT n'est pas installé ou n'est pas dans le PATH. Veuillez l'installer pour continuer.
    exit /b 1
)

REM Créer le répertoire de données s'il n'existe pas
echo Création des répertoires de données...
if not exist data mkdir data
if not exist data\halfhourly_dataset mkdir data\halfhourly_dataset
if not exist data\daily_dataset mkdir data\daily_dataset

if not exist data\informations_households.csv (
    echo ATTENTION: Les fichiers de données semblent manquer.
    echo Veuillez télécharger le dataset Smart Meters et le décompresser dans le répertoire 'data/'.
)

REM Compiler le projet
echo Compilation du projet...
call sbt clean compile assembly
if %ERRORLEVEL% NEQ 0 (
    echo Erreur de compilation. Abandon.
    exit /b 1
)

REM Exécuter uniquement le producteur avec spark-submit
echo Exécution du producteur Kafka avec Spark...

call spark-submit ^
    --class kafka.SmartMeterKafkaProducer ^
    --master local[*] ^
    --packages org.apache.spark:spark-sql_2.12:3.3.2,org.apache.kafka:kafka-clients:3.3.1 ^
    target\scala-2.12\smart-meters-streaming-assembly-1.0.jar

if %ERRORLEVEL% NEQ 0 (
    echo Erreur lors de l'exécution du producteur Kafka.
    exit /b 1
)

echo Producteur Kafka terminé avec succès. 