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
