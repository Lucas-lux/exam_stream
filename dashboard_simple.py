#!/usr/bin/env python3
"""
🌐 DASHBOARD STREAMLIT SIMPLE - Smart Meters
Version simplifiée sans threads - lecture directe des données Kafka
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from kafka import KafkaConsumer
import json
import time
from datetime import datetime
from collections import defaultdict

# Configuration de la page
st.set_page_config(
    page_title="🏠 Smart Meters Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
    }
    .alert-critical {
        background-color: #ff4444;
        color: white;
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.2rem 0;
    }
    .alert-warning {
        background-color: #ffaa00;
        color: white;
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.2rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=5)  # Cache pendant 5 secondes
def fetch_kafka_data(max_messages=100):
    """Récupérer les données Kafka directement"""
    try:
        consumer = KafkaConsumer(
            'smart-meters',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='streamlit-dashboard-fixed',  # Groupe fixe pour éviter les problèmes
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000  # Plus de temps pour recevoir les données
        )
        
        messages = []
        start_time = time.time()
        
        for message in consumer:
            if message.value:
                data = message.value
                enriched_data = {
                    **data,
                    'timestamp': datetime.now(),
                    'energy_numeric': float(data.get('energy(kWh/hh)', 0)),
                    'meter_id': data.get('LCLid', 'Unknown')
                }
                messages.append(enriched_data)
                
                # Arrêter après max_messages ou 3 secondes
                if len(messages) >= max_messages or (time.time() - start_time) > 3:
                    break
        
        consumer.close()
        return messages
        
    except Exception as e:
        st.error(f"Erreur Kafka: {e}")
        return []

def create_metrics_from_data(data):
    """Calculer les métriques à partir des données"""
    if not data:
        return None
        
    df = pd.DataFrame(data)
    
    return {
        'total_messages': len(df),
        'avg_consumption': df['energy_numeric'].mean(),
        'max_consumption': df['energy_numeric'].max(),
        'min_consumption': df['energy_numeric'].min(),
        'total_consumption': df['energy_numeric'].sum(),
        'active_meters': df['meter_id'].nunique(),
        'high_consumption_count': len(df[df['energy_numeric'] > 1.0]),
        'critical_consumption_count': len(df[df['energy_numeric'] > 2.0])
    }

def create_consumption_chart(data):
    """Créer le graphique de consommation"""
    if not data:
        return go.Figure()
        
    df = pd.DataFrame(data)
    
    # Graphique en barres par compteur
    meter_summary = df.groupby('meter_id')['energy_numeric'].agg(['mean', 'count', 'sum']).reset_index()
    
    fig = px.bar(
        meter_summary, 
        x='meter_id', 
        y='mean',
        title="📊 Consommation Moyenne par Compteur",
        labels={'mean': 'Consommation Moyenne (kWh)', 'meter_id': 'Compteur'},
        color='mean',
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(height=400, showlegend=False)
    return fig

def create_distribution_chart(data):
    """Créer le graphique de distribution"""
    if not data:
        return go.Figure()
        
    df = pd.DataFrame(data)
    
    fig = px.histogram(
        df, 
        x='energy_numeric',
        nbins=20,
        title="📈 Distribution de la Consommation",
        labels={'energy_numeric': 'Consommation (kWh)', 'count': 'Nombre de lectures'},
        color_discrete_sequence=['lightblue']
    )
    
    # Ajouter des lignes pour les seuils
    fig.add_vline(x=1.0, line_dash="dash", line_color="orange", 
                  annotation_text="Seuil d'alerte")
    fig.add_vline(x=2.0, line_dash="dash", line_color="red", 
                  annotation_text="Seuil critique")
    
    fig.update_layout(height=400)
    return fig

def display_recent_data(data, max_rows=20):
    """Afficher les données récentes"""
    if not data:
        st.info("Aucune donnée disponible")
        return
        
    df = pd.DataFrame(data)
    
    # Prendre les données les plus récentes
    recent_df = df.tail(max_rows).copy()
    
    # Formater pour l'affichage
    display_df = recent_df[['meter_id', 'energy_numeric', 'tstp']].copy()
    display_df.columns = ['Compteur', 'Énergie (kWh)', 'Timestamp Original']
    
    # Colorier selon la consommation
    def color_energy(val):
        if val > 2.0:
            return 'background-color: #ffcccc'  # Rouge clair
        elif val > 1.0:
            return 'background-color: #fff3cd'  # Jaune clair
        else:
            return 'background-color: #d4edda'  # Vert clair
    
    styled_df = display_df.style.applymap(color_energy, subset=['Énergie (kWh)'])
    st.dataframe(styled_df, use_container_width=True, height=400)

def display_alerts(data):
    """Afficher les alertes"""
    if not data:
        st.info("Aucune alerte")
        return
        
    df = pd.DataFrame(data)
    
    # Filtrer les alertes
    critical_alerts = df[df['energy_numeric'] > 2.0]
    warning_alerts = df[(df['energy_numeric'] > 1.0) & (df['energy_numeric'] <= 2.0)]
    
    alert_count = 0
    
    # Alertes critiques
    for _, alert in critical_alerts.tail(5).iterrows():
        st.markdown(f"""
        <div class="alert-critical">
            🔴 <strong>CRITIQUE</strong> - {alert['meter_id']}: {alert['energy_numeric']:.3f} kWh
        </div>
        """, unsafe_allow_html=True)
        alert_count += 1
    
    # Alertes d'avertissement
    for _, alert in warning_alerts.tail(5).iterrows():
        st.markdown(f"""
        <div class="alert-warning">
            🟠 <strong>ATTENTION</strong> - {alert['meter_id']}: {alert['energy_numeric']:.3f} kWh
        </div>
        """, unsafe_allow_html=True)
        alert_count += 1
    
    if alert_count == 0:
        st.success("✅ Aucune anomalie détectée")

def main():
    """Fonction principale du dashboard"""
    
    # Titre principal
    st.markdown('<h1 class="main-header">⚡ Smart Meters Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar
    st.sidebar.title("🔧 Contrôles")
    
    # Configuration
    max_messages = st.sidebar.slider("Nombre max de messages", 50, 500, 100)
    auto_refresh = st.sidebar.checkbox("🔄 Actualisation automatique", value=True)
    
    if auto_refresh:
        refresh_interval = st.sidebar.slider("Intervalle (secondes)", 3, 30, 5)
        st.sidebar.write(f"Actualisation toutes les {refresh_interval}s")
    
    # Bouton de rafraîchissement manuel
    if st.sidebar.button("🔄 Rafraîchir maintenant"):
        st.cache_data.clear()
    
    # Récupérer les données Kafka
    with st.spinner("📡 Récupération des données Kafka..."):
        kafka_data = fetch_kafka_data(max_messages)
    
    if not kafka_data:
        st.error("❌ Aucune donnée Kafka disponible")
        st.info("Vérifiez que le producteur Kafka fonctionne et que le topic 'smart-meters' contient des données")
        return
    
    # Calculer les métriques
    metrics = create_metrics_from_data(kafka_data)
    
    # Affichage des métriques principales
    st.subheader("📊 Métriques Temps Réel")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Messages reçus",
            value=metrics['total_messages'],
            delta=f"Max: {metrics['max_consumption']:.3f} kWh"
        )
    
    with col2:
        st.metric(
            label="Consommation Moyenne",
            value=f"{metrics['avg_consumption']:.3f} kWh",
            delta=f"Min: {metrics['min_consumption']:.3f} kWh"
        )
    
    with col3:
        st.metric(
            label="Compteurs Actifs",
            value=metrics['active_meters'],
            delta=f"Total: {metrics['total_consumption']:.2f} kWh"
        )
    
    with col4:
        st.metric(
            label="Alertes",
            value=metrics['high_consumption_count'],
            delta=f"Critiques: {metrics['critical_consumption_count']}"
        )
    
    # Graphiques principaux
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Graphique de consommation
        fig_consumption = create_consumption_chart(kafka_data)
        st.plotly_chart(fig_consumption, use_container_width=True)
        
        # Graphique de distribution
        fig_distribution = create_distribution_chart(kafka_data)
        st.plotly_chart(fig_distribution, use_container_width=True)
    
    with col2:
        # Section alertes
        st.subheader("🚨 Alertes et Anomalies")
        display_alerts(kafka_data)
    
    # Tableau des données récentes
    st.subheader("📋 Données Récentes")
    display_recent_data(kafka_data)
    
    # Informations de debug
    with st.expander("🔧 Informations Debug"):
        st.write(f"**Nombre de messages traités:** {len(kafka_data)}")
        st.write(f"**Dernière mise à jour:** {datetime.now().strftime('%H:%M:%S')}")
        if kafka_data:
            df_debug = pd.DataFrame(kafka_data)
            st.write(f"**Compteurs uniques:** {df_debug['meter_id'].nunique()}")
            st.write("**Premiers messages:**")
            st.json(kafka_data[:3])
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main() 