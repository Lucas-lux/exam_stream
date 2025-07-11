#!/usr/bin/env python3
"""
🏠 DASHBOARD SMART METERS - Surveillance en Temps Réel
Surveillance complète de la consommation électrique avec données Kafka
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from kafka import KafkaConsumer
import json
import time
from datetime import datetime, timedelta
import threading
import queue
from collections import defaultdict, deque
import warnings
warnings.filterwarnings('ignore')

# Configuration de la page
st.set_page_config(
    page_title="🏠 Smart Meters Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé pour un design moderne
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
    .stMetric {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .streaming-log {
        background-color: #1e1e1e;
        color: #00ff00;
        padding: 1rem;
        border-radius: 10px;
        font-family: 'Courier New', monospace;
        font-size: 0.9rem;
        max-height: 400px;
        overflow-y: auto;
        border: 1px solid #333;
    }
    .streaming-line {
        margin: 0.2rem 0;
        padding: 0.1rem 0;
        border-bottom: 1px solid #333;
    }
</style>
""", unsafe_allow_html=True)

# Variables globales pour le cache des données (thread-safe)
kafka_data_global = deque(maxlen=1000)
meters_data_global = defaultdict(lambda: deque(maxlen=100))
total_messages_global = 0
alerts_global = deque(maxlen=50)
last_update_global = datetime.now()
kafka_error_global = None
streaming_log_global = deque(maxlen=100)  # Nouveau: pour le log de streaming
start_time_global = datetime.now()  # Nouveau: pour calculer le temps écoulé

# Variables de session Streamlit
if 'kafka_data' not in st.session_state:
    st.session_state.kafka_data = deque(maxlen=1000)
if 'meters_data' not in st.session_state:
    st.session_state.meters_data = defaultdict(lambda: deque(maxlen=100))
if 'last_update' not in st.session_state:
    st.session_state.last_update = datetime.now()
if 'total_messages' not in st.session_state:
    st.session_state.total_messages = 0
if 'alerts' not in st.session_state:
    st.session_state.alerts = deque(maxlen=50)
if 'streaming_log' not in st.session_state:
    st.session_state.streaming_log = deque(maxlen=100)
if 'start_time' not in st.session_state:
    st.session_state.start_time = datetime.now()

def consume_kafka_data():
    """Fonction pour consommer les données Kafka en arrière-plan"""
    global kafka_data_global, meters_data_global, total_messages_global, alerts_global, last_update_global, kafka_error_global, streaming_log_global, start_time_global
    
    try:
        consumer = KafkaConsumer(
            'smart-meters',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',  # Lire depuis le début pour avoir des données
            enable_auto_commit=True,
            group_id='streamlit-dashboard',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000  # Plus de temps pour recevoir des messages
        )
        
        for message in consumer:
            if message.value:
                data = message.value
                timestamp = datetime.now()
                
                # Ajouter aux données globales (thread-safe)
                enriched_data = {
                    **data,
                    'timestamp': timestamp,
                    'hour': timestamp.hour,
                    'day_of_week': timestamp.strftime('%A'),
                    'energy_numeric': float(data.get('energy(kWh/hh)', 0))
                }
                
                kafka_data_global.append(enriched_data)
                meters_data_global[data.get('LCLid', 'Unknown')].append(enriched_data)
                total_messages_global += 1
                last_update_global = timestamp
                
                # Nouveau: Créer ligne de log de streaming dans le format demandé
                elapsed_time = int((timestamp - start_time_global).total_seconds())
                current_time_str = timestamp.strftime("%H:%M:%S")
                meter_id = data.get('LCLid', 'Unknown')
                energy = enriched_data['energy_numeric']
                original_timestamp = data.get('tstp', 'N/A')
                
                streaming_line = f"🕒 {current_time_str} | 📊 Message #{total_messages_global} | 🏠 {meter_id} | ⚡ {energy} kWh | 📅 {original_timestamp} | ⏱️  {elapsed_time}s"
                streaming_log_global.append(streaming_line)
                
                # Détection d'anomalies
                if energy > 2.0:  # Consommation très élevée
                    alert = {
                        'timestamp': timestamp,
                        'type': 'CRITICAL',
                        'meter': meter_id,
                        'message': f'Consommation critique: {energy:.3f} kWh',
                        'value': energy
                    }
                    alerts_global.append(alert)
                elif energy > 1.0:  # Consommation élevée
                    alert = {
                        'timestamp': timestamp,
                        'type': 'WARNING',
                        'meter': meter_id,
                        'message': f'Consommation élevée: {energy:.3f} kWh',
                        'value': energy
                    }
                    alerts_global.append(alert)
                    
    except Exception as e:
        kafka_error_global = f"Erreur Kafka: {e}"

def sync_global_to_session():
    """Synchroniser les données globales avec st.session_state"""
    global kafka_data_global, meters_data_global, total_messages_global, alerts_global, last_update_global, kafka_error_global, streaming_log_global, start_time_global
    
    # Synchroniser les données
    st.session_state.kafka_data = kafka_data_global.copy()
    st.session_state.meters_data = dict(meters_data_global)
    st.session_state.total_messages = total_messages_global
    st.session_state.alerts = alerts_global.copy()
    st.session_state.last_update = last_update_global
    st.session_state.streaming_log = streaming_log_global.copy()
    st.session_state.start_time = start_time_global
    
    # Afficher les erreurs Kafka s'il y en a
    if kafka_error_global:
        st.error(kafka_error_global)
        kafka_error_global = None

def get_realtime_metrics():
    """Calculer les métriques en temps réel"""
    if not st.session_state.kafka_data:
        return None
    
    recent_data = list(st.session_state.kafka_data)[-100:]  # 100 derniers messages
    df = pd.DataFrame(recent_data)
    
    if df.empty:
        return None
    
    return {
        'current_consumption': df['energy_numeric'].iloc[-1] if len(df) > 0 else 0,
        'avg_consumption': df['energy_numeric'].mean(),
        'max_consumption': df['energy_numeric'].max(),
        'min_consumption': df['energy_numeric'].min(),
        'total_consumption': df['energy_numeric'].sum(),
        'active_meters': df['LCLid'].nunique(),
        'messages_per_minute': len(df),
        'std_consumption': df['energy_numeric'].std()
    }

def create_realtime_chart():
    """Créer le graphique de consommation temps réel"""
    if not st.session_state.kafka_data:
        return go.Figure()
    
    df = pd.DataFrame(list(st.session_state.kafka_data)[-200:])  # 200 derniers points
    
    if df.empty:
        return go.Figure()
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Consommation par Compteur', 'Consommation Totale'),
        row_heights=[0.7, 0.3]
    )
    
    # Graphique par compteur
    for meter in df['LCLid'].unique():
        meter_data = df[df['LCLid'] == meter]
        fig.add_trace(
            go.Scatter(
                x=meter_data['timestamp'],
                y=meter_data['energy_numeric'],
                name=meter,
                mode='lines+markers',
                line=dict(width=2),
                hovertemplate='<b>%{fullData.name}</b><br>Temps: %{x}<br>Énergie: %{y:.3f} kWh<extra></extra>'
            ),
            row=1, col=1
        )
    
    # Consommation totale agrégée par minute
    df['minute'] = df['timestamp'].dt.floor('1min')
    total_by_minute = df.groupby('minute')['energy_numeric'].sum().reset_index()
    
    fig.add_trace(
        go.Scatter(
            x=total_by_minute['minute'],
            y=total_by_minute['energy_numeric'],
            name='Total',
            mode='lines+markers',
            fill='tonexty',
            line=dict(color='orange', width=3),
            hovertemplate='Temps: %{x}<br>Total: %{y:.3f} kWh<extra></extra>'
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        title="📊 Consommation Électrique Temps Réel",
        showlegend=True,
        height=600,
        hovermode='x unified'
    )
    
    fig.update_xaxes(title_text="Temps", row=2, col=1)
    fig.update_yaxes(title_text="Consommation (kWh)", row=1, col=1)
    fig.update_yaxes(title_text="Total (kWh)", row=2, col=1)
    
    return fig

def create_consumption_distribution():
    """Créer le graphique de distribution de consommation"""
    if not st.session_state.kafka_data:
        return go.Figure()
    
    df = pd.DataFrame(list(st.session_state.kafka_data))
    
    if df.empty:
        return go.Figure()
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Distribution par Compteur', 'Distribution Horaire'),
        specs=[[{"type": "bar"}, {"type": "box"}]]
    )
    
    # Distribution par compteur
    meter_stats = df.groupby('LCLid')['energy_numeric'].agg(['mean', 'std', 'count']).reset_index()
    
    fig.add_trace(
        go.Bar(
            x=meter_stats['LCLid'],
            y=meter_stats['mean'],
            error_y=dict(type='data', array=meter_stats['std']),
            name='Moyenne ± Écart-type',
            marker_color='lightblue',
            hovertemplate='Compteur: %{x}<br>Moyenne: %{y:.3f} kWh<br>Messages: %{customdata}<extra></extra>',
            customdata=meter_stats['count']
        ),
        row=1, col=1
    )
    
    # Distribution horaire
    for meter in df['LCLid'].unique()[:5]:  # Limiter à 5 compteurs pour la lisibilité
        meter_data = df[df['LCLid'] == meter]
        fig.add_trace(
            go.Box(
                y=meter_data['energy_numeric'],
                name=meter,
                boxpoints='outliers'
            ),
            row=1, col=2
        )
    
    fig.update_layout(
        title="📈 Analyse de Distribution",
        showlegend=True,
        height=400
    )
    
    fig.update_xaxes(title_text="Compteurs", row=1, col=1)
    fig.update_yaxes(title_text="Consommation (kWh)", row=1, col=1)
    fig.update_yaxes(title_text="Consommation (kWh)", row=1, col=2)
    
    return fig

def create_alerts_section():
    """Créer la section des alertes"""
    st.subheader("🚨 Alertes et Anomalies")
    
    if not st.session_state.alerts:
        st.info("Aucune alerte pour le moment")
        return
    
    # Afficher les alertes récentes
    recent_alerts = list(st.session_state.alerts)[-10:]
    
    for alert in reversed(recent_alerts):
        alert_type = alert['type']
        if alert_type == 'CRITICAL':
            st.markdown(f"""
            <div class="alert-critical">
                🔴 <strong>CRITIQUE</strong> - {alert['timestamp'].strftime('%H:%M:%S')} - 
                {alert['meter']}: {alert['message']}
            </div>
            """, unsafe_allow_html=True)
        elif alert_type == 'WARNING':
            st.markdown(f"""
            <div class="alert-warning">
                🟠 <strong>ATTENTION</strong> - {alert['timestamp'].strftime('%H:%M:%S')} - 
                {alert['meter']}: {alert['message']}
            </div>
            """, unsafe_allow_html=True)

def create_streaming_log_section():
    """Créer la section de log de streaming en temps réel"""
    st.subheader("📡 Flux de Données en Temps Réel")
    
    if not st.session_state.streaming_log:
        st.info("En attente des données de streaming...")
        return
    
    # Afficher les logs de streaming dans un container avec style terminal
    log_lines = list(st.session_state.streaming_log)
    
    # Créer le contenu HTML pour le log
    log_content = ""
    for line in reversed(log_lines[-20:]):  # Afficher les 20 dernières lignes, les plus récentes en premier
        log_content += f'<div class="streaming-line">{line}</div>'
    
    st.markdown(f"""
    <div class="streaming-log">
        <div style="color: #00ff00; font-weight: bold; margin-bottom: 0.5rem;">
            📡 STREAMING LOG - {len(log_lines)} messages reçus
        </div>
        {log_content}
    </div>
    """, unsafe_allow_html=True)
    
    # Statistiques du streaming
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.session_state.streaming_log:
            total_time = (datetime.now() - st.session_state.start_time).total_seconds()
            rate = len(st.session_state.streaming_log) / max(total_time, 1)
            st.metric("🚀 Débit", f"{rate:.1f} msg/s")
    
    with col2:
        if st.session_state.streaming_log:
            st.metric("📊 Total Messages", f"{len(st.session_state.streaming_log)}")
    
    with col3:
        if st.session_state.streaming_log:
            elapsed = (datetime.now() - st.session_state.start_time).total_seconds()
            st.metric("⏱️ Temps Écoulé", f"{int(elapsed)}s")

def main():
    """Fonction principale du dashboard"""
    
    # Synchroniser les données globales avec la session
    sync_global_to_session()
    
    # Titre principal
    st.markdown('<h1 class="main-header">⚡ Smart Meters Dashboard</h1>', unsafe_allow_html=True)
    
    # Sidebar pour les contrôles
    st.sidebar.title("🔧 Contrôles")
    
    # Démarrer automatiquement la consommation Kafka
    if 'kafka_thread' not in st.session_state or not st.session_state.kafka_thread.is_alive():
        st.session_state.kafka_thread = threading.Thread(target=consume_kafka_data, daemon=True)
        st.session_state.kafka_thread.start()
        st.sidebar.success("🔄 Connexion Kafka démarrée")
    else:
        st.sidebar.success("✅ Connexion Kafka active")
    
    # Bouton de rafraîchissement
    if st.sidebar.button("🔄 Redémarrer connexion Kafka"):
        # Redémarrer la consommation Kafka
        st.session_state.kafka_thread = threading.Thread(target=consume_kafka_data, daemon=True)
        st.session_state.kafka_thread.start()
        st.sidebar.success("🔄 Connexion Kafka redémarrée")
    
    # Auto-refresh
    auto_refresh = st.sidebar.checkbox("🔄 Actualisation automatique", value=True)
    if auto_refresh:
        refresh_interval = st.sidebar.slider("Intervalle (secondes)", 1, 30, 5)
        st.sidebar.write(f"Actualisation toutes les {refresh_interval}s")
    
    # Filtres
    st.sidebar.subheader("🔍 Filtres")
    
    # Filtre par compteur
    available_meters = []
    if st.session_state.kafka_data:
        df_temp = pd.DataFrame(list(st.session_state.kafka_data))
        available_meters = df_temp['LCLid'].unique().tolist()
    
    selected_meters = st.sidebar.multiselect(
        "Sélectionner les compteurs",
        available_meters,
        default=available_meters[:5] if len(available_meters) > 5 else available_meters
    )
    
    # Seuils d'alerte
    st.sidebar.subheader("⚠️ Seuils d'Alerte")
    warning_threshold = st.sidebar.number_input("Seuil d'avertissement (kWh)", value=1.0, step=0.1)
    critical_threshold = st.sidebar.number_input("Seuil critique (kWh)", value=2.0, step=0.1)
    
    # Métriques principales
    metrics = get_realtime_metrics()
    
    if metrics:
        st.subheader("📊 Métriques Temps Réel")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Consommation Actuelle",
                value=f"{metrics['current_consumption']:.3f} kWh",
                delta=f"{metrics['current_consumption'] - metrics['avg_consumption']:.3f}"
            )
        
        with col2:
            st.metric(
                label="Moyenne",
                value=f"{metrics['avg_consumption']:.3f} kWh",
                delta=f"±{metrics['std_consumption']:.3f}"
            )
        
        with col3:
            st.metric(
                label="Maximum",
                value=f"{metrics['max_consumption']:.3f} kWh",
                delta=f"Min: {metrics['min_consumption']:.3f}"
            )
        
        with col4:
            st.metric(
                label="Compteurs Actifs",
                value=f"{metrics['active_meters']}",
                delta=f"{st.session_state.total_messages} messages"
            )
        
        # Indicateurs de statut
        col1, col2, col3 = st.columns(3)
        
        with col1:
            last_update_ago = (datetime.now() - st.session_state.last_update).total_seconds()
            if last_update_ago < 10:
                st.success(f"🟢 Système en ligne (dernière MAJ: {last_update_ago:.0f}s)")
            elif last_update_ago < 60:
                st.warning(f"🟡 Latence détectée (dernière MAJ: {last_update_ago:.0f}s)")
            else:
                st.error(f"🔴 Système hors ligne (dernière MAJ: {last_update_ago:.0f}s)")
        
        with col2:
            rate = metrics['messages_per_minute'] * 60 / 100  # Estimation messages/seconde
            st.info(f"📈 Débit: ~{rate:.1f} msg/s")
        
        with col3:
            total_power = metrics['total_consumption']
            st.info(f"⚡ Consommation totale: {total_power:.2f} kWh")
    
    # Graphiques principaux
    col1, col2 = st.columns([2, 1])
    
    with col1:
        fig_realtime = create_realtime_chart()
        st.plotly_chart(fig_realtime, use_container_width=True)
    
    with col2:
        create_alerts_section()
    
    # Graphiques d'analyse
    st.subheader("🔍 Analyse Détaillée")
    
    fig_distribution = create_consumption_distribution()
    st.plotly_chart(fig_distribution, use_container_width=True)
    
    # Tableau des données récentes
    if st.session_state.kafka_data:
        st.subheader("📋 Données Récentes")
        
        df_recent = pd.DataFrame(list(st.session_state.kafka_data)[-20:])
        if not df_recent.empty:
            # Formater le tableau
            df_display = df_recent[['timestamp', 'LCLid', 'energy_numeric', 'tstp']].copy()
            df_display['timestamp'] = df_display['timestamp'].dt.strftime('%H:%M:%S')
            df_display.columns = ['Temps Local', 'Compteur', 'Énergie (kWh)', 'Timestamp Original']
            
            st.dataframe(
                df_display.iloc[::-1],  # Inverser pour montrer les plus récents en premier
                use_container_width=True,
                height=300
            )
    
    # NOUVELLE SECTION: Flux de données en temps réel
    st.markdown("---")
    create_streaming_log_section()
    
    # Debug section
    with st.expander("🔧 Debug Kafka"):
        st.write(f"**Messages totaux reçus:** {st.session_state.total_messages}")
        st.write(f"**Dernière mise à jour:** {st.session_state.last_update}")
        st.write(f"**Données en cache:** {len(st.session_state.kafka_data)}")
        st.write(f"**Compteurs actifs:** {len(st.session_state.meters_data)}")
        
        if st.button("🧪 Test Kafka Direct"):
            st.write("Test de connexion Kafka...")
            try:
                from kafka import KafkaConsumer
                import json
                
                consumer = KafkaConsumer(
                    'smart-meters',
                    bootstrap_servers=['localhost:9092'],
                    auto_offset_reset='earliest',
                    group_id='test-debug',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=3000
                )
                
                messages = []
                for message in consumer:
                    if message.value:
                        messages.append(message.value)
                        if len(messages) >= 3:
                            break
                
                if messages:
                    st.success(f"✅ {len(messages)} messages reçus")
                    for i, msg in enumerate(messages):
                        st.json(msg)
                else:
                    st.warning("⚠️ Aucun message reçu")
                    
            except Exception as e:
                st.error(f"❌ Erreur: {e}")
    
    # Statistiques de session
    with st.expander("📊 Statistiques de Session"):
        if st.session_state.kafka_data:
            df_session = pd.DataFrame(list(st.session_state.kafka_data))
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Résumé par Compteur:**")
                meter_summary = df_session.groupby('LCLid')['energy_numeric'].agg([
                    'count', 'mean', 'std', 'min', 'max'
                ]).round(3)
                st.dataframe(meter_summary)
            
            with col2:
                st.write("**Statistiques Globales:**")
                st.write(f"- Total messages reçus: {len(df_session)}")
                st.write(f"- Période: {df_session['timestamp'].min().strftime('%H:%M:%S')} - {df_session['timestamp'].max().strftime('%H:%M:%S')}")
                st.write(f"- Durée de session: {(df_session['timestamp'].max() - df_session['timestamp'].min()).total_seconds():.0f}s")
                st.write(f"- Consommation moyenne: {df_session['energy_numeric'].mean():.3f} kWh")
                st.write(f"- Écart-type: {df_session['energy_numeric'].std():.3f} kWh")
                st.write(f"- Compteurs uniques: {df_session['LCLid'].nunique()}")
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main() 