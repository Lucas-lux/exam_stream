import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import threading
import warnings
warnings.filterwarnings('ignore')

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Smart Meters - Temps Réel",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé pour un design responsive
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
    }
    
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin: 1rem 0;
        border-left: 5px solid #2a5298;
    }
    
    .real-time-indicator {
        background: linear-gradient(45deg, #28a745, #20c997);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: bold;
        margin: 0.5rem;
        display: inline-block;
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.7; }
        100% { opacity: 1; }
    }
    
    .status-connected {
        color: #28a745;
        font-weight: bold;
    }
    
    .status-disconnected {
        color: #dc3545;
        font-weight: bold;
    }
    
    .stSelectbox > div > div {
        background-color: #f8f9fa;
        border-radius: 5px;
    }
    
    .custom-metric {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        margin: 0.5rem;
    }
    
    .peak-alert {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
        animation: glow 2s ease-in-out infinite alternate;
    }
    
    @keyframes glow {
        from { box-shadow: 0 0 10px rgba(255,107,107,0.5); }
        to { box-shadow: 0 0 20px rgba(255,107,107,0.8); }
    }
    
    .thermal-section {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        margin: 2rem 0;
    }
    
    .performance-gauge {
        background: linear-gradient(135deg, #1dd1a1 0%, #55a3ff 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    @media (max-width: 640px) {
        .main-header {
            padding: 1rem;
            font-size: 0.9rem;
        }
        
        .metric-card {
            padding: 1rem;
            margin: 0.5rem 0;
        }
        
        .real-time-indicator {
            font-size: 0.8rem;
            padding: 0.3rem 0.8rem;
        }
    }
    
    @media (min-width: 641px) and (max-width: 1024px) {
        .main-header {
            padding: 1.5rem;
        }
        
        .metric-card {
            padding: 1.25rem;
        }
    }
</style>
""", unsafe_allow_html=True)

# Configuration Kafka (identique aux scripts Scala)
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic': 'smart-meters',  # Même topic que application.conf
    'consumer_timeout_ms': 5000,  # Timeout plus long
    'auto_offset_reset': 'earliest',  # Même que SmartMeterStreamProcessor
    'enable_auto_commit': True,
    'group_id': 'streamlit-dashboard',  # Groupe unique pour Streamlit
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')) if x else None
}

# Cache pour les données temps réel
if 'kafka_data' not in st.session_state:
    st.session_state.kafka_data = []
if 'kafka_connected' not in st.session_state:
    st.session_state.kafka_connected = False
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'total_messages' not in st.session_state:
    st.session_state.total_messages = 0

def test_kafka_connection():
    """Teste la connexion Kafka"""
    try:
        # Test basique de connexion comme dans les scripts Scala
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            consumer_timeout_ms=3000,
            group_id='test-connection'
        )
        
        # Vérifier les topics disponibles
        topics = consumer.topics()
        consumer.close()
        
        # Vérifier si notre topic existe
        if KAFKA_CONFIG['topic'] in topics:
            st.success(f"✅ Topic '{KAFKA_CONFIG['topic']}' trouvé")
            st.info(f"📊 Nombre de topics disponibles: {len(topics)}")
        else:
            st.warning(f"⚠️ Topic '{KAFKA_CONFIG['topic']}' non trouvé. Topics disponibles: {list(topics)}")
        
        return True
    except Exception as e:
        st.error(f"❌ Erreur de connexion Kafka: {e}")
        st.error("💡 Vérifiez que Kafka est démarré et que le producteur fonctionne")
        return False

def get_kafka_data_batch(max_messages=100):
    """Récupère un lot de données depuis Kafka (SANS CACHE pour corriger les problèmes)"""
    try:
        # Utiliser un group_id unique pour éviter les conflits
        import uuid
        unique_group_id = f"streamlit-{uuid.uuid4().hex[:8]}"
        
        consumer = KafkaConsumer(
            KAFKA_CONFIG['topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            consumer_timeout_ms=8000,  # Plus de temps
            auto_offset_reset='earliest',  # Toujours depuis le début
            enable_auto_commit=False,  # Pas de commit
            group_id=unique_group_id,
            value_deserializer=KAFKA_CONFIG['value_deserializer']
        )
        
        messages = []
        message_count = 0
        
        st.info(f"🔄 Lecture Kafka en cours (group: {unique_group_id})...")
        
        for message in consumer:
            if message.value:
                data = message.value.copy()  # Copie pour éviter les modifications
                # Ajouter timestamp de réception
                data['received_at'] = datetime.now().isoformat()
                data['kafka_offset'] = message.offset
                data['kafka_partition'] = message.partition
                messages.append(data)
                message_count += 1
                
                if message_count >= max_messages:
                    break
        
        consumer.close()
        st.session_state.kafka_connected = True
        st.session_state.last_update = datetime.now()
        st.session_state.total_messages += len(messages)
        
        if messages:
            st.success(f"✅ {len(messages)} messages lus depuis Kafka")
        else:
            st.warning("⚠️ Aucun message lu depuis Kafka")
        
        return messages
    except Exception as e:
        st.session_state.kafka_connected = False
        st.error(f"❌ Erreur lors de la lecture Kafka: {e}")
        st.exception(e)
        return []

def get_kafka_data_batch_no_cache(max_messages=100):
    """Récupère un lot de données depuis Kafka SANS CACHE pour test"""
    try:
        # Utiliser un group_id unique pour chaque lecture
        import uuid
        unique_group_id = f"streamlit-test-{uuid.uuid4().hex[:8]}"
        
        consumer = KafkaConsumer(
            KAFKA_CONFIG['topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            consumer_timeout_ms=8000,  # Plus de temps pour lire
            auto_offset_reset='earliest',  # Toujours lire depuis le début
            enable_auto_commit=False,  # Pas de commit pour test
            group_id=unique_group_id,
            value_deserializer=KAFKA_CONFIG['value_deserializer']
        )
        
        messages = []
        message_count = 0
        
        st.info(f"🔍 Lecture des messages depuis Kafka (group_id: {unique_group_id})...")
        
        for message in consumer:
            if message.value:
                data = message.value
                # Ajouter timestamp de réception
                data['received_at'] = datetime.now().isoformat()
                data['kafka_offset'] = message.offset
                data['kafka_partition'] = message.partition
                messages.append(data)
                message_count += 1
                
                if message_count >= max_messages:
                    break
        
        consumer.close()
        st.session_state.kafka_connected = True
        st.session_state.last_update = datetime.now()
        st.session_state.total_messages += len(messages)
        
        st.success(f"✅ {len(messages)} messages lus depuis Kafka")
        return messages
    except Exception as e:
        st.session_state.kafka_connected = False
        st.error(f"❌ Erreur lors de la lecture Kafka: {e}")
        return []

def get_historical_kafka_data(limit=1000):
    """Récupère les données historiques depuis Kafka"""
    try:
        # Utiliser la même configuration que le test qui fonctionne
        import uuid
        unique_group_id = f"streamlit-historical-{uuid.uuid4().hex[:8]}"
        
        consumer = KafkaConsumer(
            KAFKA_CONFIG['topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            auto_offset_reset='earliest',  # Commence depuis le début
            consumer_timeout_ms=8000,  # Plus de temps
            enable_auto_commit=False,  # Pas de commit
            group_id=unique_group_id,
            value_deserializer=KAFKA_CONFIG['value_deserializer']
        )
        
        messages = []
        message_count = 0
        
        st.info(f"🔄 Lecture historique (group: {unique_group_id})...")
        
        for message in consumer:
            if message.value:
                data = message.value.copy()
                data['kafka_offset'] = message.offset
                data['kafka_partition'] = message.partition
                data['received_at'] = datetime.now().isoformat()
                messages.append(data)
                message_count += 1
                
                if message_count >= limit:
                    break
        
        consumer.close()
        
        if messages:
            st.success(f"✅ {len(messages)} messages historiques lus")
        else:
            st.warning("⚠️ Aucun message historique trouvé")
        
        return messages
    except Exception as e:
        st.error(f"❌ Erreur lors de la lecture historique Kafka: {e}")
        st.exception(e)
        return []

def process_kafka_messages(messages):
    """Traite les messages Kafka en DataFrame"""
    if not messages:
        return pd.DataFrame()
    
    try:
        df = pd.DataFrame(messages)
        
        # Normaliser les noms de colonnes (conforme aux schémas Scala)
        column_mapping = {
            'LCLid': 'meterid',  # Même que SmartMeterKafkaProducer
            'tstp': 'datetime',  # Même que SmartMeterKafkaProducer
            'energy(kWh/hh)': 'energy'  # Même que SmartMeterKafkaProducer
        }
        
        # Renommer les colonnes si elles existent
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.rename(columns={old_col: new_col})
        
        # Conversion des types
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        
        if 'energy' in df.columns:
            df['energy'] = pd.to_numeric(df['energy'], errors='coerce')
        
        if 'received_at' in df.columns:
            df['received_at'] = pd.to_datetime(df['received_at'])
        
        # Supprimer les lignes avec des valeurs manquantes critiques
        df = df.dropna(subset=['meterid', 'energy'])
        
        return df
    except Exception as e:
        st.error(f"Erreur lors du traitement des messages: {e}")
        return pd.DataFrame()

def load_static_data():
    """Charge les données statiques (foyers, météo, etc.)"""
    try:
        household_info = pd.read_csv('data/informations_households.csv')
        weather_data = pd.read_csv('data/weather_hourly_darksky.csv')
        return household_info, weather_data
    except Exception as e:
        st.warning(f"Données statiques non disponibles: {e}")
        return pd.DataFrame(), pd.DataFrame()

def create_real_time_metrics(df):
    """Crée les métriques temps réel"""
    if df.empty:
        return {
            'total_readings': 0,
            'active_meters': 0,
            'avg_consumption': 0,
            'max_consumption': 0,
            'last_reading_time': 'N/A'
        }
    
    metrics = {
        'total_readings': len(df),
        'active_meters': df['meterid'].nunique(),
        'avg_consumption': df['energy'].mean(),
        'max_consumption': df['energy'].max(),
        'last_reading_time': df['datetime'].max() if 'datetime' in df.columns else 'N/A'
    }
    
    return metrics

def create_consumption_chart(df):
    """Crée un graphique de consommation temps réel"""
    if df.empty or 'datetime' not in df.columns:
        return go.Figure().add_annotation(
            text="Aucune donnée disponible",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    # Agrégation par heure
    df_hourly = df.groupby([
        df['datetime'].dt.floor('H'),
        'meterid'
    ])['energy'].sum().reset_index()
    
    fig = px.line(
        df_hourly,
        x='datetime',
        y='energy',
        color='meterid',
        title="💡 Consommation Énergétique en Temps Réel",
        labels={
            'datetime': 'Heure',
            'energy': 'Consommation (kWh)',
            'meterid': 'Compteur'
        }
    )
    
    fig.update_layout(
        height=400,
        showlegend=False,  # Masquer légende si trop de compteurs
        hovermode='x unified'
    )
    
    return fig

def create_meter_distribution_chart(df):
    """Crée un graphique de distribution par compteur"""
    if df.empty:
        return go.Figure()
    
    meter_consumption = df.groupby('meterid')['energy'].agg(['sum', 'mean', 'count']).reset_index()
    
    fig = px.scatter(
        meter_consumption,
        x='count',
        y='sum',
        size='mean',
        hover_data=['meterid'],
        title="📊 Distribution de Consommation par Compteur",
        labels={
            'count': 'Nombre de Relevés',
            'sum': 'Consommation Totale (kWh)',
            'mean': 'Consommation Moyenne'
        }
    )
    
    fig.update_layout(height=400)
    return fig

def create_anomaly_detection(df):
    """Détecte les anomalies de consommation"""
    if df.empty or len(df) < 10:
        return df, go.Figure()
    
    # Calcul des seuils d'anomalie (méthode IQR)
    Q1 = df['energy'].quantile(0.25)
    Q3 = df['energy'].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    # Identifier les anomalies
    df['is_anomaly'] = (df['energy'] < lower_bound) | (df['energy'] > upper_bound)
    anomalies = df[df['is_anomaly']]
    
    # Graphique des anomalies
    fig = go.Figure()
    
    # Points normaux
    normal_data = df[~df['is_anomaly']]
    if not normal_data.empty:
        fig.add_trace(go.Scatter(
            x=normal_data.index,
            y=normal_data['energy'],
            mode='markers',
            name='Normal',
            marker=dict(color='blue', size=6)
        ))
    
    # Points anomalies
    if not anomalies.empty:
        fig.add_trace(go.Scatter(
            x=anomalies.index,
            y=anomalies['energy'],
            mode='markers',
            name='Anomalies',
            marker=dict(color='red', size=10, symbol='x')
        ))
    
    fig.update_layout(
        title="🚨 Détection d'Anomalies de Consommation",
        xaxis_title="Index",
        yaxis_title="Consommation (kWh)",
        height=400
    )
    
    return anomalies, fig

def create_thermal_heatmap(df):
    """Crée une carte thermique de la consommation par heure et jour"""
    if df.empty or 'datetime' not in df.columns:
        return go.Figure().add_annotation(
            text="Aucune donnée temporelle disponible",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    # Extraire heure et jour de la semaine
    df['hour'] = df['datetime'].dt.hour
    df['day_name'] = df['datetime'].dt.day_name()
    
    # Ordre des jours de la semaine
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_names_fr = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
    
    # Créer pivot table pour heatmap
    heatmap_data = df.groupby(['day_name', 'hour'])['energy'].mean().reset_index()
    pivot_data = heatmap_data.pivot(index='day_name', columns='hour', values='energy')
    
    # Réorganiser selon l'ordre des jours
    pivot_data = pivot_data.reindex(days_order)
    pivot_data.index = day_names_fr
    
    # Créer la heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=pivot_data.columns,
        y=pivot_data.index,
        colorscale='Viridis',
        colorbar=dict(title="Consommation (kWh)"),
        hoverongaps=False
    ))
    
    fig.update_layout(
        title="🌡️ Carte Thermique - Consommation par Heure et Jour",
        xaxis_title="Heure de la journée",
        yaxis_title="Jour de la semaine",
        height=400
    )
    
    return fig

def create_consumption_gauges(df):
    """Crée des jauges de performance pour la consommation"""
    if df.empty:
        return go.Figure()
    
    # Calculer les métriques
    current_avg = df['energy'].mean()
    current_max = df['energy'].max()
    
    # Définir les seuils (exemple)
    avg_threshold = 0.5  # kWh
    max_threshold = 2.0  # kWh
    
    # Créer les jauges
    fig = make_subplots(
        rows=1, cols=2,
        specs=[[{'type': 'indicator'}, {'type': 'indicator'}]],
        subplot_titles=('Consommation Moyenne', 'Consommation Maximum')
    )
    
    # Jauge moyenne
    fig.add_trace(go.Indicator(
        mode = "gauge+number+delta",
        value = current_avg,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "Moyenne (kWh)"},
        delta = {'reference': avg_threshold},
        gauge = {
            'axis': {'range': [None, 1.0]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 0.3], 'color': "lightgreen"},
                {'range': [0.3, 0.6], 'color': "yellow"},
                {'range': [0.6, 1.0], 'color': "red"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': avg_threshold
            }
        }
    ), row=1, col=1)
    
    # Jauge maximum
    fig.add_trace(go.Indicator(
        mode = "gauge+number+delta",
        value = current_max,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "Maximum (kWh)"},
        delta = {'reference': max_threshold},
        gauge = {
            'axis': {'range': [None, 3.0]},
            'bar': {'color': "darkred"},
            'steps': [
                {'range': [0, 1.0], 'color': "lightgreen"},
                {'range': [1.0, 2.0], 'color': "yellow"},
                {'range': [2.0, 3.0], 'color': "red"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_threshold
            }
        }
    ), row=1, col=2)
    
    fig.update_layout(height=300)
    return fig

def main():
    # En-tête principal avec indicateur temps réel
    st.markdown("""
    <div class="main-header">
        <h1>⚡ Dashboard Smart Meters - Temps Réel</h1>
        <p>Analyse en temps réel des données de consommation énergétique via Kafka</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Status de connexion Kafka
    col_status1, col_status2, col_status3 = st.columns([2, 2, 2])
    
    with col_status1:
        if st.session_state.kafka_connected:
            st.markdown('<p class="status-connected">🟢 Kafka Connecté</p>', unsafe_allow_html=True)
        else:
            st.markdown('<p class="status-disconnected">🔴 Kafka Déconnecté</p>', unsafe_allow_html=True)
    
    with col_status2:
        if st.session_state.last_update:
            st.write(f"🕒 Dernière MAJ: {st.session_state.last_update.strftime('%H:%M:%S')}")
        else:
            st.write("🕒 Aucune mise à jour")
    
    with col_status3:
        st.write(f"📊 Messages total: {st.session_state.total_messages}")
    
    # Sidebar pour les paramètres
    st.sidebar.header("⚙️ Configuration Kafka")
    
    # Test de connexion
    if st.sidebar.button("🔄 Tester Connexion Kafka"):
        with st.spinner("Test de connexion..."):
            if test_kafka_connection():
                st.sidebar.success("✅ Connexion réussie!")
            else:
                st.sidebar.error("❌ Connexion échouée")
    
    # Paramètres de récupération
    max_messages = st.sidebar.slider(
        "Messages max par batch",
        min_value=10,
        max_value=500,
        value=100,
        help="Nombre maximum de messages à récupérer"
    )
    
    auto_refresh = st.sidebar.checkbox(
        "🔄 Actualisation automatique",
        value=True,
        help="Actualise automatiquement les données"
    )
    
    refresh_interval = st.sidebar.slider(
        "Intervalle d'actualisation (sec)",
        min_value=5,
        max_value=60,
        value=10
    )
    
    # Bouton de récupération manuelle
    if st.sidebar.button("🔄 Récupérer Nouvelles Données"):
        with st.spinner("Récupération des données Kafka..."):
            new_data = get_kafka_data_batch(max_messages)
            if new_data:
                st.session_state.kafka_data.extend(new_data)
                st.sidebar.success(f"✅ {len(new_data)} nouveaux messages récupérés!")
                st.rerun()  # Actualiser l'interface pour afficher les nouvelles données
            else:
                st.sidebar.warning("⚠️ Aucune nouvelle donnée")
    
    # Bouton de test SANS cache
    if st.sidebar.button("🧪 Test Lecture Kafka (SANS CACHE)"):
        with st.spinner("Test de lecture Kafka sans cache..."):
            test_data = get_kafka_data_batch_no_cache(max_messages)
            if test_data:
                st.session_state.kafka_data = test_data  # Remplacer les données
                st.sidebar.success(f"✅ {len(test_data)} messages lus en test!")
                st.rerun()  # Actualiser l'interface pour afficher les nouvelles données
            else:
                st.sidebar.error("❌ Aucune donnée en test")
    
    # Actualisation automatique
    if auto_refresh:
        placeholder = st.empty()
        with placeholder.container():
            new_data = get_kafka_data_batch(max_messages)
            if new_data:
                st.session_state.kafka_data.extend(new_data)
        
        # Programmer la prochaine actualisation
        time.sleep(refresh_interval)
        st.rerun()
    
    # Traitement des données - FORCER LA LECTURE
    df_kafka = pd.DataFrame()
    
    # Vérifier et forcer la lecture si nécessaire
    if not st.session_state.kafka_data:
        st.warning("⚠️ Aucune donnée en session. Lecture forcée depuis Kafka...")
        with st.spinner("🔄 Lecture des données depuis Kafka..."):
            new_data = get_kafka_data_batch(200)  # Lire plus de messages
            if new_data:
                st.session_state.kafka_data = new_data
                st.success(f"✅ {len(new_data)} messages chargés avec succès !")
            else:
                st.error("❌ Impossible de lire les données depuis Kafka")
    
    # Traitement des données en session
    if st.session_state.kafka_data:
        # Garder seulement les 1000 derniers messages pour la performance
        recent_data = st.session_state.kafka_data[-1000:]
        df_kafka = process_kafka_messages(recent_data)
        
        if df_kafka.empty:
            st.error("❌ DataFrame vide après traitement des messages")
            st.write("**Messages bruts:**")
            for i, msg in enumerate(recent_data[:3]):
                st.json(msg)
                if i >= 2:
                    break
        else:
            st.success(f"✅ DataFrame créé avec {len(df_kafka)} lignes")
    
    # Si toujours pas de données, essayer la lecture historique
    if df_kafka.empty:
        st.info("🔄 Tentative de lecture historique...")
        with st.spinner("Chargement des données historiques..."):
            historical_data = get_historical_kafka_data(500)
            if historical_data:
                st.session_state.kafka_data = historical_data
                df_kafka = process_kafka_messages(historical_data)
                if not df_kafka.empty:
                    st.success(f"✅ Données historiques chargées: {len(df_kafka)} lignes")
            else:
                st.error("❌ Aucune donnée historique trouvée")
    
    # Onglets du dashboard
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Temps Réel",
        "📈 Consommation", 
        "🌡️ Analyse Thermique",
        "🚨 Anomalies",
        "📋 Données Brutes",
        "⚙️ Diagnostic"
    ])
    
    with tab1:
        st.header("📊 Métriques Temps Réel")
        
        if not df_kafka.empty:
            metrics = create_real_time_metrics(df_kafka)
            
            # Métriques principales
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    label="📊 Total Relevés",
                    value=f"{metrics['total_readings']:,}",
                    delta=f"+{len(df_kafka[-100:])}" if len(df_kafka) > 100 else "Initial"
                )
            
            with col2:
                st.metric(
                    label="🏠 Compteurs Actifs",
                    value=f"{metrics['active_meters']:,}",
                    delta="En ligne"
                )
            
            with col3:
                st.metric(
                    label="⚡ Consommation Moy.",
                    value=f"{metrics['avg_consumption']:.3f} kWh",
                    delta=f"Max: {metrics['max_consumption']:.3f}"
                )
            
            with col4:
                if metrics['last_reading_time'] != 'N/A':
                    time_diff = datetime.now() - pd.to_datetime(metrics['last_reading_time'])
                    delta_text = f"Il y a {int(time_diff.total_seconds())}s"
                else:
                    delta_text = "N/A"
                
                st.metric(
                    label="🕒 Dernier Relevé",
                    value=metrics['last_reading_time'].strftime('%H:%M:%S') if metrics['last_reading_time'] != 'N/A' else 'N/A',
                    delta=delta_text
                )
            
            # Jauges de performance
            st.subheader("🎯 Jauges de Performance")
            gauges_chart = create_consumption_gauges(df_kafka)
            st.plotly_chart(gauges_chart, use_container_width=True)
            
            # Graphiques temps réel
            col1, col2 = st.columns(2)
            
            with col1:
                consumption_chart = create_consumption_chart(df_kafka)
                st.plotly_chart(consumption_chart, use_container_width=True)
            
            with col2:
                distribution_chart = create_meter_distribution_chart(df_kafka)
                st.plotly_chart(distribution_chart, use_container_width=True)
            
            # Carte thermique
            st.subheader("🌡️ Analyse Thermique")
            thermal_chart = create_thermal_heatmap(df_kafka)
            st.plotly_chart(thermal_chart, use_container_width=True)
            
            # Indicateur de flux en temps réel
            st.markdown(
                '<div class="real-time-indicator">🔴 LIVE - Données en temps réel</div>',
                unsafe_allow_html=True
            )
        
        else:
            st.warning("⚠️ Aucune donnée Kafka disponible. Vérifiez que le producteur est en cours d'exécution.")
            
            # Instructions de démarrage
            st.info("""
            **Pour démarrer le flux de données:**
            1. Assurez-vous que Kafka est démarré
            2. Lancez le producteur: `./run-producer.sh`
            3. Actualisez cette page
            """)
    
    with tab2:
        st.header("📈 Analyse de Consommation")
        
        if not df_kafka.empty:
            # Histogramme de distribution
            st.subheader("📊 Distribution des Consommations")
            
            fig_hist = px.histogram(
                df_kafka,
                x='energy',
                nbins=50,
                title="Distribution des Consommations Énergétiques",
                labels={'energy': 'Consommation (kWh)', 'count': 'Fréquence'}
            )
            fig_hist.update_layout(height=400)
            st.plotly_chart(fig_hist, use_container_width=True)
            
            # Top consommateurs
            st.subheader("🏆 Top Consommateurs")
            
            top_consumers = df_kafka.groupby('meterid')['energy'].agg(['sum', 'mean', 'count']).sort_values('sum', ascending=False).head(10)
            top_consumers = top_consumers.round(6)
            
            # Reformater pour plus de clarté
            top_consumers = top_consumers.reset_index()
            top_consumers.columns = ['🏠 Compteur', '⚡ Total (kWh)', '📊 Moyenne (kWh)', '📈 Nb Relevés']
            
            # Ajouter une colonne de classement
            top_consumers['🏆 Rang'] = range(1, len(top_consumers) + 1)
            
            # Réorganiser les colonnes
            top_consumers = top_consumers[['🏆 Rang', '🏠 Compteur', '⚡ Total (kWh)', '📊 Moyenne (kWh)', '📈 Nb Relevés']]
            
            # Ajouter un indicateur visuel
            top_consumers['📊 Niveau'] = top_consumers['⚡ Total (kWh)'].apply(
                lambda x: "🔴 TRÈS ÉLEVÉ" if x > 2.0 else "🟠 ÉLEVÉ" if x > 1.0 else "🟡 MODÉRÉ" if x > 0.5 else "🟢 FAIBLE"
            )
            
            st.dataframe(
                top_consumers,
                use_container_width=True,
                hide_index=True,
                column_config={
                    '🏆 Rang': st.column_config.NumberColumn(
                        "🏆 Rang",
                        help="Classement du compteur",
                        width="small",
                        format="%d"
                    ),
                    '🏠 Compteur': st.column_config.TextColumn(
                        "🏠 Compteur",
                        help="Identifiant du compteur",
                        width="medium"
                    ),
                    '⚡ Total (kWh)': st.column_config.NumberColumn(
                        "⚡ Total (kWh)",
                        help="Consommation totale",
                        width="medium",
                        format="%.6f"
                    ),
                    '📊 Moyenne (kWh)': st.column_config.NumberColumn(
                        "📊 Moyenne (kWh)",
                        help="Consommation moyenne par relevé",
                        width="medium",
                        format="%.6f"
                    ),
                    '📈 Nb Relevés': st.column_config.NumberColumn(
                        "📈 Nb Relevés",
                        help="Nombre total de relevés",
                        width="small",
                        format="%d"
                    ),
                    '📊 Niveau': st.column_config.TextColumn(
                        "📊 Niveau",
                        help="Niveau de consommation",
                        width="medium"
                    )
                }
            )
            
            # Évolution temporelle
            if 'datetime' in df_kafka.columns:
                st.subheader("⏰ Évolution Temporelle")
                
                # Agrégation par heure
                hourly_consumption = df_kafka.groupby(df_kafka['datetime'].dt.floor('H'))['energy'].agg(['sum', 'mean', 'count']).reset_index()
                
                fig_evolution = make_subplots(
                    rows=2, cols=1,
                    subplot_titles=['Consommation Totale par Heure', 'Nombre de Relevés par Heure'],
                    vertical_spacing=0.1
                )
                
                fig_evolution.add_trace(
                    go.Scatter(x=hourly_consumption['datetime'], y=hourly_consumption['sum'], name='Total'),
                    row=1, col=1
                )
                
                fig_evolution.add_trace(
                    go.Bar(x=hourly_consumption['datetime'], y=hourly_consumption['count'], name='Relevés'),
                    row=2, col=1
                )
                
                fig_evolution.update_layout(height=600)
                st.plotly_chart(fig_evolution, use_container_width=True)
        
        else:
            st.warning("Aucune donnée de consommation disponible")
    
    with tab3:
        st.header("🌡️ Analyse Thermique et Temporelle")
        
        if not df_kafka.empty:
            # Carte thermique principale
            st.subheader("🌡️ Carte Thermique - Consommation par Heure et Jour")
            thermal_chart = create_thermal_heatmap(df_kafka)
            st.plotly_chart(thermal_chart, use_container_width=True)
            
            # Analyse par période
            if 'datetime' in df_kafka.columns:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("📈 Profil Horaire")
                    hourly_profile = df_kafka.groupby(df_kafka['datetime'].dt.hour)['energy'].mean().reset_index()
                    
                    fig_hourly = px.line(
                        hourly_profile,
                        x='datetime',
                        y='energy',
                        title="Consommation Moyenne par Heure",
                        labels={'datetime': 'Heure', 'energy': 'Consommation (kWh)'}
                    )
                    fig_hourly.update_layout(height=300)
                    st.plotly_chart(fig_hourly, use_container_width=True)
                
                with col2:
                    st.subheader("📊 Profil Hebdomadaire")
                    daily_profile = df_kafka.groupby(df_kafka['datetime'].dt.day_name())['energy'].mean().reset_index()
                    
                    # Ordre des jours
                    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                    day_names_fr = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']
                    
                    daily_profile['day_order'] = daily_profile['datetime'].map(dict(zip(days_order, range(7))))
                    daily_profile = daily_profile.sort_values('day_order')
                    daily_profile['day_fr'] = daily_profile['datetime'].map(dict(zip(days_order, day_names_fr)))
                    
                    fig_daily = px.bar(
                        daily_profile,
                        x='day_fr',
                        y='energy',
                        title="Consommation Moyenne par Jour",
                        labels={'day_fr': 'Jour', 'energy': 'Consommation (kWh)'}
                    )
                    fig_daily.update_layout(height=300)
                    st.plotly_chart(fig_daily, use_container_width=True)
                
                # Analyse des pics de consommation
                st.subheader("⚡ Analyse des Pics de Consommation")
                
                # Identifier les pics (top 10%)
                peak_threshold = df_kafka['energy'].quantile(0.9)
                peaks = df_kafka[df_kafka['energy'] >= peak_threshold].copy()
                
                if not peaks.empty:
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric(
                            "🔥 Pics Détectés",
                            len(peaks),
                            f"{len(peaks)/len(df_kafka)*100:.1f}% du total"
                        )
                    
                    with col2:
                        peak_hour = peaks['datetime'].dt.hour.mode()
                        if len(peak_hour) > 0:
                            st.metric(
                                "⏰ Heure de Pic",
                                f"{peak_hour[0]}h",
                                "Heure la plus fréquente"
                            )
                    
                    with col3:
                        st.metric(
                            "📊 Consommation Pic Max",
                            f"{peaks['energy'].max():.3f} kWh",
                            f"Seuil: {peak_threshold:.3f}"
                        )
                    
                    # Graphique des pics
                    peaks['hour'] = peaks['datetime'].dt.hour
                    peak_hours = peaks.groupby('hour').size().reset_index(name='count')
                    
                    fig_peaks = px.bar(
                        peak_hours,
                        x='hour',
                        y='count',
                        title="Distribution des Pics de Consommation par Heure",
                        labels={'hour': 'Heure', 'count': 'Nombre de Pics'}
                    )
                    fig_peaks.update_layout(height=300)
                    st.plotly_chart(fig_peaks, use_container_width=True)
                
        else:
            st.warning("Aucune donnée disponible pour l'analyse thermique")
    
    with tab4:
        st.header("🚨 Détection d'Anomalies")
        
        if not df_kafka.empty:
            anomalies, anomaly_chart = create_anomaly_detection(df_kafka)
            
            # Métriques d'anomalies
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    label="🚨 Anomalies Détectées",
                    value=len(anomalies),
                    delta=f"{(len(anomalies)/len(df_kafka)*100):.1f}% du total"
                )
            
            with col2:
                if not anomalies.empty:
                    st.metric(
                        label="⚡ Consommation Max Anormale",
                        value=f"{anomalies['energy'].max():.3f} kWh",
                        delta="Seuil dépassé"
                    )
                else:
                    st.metric(label="⚡ Consommation Max Anormale", value="N/A")
            
            with col3:
                if not anomalies.empty:
                    st.metric(
                        label="🏠 Compteurs Concernés",
                        value=anomalies['meterid'].nunique(),
                        delta="Avec anomalies"
                    )
                else:
                    st.metric(label="🏠 Compteurs Concernés", value=0)
            
            # Graphique d'anomalies
            st.plotly_chart(anomaly_chart, use_container_width=True)
            
            # Liste des anomalies récentes
            if not anomalies.empty:
                st.subheader("📋 Anomalies Récentes")
                
                anomalies_display = anomalies[['meterid', 'energy', 'datetime']].copy()
                anomalies_display = anomalies_display.sort_values('datetime', ascending=False).head(20)
                
                # Formatage pour la clarté
                anomalies_display['datetime'] = anomalies_display['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
                anomalies_display['energy'] = anomalies_display['energy'].round(6)
                
                # Renommer les colonnes avec des emojis
                anomalies_display = anomalies_display.rename(columns={
                    'meterid': '🏠 Compteur',
                    'energy': '⚡ Consommation (kWh)',
                    'datetime': '📅 Date et Heure'
                })
                
                # Ajouter une colonne de sévérité
                anomalies_display['🚨 Niveau'] = anomalies_display['⚡ Consommation (kWh)'].apply(
                    lambda x: "🔴 CRITIQUE" if x > 2.0 else "🟠 ÉLEVÉ" if x > 1.0 else "🟡 MODÉRÉ"
                )
                
                st.dataframe(
                    anomalies_display,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        '🏠 Compteur': st.column_config.TextColumn(
                            "🏠 Compteur",
                            help="Identifiant du compteur concerné",
                            width="medium"
                        ),
                        '⚡ Consommation (kWh)': st.column_config.NumberColumn(
                            "⚡ Consommation (kWh)",
                            help="Valeur de consommation anormale",
                            width="medium",
                            format="%.6f"
                        ),
                        '📅 Date et Heure': st.column_config.TextColumn(
                            "📅 Date et Heure",
                            help="Moment de l'anomalie",
                            width="large"
                        ),
                        '🚨 Niveau': st.column_config.TextColumn(
                            "🚨 Niveau",
                            help="Niveau de sévérité de l'anomalie",
                            width="medium"
                        )
                    }
                )
            
        else:
            st.warning("Aucune donnée pour la détection d'anomalies")
    
    with tab5:
        st.header("📋 Données Brutes Kafka")
        
        if not df_kafka.empty:
            # Statistiques des données
            st.subheader("📊 Statistiques")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Informations Générales:**")
                st.write(f"- Nombre total d'enregistrements: {len(df_kafka):,}")
                st.write(f"- Nombre de compteurs uniques: {df_kafka['meterid'].nunique():,}")
                st.write(f"- Période couverte: {df_kafka['datetime'].min()} → {df_kafka['datetime'].max()}" if 'datetime' in df_kafka.columns else "- Période: Non définie")
            
            with col2:
                st.write("**📊 Statistiques de Consommation:**")
                energy_stats = df_kafka['energy'].describe()
                
                # Créer un DataFrame pour les statistiques avec formatage
                stats_display = pd.DataFrame({
                    '📊 Métrique': [
                        '📈 Nombre de valeurs',
                        '📊 Moyenne',
                        '📉 Écart-type',
                        '🔻 Minimum',
                        '🔹 1er quartile (25%)',
                        '🔸 Médiane (50%)',
                        '🔹 3ème quartile (75%)',
                        '🔺 Maximum'
                    ],
                    '⚡ Valeur': [
                        f"{energy_stats['count']:.0f} relevés",
                        f"{energy_stats['mean']:.6f} kWh",
                        f"{energy_stats['std']:.6f} kWh",
                        f"{energy_stats['min']:.6f} kWh",
                        f"{energy_stats['25%']:.6f} kWh",
                        f"{energy_stats['50%']:.6f} kWh",
                        f"{energy_stats['75%']:.6f} kWh",
                        f"{energy_stats['max']:.6f} kWh"
                    ],
                    '📋 Description': [
                        'Total des mesures',
                        'Consommation moyenne',
                        'Variabilité des données',
                        'Consommation la plus faible',
                        '25% des valeurs sont inférieures',
                        'Valeur médiane',
                        '75% des valeurs sont inférieures',
                        'Consommation la plus élevée'
                    ]
                })
                
                st.dataframe(
                    stats_display,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        '📊 Métrique': st.column_config.TextColumn(
                            "📊 Métrique",
                            help="Type de statistique",
                            width="medium"
                        ),
                        '⚡ Valeur': st.column_config.TextColumn(
                            "⚡ Valeur",
                            help="Valeur de la statistique",
                            width="medium"
                        ),
                        '📋 Description': st.column_config.TextColumn(
                            "📋 Description",
                            help="Explication de la métrique",
                            width="large"
                        )
                    }
                )
            
            # Aperçu des données
            st.subheader("👁️ Aperçu des Données")
            
            # Filtres
            col1, col2, col3 = st.columns(3)
            
            with col1:
                selected_meters = st.multiselect(
                    "Filtrer par compteur:",
                    options=df_kafka['meterid'].unique()[:20],  # Limiter à 20 pour performance
                    default=df_kafka['meterid'].unique()[:5] if len(df_kafka['meterid'].unique()) >= 5 else df_kafka['meterid'].unique()
                )
            
            with col2:
                show_anomalies_only = st.checkbox("Afficher seulement les anomalies")
            
            with col3:
                max_rows = st.slider("Nombre de lignes à afficher:", 10, 500, 100)
            
            # Appliquer les filtres
            filtered_df = df_kafka.copy()
            
            if selected_meters:
                filtered_df = filtered_df[filtered_df['meterid'].isin(selected_meters)]
            
            if show_anomalies_only and 'is_anomaly' in filtered_df.columns:
                filtered_df = filtered_df[filtered_df['is_anomaly']]
            
            # Préparer les données pour l'affichage
            display_df = filtered_df.copy()
            
            # Formater les colonnes pour une meilleure lisibilité
            if 'datetime' in display_df.columns:
                display_df['datetime'] = display_df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
                display_df = display_df.sort_values('datetime', ascending=False)
            
            if 'energy' in display_df.columns:
                display_df['energy'] = display_df['energy'].round(6)
            
            if 'received_at' in display_df.columns:
                display_df['received_at'] = pd.to_datetime(display_df['received_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Renommer les colonnes pour l'affichage
            column_names = {
                'meterid': '🏠 Compteur',
                'datetime': '📅 Date et Heure',
                'energy': '⚡ Consommation (kWh)',
                'received_at': '📡 Reçu le',
                'is_anomaly': '🚨 Anomalie',
                'kafka_offset': '📊 Offset Kafka',
                'kafka_partition': '🔄 Partition'
            }
            
            # Appliquer les nouveaux noms de colonnes
            display_df = display_df.rename(columns=column_names)
            
            # Limiter le nombre de lignes
            display_df = display_df.head(max_rows)
            
            # Affichage des données avec style
            st.markdown("### 📋 Données en Temps Réel")
            
            # Afficher le nombre total de lignes
            st.info(f"📊 Affichage de {len(display_df)} lignes sur {len(filtered_df)} disponibles")
            
            # Configuration des colonnes à afficher
            if not display_df.empty:
                # Réorganiser les colonnes dans un ordre logique
                preferred_order = ['🏠 Compteur', '📅 Date et Heure', '⚡ Consommation (kWh)', '📡 Reçu le', '🚨 Anomalie']
                available_columns = [col for col in preferred_order if col in display_df.columns]
                other_columns = [col for col in display_df.columns if col not in preferred_order]
                final_columns = available_columns + other_columns
                
                display_df = display_df[final_columns]
            
            # Affichage avec style personnalisé
            st.dataframe(
                display_df,
                use_container_width=True,
                height=400,
                hide_index=True,
                column_config={
                    '🏠 Compteur': st.column_config.TextColumn(
                        "🏠 Compteur",
                        help="Identifiant du compteur électrique",
                        width="medium"
                    ),
                    '📅 Date et Heure': st.column_config.TextColumn(
                        "📅 Date et Heure",
                        help="Horodatage de la mesure",
                        width="large"
                    ),
                    '⚡ Consommation (kWh)': st.column_config.NumberColumn(
                        "⚡ Consommation (kWh)",
                        help="Consommation énergétique en kWh",
                        width="medium",
                        format="%.6f"
                    ),
                    '📡 Reçu le': st.column_config.TextColumn(
                        "📡 Reçu le",
                        help="Date de réception par Kafka",
                        width="large"
                    ),
                    '🚨 Anomalie': st.column_config.CheckboxColumn(
                        "🚨 Anomalie",
                        help="Indique si la mesure est anormale",
                        width="small"
                    )
                }
            )
            
            # Export des données
            if st.button("💾 Exporter les données (CSV)"):
                csv_data = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 Télécharger CSV",
                    data=csv_data,
                    file_name=f"kafka_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        else:
            st.warning("Aucune donnée brute disponible")
    
    with tab6:
        st.header("⚙️ Diagnostic Système")
        
        # Configuration Kafka
        st.subheader("🔧 Configuration Kafka")
        
        config_df = pd.DataFrame([
            {"Paramètre": "Bootstrap Servers", "Valeur": ", ".join(KAFKA_CONFIG['bootstrap_servers'])},
            {"Paramètre": "Topic", "Valeur": KAFKA_CONFIG['topic']},
            {"Paramètre": "Timeout (ms)", "Valeur": KAFKA_CONFIG['consumer_timeout_ms']},
            {"Paramètre": "Auto Offset Reset", "Valeur": KAFKA_CONFIG['auto_offset_reset']}
        ])
        
        st.dataframe(config_df, use_container_width=True, hide_index=True)
        
        # Statistiques de session
        st.subheader("📊 Statistiques de Session")
        
        session_stats = pd.DataFrame([
            {"Métrique": "Messages en mémoire", "Valeur": len(st.session_state.kafka_data)},
            {"Métrique": "Statut connexion", "Valeur": "Connecté" if st.session_state.kafka_connected else "Déconnecté"},
            {"Métrique": "Total messages traités", "Valeur": st.session_state.total_messages},
            {"Métrique": "Dernière mise à jour", "Valeur": str(st.session_state.last_update) if st.session_state.last_update else "Jamais"}
        ])
        
        st.dataframe(session_stats, use_container_width=True, hide_index=True)
        
        # Actions de maintenance
        st.subheader("🧹 Actions de Maintenance")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🗑️ Vider le Cache"):
                st.session_state.kafka_data = []
                st.session_state.total_messages = 0
                st.session_state.last_update = None
                st.success("Cache vidé!")
        
        with col2:
            if st.button("🔄 Réinitialiser Connexion"):
                st.session_state.kafka_connected = False
                st.success("Connexion réinitialisée!")
        
        with col3:
            if st.button("📊 Rafraîchir Métriques"):
                st.session_state.total_messages = len(st.session_state.kafka_data)
                st.success("Métriques rafraîchies!")
        
        # Test de lecture Kafka direct
        st.subheader("🧪 Test de Lecture Kafka")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📊 Test Lecture Directe"):
                st.write("**🔍 Test de lecture directe depuis Kafka...**")
                try:
                    from kafka import KafkaConsumer
                    import json
                    
                    # Configuration du consumer pour le test
                    consumer = KafkaConsumer(
                        'smart-meters',
                        bootstrap_servers=['localhost:9092'],
                        auto_offset_reset='earliest',
                        consumer_timeout_ms=8000,  # Plus de temps
                        group_id=f'debug-{datetime.now().strftime("%H%M%S")}',
                        value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
                    )
                    
                    st.info("🔄 Lecture en cours...")
                    
                    messages = []
                    message_details = []
                    
                    for i, message in enumerate(consumer):
                        if message.value:
                            msg_data = message.value
                            messages.append(msg_data)
                            
                            # Collecter les détails du message
                            message_details.append({
                                'Offset': message.offset,
                                'Partition': message.partition,
                                'Timestamp': message.timestamp,
                                'LCLid': msg_data.get('LCLid', 'N/A'),
                                'Timestamp Mesure': msg_data.get('tstp', 'N/A'),
                                'Consommation (kWh)': msg_data.get('energy(kWh/hh)', 'N/A')
                            })
                        
                        if i >= 9:  # Limiter à 10 messages pour test
                            break
                    
                    consumer.close()
                    
                    if messages:
                        st.success(f"✅ **{len(messages)} messages lus directement depuis Kafka**")
                        
                        # Affichage des messages en tableau clair
                        st.subheader("📋 Messages Détaillés")
                        
                        if message_details:
                            import pandas as pd
                            df_messages = pd.DataFrame(message_details)
                            
                            # Formatage des colonnes
                            if 'Timestamp' in df_messages.columns:
                                df_messages['Timestamp'] = pd.to_datetime(df_messages['Timestamp'], unit='ms', errors='coerce')
                                df_messages['Timestamp'] = df_messages['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                            
                            if 'Consommation (kWh)' in df_messages.columns:
                                df_messages['Consommation (kWh)'] = pd.to_numeric(df_messages['Consommation (kWh)'], errors='coerce').round(6)
                            
                            st.dataframe(
                                df_messages, 
                                use_container_width=True,
                                hide_index=True,
                                column_config={
                                    'Offset': st.column_config.NumberColumn("📊 Offset", format="%d"),
                                    'Partition': st.column_config.NumberColumn("🔄 Partition", format="%d"),
                                    'Timestamp': st.column_config.TextColumn("🕒 Timestamp Kafka"),
                                    'LCLid': st.column_config.TextColumn("🏠 Compteur"),
                                    'Timestamp Mesure': st.column_config.TextColumn("📅 Timestamp Mesure"),
                                    'Consommation (kWh)': st.column_config.NumberColumn("⚡ Consommation", format="%.6f")
                                }
                            )
                        
                        # Affichage JSON détaillé des premiers messages
                        st.subheader("🔍 Format JSON Brut")
                        
                        for i, msg in enumerate(messages[:3]):
                            st.write(f"**Message {i+1}:**")
                            st.json(msg)
                            if i < len(messages) - 1:
                                st.divider()
                        
                        # Statistiques rapides
                        st.subheader("📊 Statistiques Rapides")
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            compteurs_uniques = len(set(msg.get('LCLid', '') for msg in messages))
                            st.metric("🏠 Compteurs Uniques", compteurs_uniques)
                        
                        with col2:
                            consommations = [float(msg.get('energy(kWh/hh)', 0)) for msg in messages if msg.get('energy(kWh/hh)')]
                            if consommations:
                                avg_consumption = sum(consommations) / len(consommations)
                                st.metric("⚡ Consommation Moy.", f"{avg_consumption:.6f} kWh")
                            else:
                                st.metric("⚡ Consommation Moy.", "N/A")
                        
                        with col3:
                            if consommations:
                                max_consumption = max(consommations)
                                st.metric("🔥 Consommation Max", f"{max_consumption:.6f} kWh")
                            else:
                                st.metric("🔥 Consommation Max", "N/A")
                    
                    else:
                        st.error("❌ **Aucun message lu depuis Kafka**")
                        st.warning("💡 Vérifiez que le producteur envoie des données")
                        
                except Exception as e:
                    st.error(f"❌ **Erreur lors du test:** {str(e)}")
                    st.exception(e)
        
        with col2:
            if st.button("🔍 Vérifier Topic"):
                st.write("**Vérification du topic smart-meters...**")
                try:
                    from kafka import KafkaConsumer
                    
                    consumer = KafkaConsumer(
                        bootstrap_servers=['localhost:9092'],
                        consumer_timeout_ms=3000
                    )
                    
                    topics = consumer.topics()
                    consumer.close()
                    
                    if 'smart-meters' in topics:
                        st.success("✅ Topic 'smart-meters' trouvé")
                        st.write(f"Topics disponibles: {list(topics)}")
                    else:
                        st.error("❌ Topic 'smart-meters' non trouvé")
                        st.write(f"Topics disponibles: {list(topics)}")
                        
                except Exception as e:
                    st.error(f"❌ Erreur: {e}")
        
        # Logs système (simulation)
        st.subheader("📝 Logs Récents")
        
        logs = [
            f"[{datetime.now().strftime('%H:%M:%S')}] Application démarrée",
            f"[{datetime.now().strftime('%H:%M:%S')}] Connexion Kafka initialisée",
            f"[{datetime.now().strftime('%H:%M:%S')}] {len(st.session_state.kafka_data)} messages en mémoire",
            f"[{datetime.now().strftime('%H:%M:%S')}] Statut: {'Connecté' if st.session_state.kafka_connected else 'Déconnecté'}"
        ]
        
        for log in logs:
            st.text(log)
        
        # Affichage des données actuelles en session
        st.subheader("💾 Données en Session Streamlit")
        
        if st.session_state.kafka_data:
            st.success(f"✅ **{len(st.session_state.kafka_data)} messages en mémoire**")
            
            # Créer un DataFrame pour l'affichage
            try:
                import pandas as pd
                
                # Prendre les 10 premiers messages pour l'aperçu
                sample_data = st.session_state.kafka_data[:10]
                
                session_details = []
                for i, msg in enumerate(sample_data):
                    session_details.append({
                        'Index': i + 1,
                        'Compteur': msg.get('LCLid', 'N/A'),
                        'Timestamp': msg.get('tstp', 'N/A'),
                        'Consommation (kWh)': msg.get('energy(kWh/hh)', msg.get('energy', 'N/A')),
                        'Reçu le': msg.get('received_at', 'N/A'),
                        'Offset Kafka': msg.get('kafka_offset', 'N/A')
                    })
                
                if session_details:
                    df_session = pd.DataFrame(session_details)
                    
                    # Formatage
                    if 'Consommation (kWh)' in df_session.columns:
                        df_session['Consommation (kWh)'] = pd.to_numeric(df_session['Consommation (kWh)'], errors='coerce').round(6)
                    
                    st.subheader("📋 Aperçu des Données en Session")
                    st.dataframe(
                        df_session,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            'Index': st.column_config.NumberColumn("📊 #", format="%d", width="small"),
                            'Compteur': st.column_config.TextColumn("🏠 Compteur", width="medium"),
                            'Timestamp': st.column_config.TextColumn("📅 Timestamp", width="large"),
                            'Consommation (kWh)': st.column_config.NumberColumn("⚡ Consommation", format="%.6f", width="medium"),
                            'Reçu le': st.column_config.TextColumn("📡 Reçu le", width="large"),
                            'Offset Kafka': st.column_config.NumberColumn("📊 Offset", format="%d", width="small")
                        }
                    )
                
                # Statistiques de session
                st.subheader("📊 Statistiques de Session")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("📋 Messages Total", len(st.session_state.kafka_data))
                
                with col2:
                    # Compteurs uniques
                    compteurs = set()
                    for msg in st.session_state.kafka_data:
                        compteur = msg.get('LCLid') or msg.get('meterid')
                        if compteur:
                            compteurs.add(compteur)
                    st.metric("🏠 Compteurs", len(compteurs))
                
                with col3:
                    # Consommation moyenne
                    consommations = []
                    for msg in st.session_state.kafka_data:
                        energy = msg.get('energy(kWh/hh)') or msg.get('energy')
                        if energy is not None:
                            try:
                                consommations.append(float(energy))
                            except:
                                pass
                    if consommations:
                        avg_cons = sum(consommations) / len(consommations)
                        st.metric("⚡ Consommation Moy.", f"{avg_cons:.6f} kWh")
                    else:
                        st.metric("⚡ Consommation Moy.", "N/A")
                
                with col4:
                    # Dernière mise à jour
                    if st.session_state.last_update:
                        last_update_str = st.session_state.last_update.strftime("%H:%M:%S")
                        st.metric("🕒 Dernière MAJ", last_update_str)
                    else:
                        st.metric("🕒 Dernière MAJ", "N/A")
                
                # Affichage JSON des premiers messages
                with st.expander("🔍 Voir les Messages JSON Bruts"):
                    st.write("**3 premiers messages en format JSON:**")
                    for i, msg in enumerate(st.session_state.kafka_data[:3]):
                        st.write(f"**Message {i+1}:**")
                        st.json(msg)
                        if i < 2:
                            st.divider()
                
            except Exception as e:
                st.error(f"❌ Erreur lors de l'affichage des données de session: {e}")
                st.write("**Messages bruts:**")
                for i, msg in enumerate(st.session_state.kafka_data[:3]):
                    st.json(msg)
                    if i >= 2:
                        break
        
        else:
            st.warning("⚠️ **Aucun message en mémoire**")
            st.info("💡 **Actions recommandées:**")
            st.write("1. Utilisez le bouton '🧪 Test Lecture Kafka (SANS CACHE)' dans la sidebar")
            st.write("2. Ou cliquez sur '🔄 Récupérer Nouvelles Données' dans la sidebar")
            st.write("3. Vérifiez que le producteur Python envoie des données")
            
            # Bouton de test rapide
            if st.button("🚀 Test Rapide de Lecture"):
                st.info("🔄 Test de lecture rapide en cours...")
                try:
                    test_data = get_kafka_data_batch_no_cache(50)
                    if test_data:
                        st.session_state.kafka_data = test_data
                        st.success(f"✅ {len(test_data)} messages récupérés !")
                        st.rerun()
                    else:
                        st.error("❌ Aucune donnée récupérée")
                except Exception as e:
                    st.error(f"❌ Erreur: {e}")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; padding: 1rem;">
        <p>⚡ Dashboard Smart Meters - Temps Réel | 
        Données: Kafka Stream | 
        Développé avec Streamlit & Kafka-Python</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main() 