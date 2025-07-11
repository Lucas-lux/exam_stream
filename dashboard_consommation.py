#!/usr/bin/env python3
"""
🌟 Dashboard Smart Meters - Consommation Énergétique Temps Réel
Système d'analyse et visualisation des données de compteurs intelligents
"""

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
from typing import List, Dict, Optional
import warnings
warnings.filterwarnings('ignore')

# Configuration de la page
st.set_page_config(
    page_title="⚡ Smart Meters Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé pour le dashboard
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%);
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 0.5rem;
    }
    
    .alert-high {
        background-color: #ffebee;
        border-left: 5px solid #f44336;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .alert-normal {
        background-color: #e8f5e8;
        border-left: 5px solid #4caf50;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    .real-time-indicator {
        background: #ff4444;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        text-align: center;
        font-weight: bold;
        margin: 1rem 0;
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.7; }
        100% { opacity: 1; }
    }
    
    .consumption-gauge {
        text-align: center;
        font-size: 2rem;
        font-weight: bold;
        color: #2a5298;
    }
</style>
""", unsafe_allow_html=True)

# Variables de session
if 'kafka_data' not in st.session_state:
    st.session_state.kafka_data = []
if 'consumption_history' not in st.session_state:
    st.session_state.consumption_history = []
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'total_consumption' not in st.session_state:
    st.session_state.total_consumption = 0.0

# Fonctions utilitaires
@st.cache_data(ttl=5)
def get_kafka_data_realtime(max_messages=200):
    """Récupérer les données Kafka en temps réel"""
    try:
        consumer = KafkaConsumer(
            'smart-meters',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='streamlit-dashboard',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=3000
        )
        
        messages = []
        for message in consumer:
            if message.value:
                messages.append(message.value)
                if len(messages) >= max_messages:
                    break
        
        consumer.close()
        return messages
        
    except Exception as e:
        st.error(f"Erreur Kafka: {e}")
        return []

def process_consumption_data(raw_data: List[Dict]) -> pd.DataFrame:
    """Traiter les données de consommation"""
    if not raw_data:
        return pd.DataFrame()
    
    df = pd.DataFrame(raw_data)
    
    # Nettoyer et convertir les données
    df['energy'] = pd.to_numeric(df['energy(kWh/hh)'], errors='coerce')
    df['timestamp'] = pd.to_datetime(df['tstp'], errors='coerce')
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.day_name()
    df['date'] = df['timestamp'].dt.date
    
    # Filtrer les valeurs valides
    df = df.dropna(subset=['energy', 'timestamp'])
    df = df[df['energy'] >= 0]
    
    return df.sort_values('timestamp')

def calculate_consumption_metrics(df: pd.DataFrame) -> Dict:
    """Calculer les métriques de consommation"""
    if df.empty:
        return {
            'total_consumption': 0,
            'avg_consumption': 0,
            'max_consumption': 0,
            'min_consumption': 0,
            'active_meters': 0,
            'high_consumption_alerts': 0
        }
    
    return {
        'total_consumption': df['energy'].sum(),
        'avg_consumption': df['energy'].mean(),
        'max_consumption': df['energy'].max(),
        'min_consumption': df['energy'].min(),
        'active_meters': df['LCLid'].nunique(),
        'high_consumption_alerts': len(df[df['energy'] > 1.0])
    }

def create_realtime_consumption_chart(df: pd.DataFrame):
    """Créer un graphique de consommation temps réel"""
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="Aucune donnée disponible", 
                          xref="paper", yref="paper",
                          x=0.5, y=0.5, showarrow=False)
        return fig
    
    # Agrégation par minute pour la visualisation
    df_minute = df.groupby(df['timestamp'].dt.floor('5min')).agg({
        'energy': ['sum', 'mean', 'count'],
        'LCLid': 'nunique'
    }).reset_index()
    
    df_minute.columns = ['timestamp', 'total_energy', 'avg_energy', 'readings_count', 'meters_count']
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=['Consommation Totale (kWh)', 'Nombre de Compteurs Actifs'],
        vertical_spacing=0.1
    )
    
    # Graphique principal - Consommation
    fig.add_trace(
        go.Scatter(
            x=df_minute['timestamp'],
            y=df_minute['total_energy'],
            mode='lines+markers',
            name='Consommation Totale',
            line=dict(color='#1f77b4', width=3),
            fill='tonexty'
        ),
        row=1, col=1
    )
    
    # Graphique secondaire - Compteurs actifs
    fig.add_trace(
        go.Bar(
            x=df_minute['timestamp'],
            y=df_minute['meters_count'],
            name='Compteurs Actifs',
            marker_color='#ff7f0e'
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        title='📊 Consommation Énergétique en Temps Réel',
        height=600,
        showlegend=True,
        template='plotly_white'
    )
    
    return fig

def create_consumption_heatmap(df: pd.DataFrame):
    """Créer une heatmap de consommation par heure et jour"""
    if df.empty:
        return go.Figure()
    
    # Agrégation par heure et jour de la semaine
    heatmap_data = df.groupby(['day_of_week', 'hour'])['energy'].mean().reset_index()
    pivot_data = heatmap_data.pivot(index='day_of_week', columns='hour', values='energy')
    
    # Réorganiser les jours de la semaine
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pivot_data = pivot_data.reindex(day_order)
    
    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=pivot_data.columns,
        y=pivot_data.index,
        colorscale='Viridis',
        colorbar=dict(title="Consommation (kWh)")
    ))
    
    fig.update_layout(
        title='🔥 Heatmap de Consommation par Heure et Jour',
        xaxis_title='Heure de la journée',
        yaxis_title='Jour de la semaine',
        height=400
    )
    
    return fig

def create_consumption_distribution(df: pd.DataFrame):
    """Créer un graphique de distribution des consommations"""
    if df.empty:
        return go.Figure()
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=['Distribution des Consommations', 'Box Plot par Compteur'],
        specs=[[{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Histogramme
    fig.add_trace(
        go.Histogram(
            x=df['energy'],
            nbinsx=50,
            name='Distribution',
            marker_color='skyblue'
        ),
        row=1, col=1
    )
    
    # Box plot pour les top 10 compteurs
    top_meters = df['LCLid'].value_counts().head(10).index
    df_top = df[df['LCLid'].isin(top_meters)]
    
    fig.add_trace(
        go.Box(
            x=df_top['LCLid'],
            y=df_top['energy'],
            name='Top Compteurs',
            marker_color='lightcoral'
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        title='📈 Distribution et Variabilité des Consommations',
        height=400,
        showlegend=False
    )
    
    return fig

def create_consumption_gauge(current_consumption: float, threshold: float = 2.0):
    """Créer une jauge de consommation instantanée"""
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = current_consumption,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "Consommation Instantanée (kWh)"},
        delta = {'reference': threshold},
        gauge = {
            'axis': {'range': [None, 5]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 1], 'color': "lightgray"},
                {'range': [1, 2], 'color': "yellow"},
                {'range': [2, 5], 'color': "red"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': threshold
            }
        }
    ))
    
    fig.update_layout(height=300)
    return fig

def detect_anomalies(df: pd.DataFrame, threshold_multiplier: float = 2.5) -> pd.DataFrame:
    """Détecter les anomalies de consommation"""
    if df.empty:
        return pd.DataFrame()
    
    # Calculer le seuil d'anomalie
    mean_consumption = df['energy'].mean()
    std_consumption = df['energy'].std()
    threshold = mean_consumption + (threshold_multiplier * std_consumption)
    
    # Identifier les anomalies
    anomalies = df[df['energy'] > threshold].copy()
    anomalies['anomaly_score'] = (anomalies['energy'] - mean_consumption) / std_consumption
    
    return anomalies.sort_values('energy', ascending=False)

# Interface principale
def main():
    # En-tête
    st.markdown("""
    <div class="main-header">
        <h1>⚡ Dashboard Smart Meters - Consommation Temps Réel</h1>
        <p>Analyse avancée des données de consommation énergétique</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sidebar - Configuration
    st.sidebar.header("🔧 Configuration")
    
    auto_refresh = st.sidebar.checkbox("🔄 Actualisation automatique", value=True)
    refresh_interval = st.sidebar.slider("Intervalle (secondes)", 5, 60, 10)
    max_messages = st.sidebar.slider("Messages max", 50, 500, 200)
    anomaly_threshold = st.sidebar.slider("Seuil d'anomalie (σ)", 1.5, 4.0, 2.5)
    
    # Bouton de rafraîchissement manuel
    if st.sidebar.button("🔄 Actualiser Maintenant"):
        st.cache_data.clear()
        st.rerun()
    
    # Récupération des données
    with st.spinner("📡 Récupération des données Kafka..."):
        new_data = get_kafka_data_realtime(max_messages)
        
        if new_data:
            st.session_state.kafka_data.extend(new_data)
            # Garder seulement les 2000 derniers messages pour la performance
            st.session_state.kafka_data = st.session_state.kafka_data[-2000:]
            st.session_state.last_update = datetime.now()
    
    # Traitement des données
    df = process_consumption_data(st.session_state.kafka_data)
    metrics = calculate_consumption_metrics(df)
    
    # Indicateur temps réel
    if st.session_state.last_update:
        st.markdown(f"""
        <div class="real-time-indicator">
            🔴 LIVE - Dernière mise à jour: {st.session_state.last_update.strftime('%H:%M:%S')}
        </div>
        """, unsafe_allow_html=True)
    
    # Métriques principales
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "📊 Consommation Totale",
            f"{metrics['total_consumption']:.2f} kWh",
            delta=f"Moy: {metrics['avg_consumption']:.3f}"
        )
    
    with col2:
        st.metric(
            "🏠 Compteurs Actifs",
            f"{metrics['active_meters']}",
            delta="En ligne"
        )
    
    with col3:
        st.metric(
            "⚡ Consommation Max",
            f"{metrics['max_consumption']:.3f} kWh",
            delta=f"Min: {metrics['min_consumption']:.3f}"
        )
    
    with col4:
        current_consumption = df['energy'].tail(10).mean() if not df.empty else 0
        st.metric(
            "🎯 Consommation Actuelle",
            f"{current_consumption:.3f} kWh",
            delta="Temps réel"
        )
    
    with col5:
        st.metric(
            "🚨 Alertes",
            f"{metrics['high_consumption_alerts']}",
            delta="Anomalies détectées"
        )
    
    # Onglets principaux
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Temps Réel",
        "🔥 Analyse Thermique", 
        "📈 Distribution",
        "⚠️ Anomalies",
        "📋 Données Détaillées"
    ])
    
    with tab1:
        st.header("📊 Consommation en Temps Réel")
        
        if not df.empty:
            # Graphique principal
            realtime_chart = create_realtime_consumption_chart(df)
            st.plotly_chart(realtime_chart, use_container_width=True)
            
            # Jauge de consommation instantanée
            col1, col2 = st.columns([1, 2])
            
            with col1:
                current_value = df['energy'].tail(1).iloc[0] if not df.empty else 0
                gauge_chart = create_consumption_gauge(current_value, anomaly_threshold)
                st.plotly_chart(gauge_chart, use_container_width=True)
            
            with col2:
                # Statistiques récentes
                st.subheader("📈 Statistiques des 100 dernières mesures")
                recent_df = df.tail(100)
                
                if not recent_df.empty:
                    st.write(f"**Consommation moyenne:** {recent_df['energy'].mean():.3f} kWh")
                    st.write(f"**Écart-type:** {recent_df['energy'].std():.3f} kWh")
                    st.write(f"**Compteurs différents:** {recent_df['LCLid'].nunique()}")
                    st.write(f"**Période:** {recent_df['timestamp'].min()} à {recent_df['timestamp'].max()}")
        else:
            st.warning("⚠️ Aucune donnée disponible. Vérifiez que le producteur Kafka fonctionne.")
    
    with tab2:
        st.header("🔥 Analyse Thermique")
        
        if not df.empty:
            heatmap_chart = create_consumption_heatmap(df)
            st.plotly_chart(heatmap_chart, use_container_width=True)
            
            # Analyse des patterns
            st.subheader("📊 Patterns de Consommation")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Consommation par heure
                hourly_consumption = df.groupby('hour')['energy'].mean()
                fig_hourly = px.line(
                    x=hourly_consumption.index,
                    y=hourly_consumption.values,
                    title="Consommation Moyenne par Heure",
                    labels={'x': 'Heure', 'y': 'Consommation (kWh)'}
                )
                st.plotly_chart(fig_hourly, use_container_width=True)
            
            with col2:
                # Consommation par jour de la semaine
                daily_consumption = df.groupby('day_of_week')['energy'].mean()
                fig_daily = px.bar(
                    x=daily_consumption.index,
                    y=daily_consumption.values,
                    title="Consommation Moyenne par Jour",
                    labels={'x': 'Jour', 'y': 'Consommation (kWh)'}
                )
                st.plotly_chart(fig_daily, use_container_width=True)
        else:
            st.info("📊 Les données d'analyse thermique apparaîtront ici une fois les données disponibles.")
    
    with tab3:
        st.header("📈 Distribution et Variabilité")
        
        if not df.empty:
            distribution_chart = create_consumption_distribution(df)
            st.plotly_chart(distribution_chart, use_container_width=True)
            
            # Top consommateurs
            st.subheader("🏆 Top Consommateurs")
            top_consumers = df.groupby('LCLid')['energy'].agg(['mean', 'max', 'count']).sort_values('mean', ascending=False).head(10)
            top_consumers.columns = ['Consommation Moyenne', 'Consommation Max', 'Nombre de Mesures']
            st.dataframe(top_consumers, use_container_width=True)
        else:
            st.info("📈 Les analyses de distribution apparaîtront ici.")
    
    with tab4:
        st.header("⚠️ Détection d'Anomalies")
        
        if not df.empty:
            anomalies = detect_anomalies(df, anomaly_threshold)
            
            if not anomalies.empty:
                st.warning(f"🚨 {len(anomalies)} anomalies détectées!")
                
                # Graphique des anomalies
                fig_anomalies = px.scatter(
                    anomalies,
                    x='timestamp',
                    y='energy',
                    size='anomaly_score',
                    color='LCLid',
                    title="Anomalies de Consommation",
                    labels={'energy': 'Consommation (kWh)', 'timestamp': 'Timestamp'}
                )
                st.plotly_chart(fig_anomalies, use_container_width=True)
                
                # Table des anomalies
                st.subheader("📋 Détail des Anomalies")
                anomaly_display = anomalies[['LCLid', 'timestamp', 'energy', 'anomaly_score']].copy()
                anomaly_display['anomaly_score'] = anomaly_display['anomaly_score'].round(2)
                st.dataframe(anomaly_display, use_container_width=True)
            else:
                st.success("✅ Aucune anomalie détectée avec le seuil actuel.")
                
                # Graphique de distribution normale
                if not df.empty:
                    fig_normal = px.histogram(
                        df,
                        x='energy',
                        title="Distribution Normale des Consommations",
                        nbins=50
                    )
                    st.plotly_chart(fig_normal, use_container_width=True)
        else:
            st.info("⚠️ Les anomalies seront détectées une fois les données disponibles.")
    
    with tab5:
        st.header("📋 Données Détaillées")
        
        if not df.empty:
            # Filtres
            st.subheader("🔍 Filtres")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                selected_meters = st.multiselect(
                    "Compteurs",
                    options=df['LCLid'].unique(),
                    default=df['LCLid'].unique()[:5] if len(df['LCLid'].unique()) > 5 else df['LCLid'].unique()
                )
            
            with col2:
                min_energy = st.number_input("Consommation min (kWh)", value=0.0, step=0.1)
            
            with col3:
                max_energy = st.number_input("Consommation max (kWh)", value=df['energy'].max(), step=0.1)
            
            # Filtrage des données
            filtered_df = df[
                (df['LCLid'].isin(selected_meters)) &
                (df['energy'] >= min_energy) &
                (df['energy'] <= max_energy)
            ]
            
            st.subheader(f"📊 Données Filtrées ({len(filtered_df)} enregistrements)")
            st.dataframe(
                filtered_df[['LCLid', 'timestamp', 'energy', 'hour', 'day_of_week']],
                use_container_width=True
            )
            
            # Option de téléchargement
            if st.button("📥 Télécharger les données filtrées (CSV)"):
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="⬇️ Télécharger CSV",
                    data=csv,
                    file_name=f"smart_meters_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        else:
            st.info("📋 Les données détaillées apparaîtront ici.")
    
    # Auto-refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()

if __name__ == "__main__":
    main() 