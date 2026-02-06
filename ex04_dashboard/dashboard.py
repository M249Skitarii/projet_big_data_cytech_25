import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import warnings
import plotly.graph_objects as go
warnings.filterwarnings("ignore", category=UserWarning)


def get_conn():
    return psycopg2.connect(
        host="localhost",
        database="ma_base",
        user="mon_user",
        password="mon_password"
    )


st.set_page_config(page_title="NYC Taxi Analysis", layout="wide")
st.title("Analyse Exploratoire du Datamart")


try:
    conn = get_conn()


# --- 1. HISTOGRAMME : Moyenne des distances par jour ---
    st.header("Distance moyenne des courses par jour")
    query_dist = """
        SELECT d.full_date, AVG(f.trip_distance) as avg_distance
        FROM fact_trips f
        JOIN dim_date d ON f.pickup_date_key = d.date_key
        GROUP BY d.full_date
        ORDER BY d.full_date
    """
    df_dist = pd.read_sql(query_dist, conn)
    fig_dist = px.bar(df_dist,
                      x='full_date',
                      y='avg_distance',
                      labels={'full_date': 'Date',
                              'avg_distance': 'Distance Moyenne (miles)'},
                      color_discrete_sequence=['#636EFA'])
    st.plotly_chart(fig_dist, width='stretch')


# --- 2. DOUBLE COURBE : Profit et Distance par heure ---
    st.header("Rentabilité et Distance par heure")
    query_hourly = """
        SELECT
            t.hour,
            AVG(f.total_amount) as avg_profit,
            AVG(f.trip_distance) as avg_distance
        FROM fact_trips f
        JOIN dim_time t ON f.pickup_time_key = t.time_key
        GROUP BY t.hour
        ORDER BY t.hour
    """
    df_hourly = pd.read_sql(query_hourly, conn)

    # Création du graphique avec deux axes Y
    fig_hourly = go.Figure()

    # Courbe du Profit (Axe Y gauche)
    fig_hourly.add_trace(go.Scatter(
        x=df_hourly['hour'],
        y=df_hourly['avg_profit'],
        name='Profit Moyen ($)',
        line=dict(color='green', width=3)
    ))

    # Courbe de la Distance (Axe Y droit)
    fig_hourly.add_trace(go.Scatter(
        x=df_hourly['hour'],
        y=df_hourly['avg_distance'],
        name='Distance Moyenne (miles)',
        line=dict(color='blue', width=3, dash='dot'),
        yaxis='y2'
    ))
    fig_hourly.update_layout(
        title="Corrélation entre Distance et Profit par heure",
        xaxis=dict(title=dict(text="Heure de la journée (0-23h)")),
        yaxis=dict(
            title=dict(text="Profit Moyen ($)", font=dict(color="green")),
            tickfont=dict(color="green")
        ),
        yaxis2=dict(
            title=dict(text="Distance Moyenne (miles)",
                       font=dict(color="blue")),
            tickfont=dict(color="blue"),
            overlaying='y',
            side='right'
        ),
        legend=dict(x=1.1, y=1),
        hovermode="x unified"
    )

    st.plotly_chart(fig_hourly, width='stretch')


# --- 5. DOUBLE BARRE : Profit vs Distance par Borough ---
    st.header("Comparaison Profit et Distance par Arrondissement")

    query_borough_comp = """
        SELECT
            z.borough_name,
            AVG(f.total_amount) as avg_profit,
            AVG(f.trip_distance) as avg_distance
        FROM fact_trips f
        JOIN dim_zones z ON f.pickup_location_id = z.location_id
        GROUP BY z.borough_name
        ORDER BY avg_profit DESC
    """
    df_bor_comp = pd.read_sql(query_borough_comp, conn)

    fig_bor_dual = go.Figure()

    # Barres pour le Profit (Axe Y gauche)
    fig_bor_dual.add_trace(go.Bar(
        x=df_bor_comp['borough_name'],
        y=df_bor_comp['avg_profit'],
        name='Profit Moyen ($)',
        marker_color='indianred'
    ))

    # Ligne ou Points pour la Distance (Axe Y droit)
    fig_bor_dual.add_trace(go.Scatter(
        x=df_bor_comp['borough_name'],
        y=df_bor_comp['avg_distance'],
        name='Distance Moyenne (miles)',
        yaxis='y2',
        mode='lines+markers',
        line=dict(color='royalblue', width=4)
    ))

    fig_bor_dual.update_layout(
        title="Performance financière vs Distance par secteur",
        xaxis=dict(title=dict(text="Arrondissement")),
        yaxis=dict(
            title=dict(text="Profit Moyen ($)", font=dict(color="indianred")),
            tickfont=dict(color="indianred")
        ),
        yaxis2=dict(
            title=dict(text="Distance Moyenne (miles)",
                       font=dict(color="royalblue")),
            tickfont=dict(color="royalblue"),
            overlaying='y',
            side='right'
        ),
        legend=dict(x=1.1, y=1),
        barmode='group'
    )

    st.plotly_chart(fig_bor_dual, width='stretch')


# --- 3. DISQUE (PIE) : Nombre de courses par Borough ---
    st.header("Répartition des trajets par Arrondissement (Borough)")
    query_borough = """
        SELECT z.zone_name, COUNT(f.trip_id) as total_trips
        FROM fact_trips f
        JOIN dim_zones z ON f.pickup_location_id = z.location_id
        GROUP BY z.zone_name
        ORDER BY total_trips DESC
    """
    df_borough = pd.read_sql(query_borough, conn)
    fig_pie = px.pie(df_borough, values='total_trips', names='zone_name',
                     hole=0.4,
                     color_discrete_sequence=px.colors.sequential.RdBu)
    st.plotly_chart(fig_pie, width='stretch')


# --- 4. DIAGRAMME : Répartition des moyens de paiement ---
    st.header("Moyens de paiement les plus utilisés")

    query_payment = """
        SELECT p.payment_name, COUNT(f.trip_id) as total_trips
        FROM fact_trips f
        JOIN dim_payment_types p ON f.payment_type_id = p.payment_type_id
        GROUP BY p.payment_name
        ORDER BY total_trips DESC
    """
    df_payment = pd.read_sql(query_payment, conn)

    # Création d'un graphique en "Donut" (disque percé)
    fig_pay = px.pie(
        df_payment,
        values='total_trips',
        names='payment_name',
        hole=0.5,
        color_discrete_sequence=px.colors.qualitative.Pastel,
        title="Part des transactions par type de paiement"
    )

    # Amélioration de l'affichage des labels
    fig_pay.update_traces(textposition='inside', textinfo='percent+label')

    st.plotly_chart(fig_pay, width='stretch')


except Exception as e:
    st.error(f"Erreur lors du chargement des données : {e}")
