import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from dotenv import load_dotenv

# ==========================================================
# Configuration
# ==========================================================

load_dotenv()

st.set_page_config(
    page_title="Olist Dashboard",
    page_icon="📊",
    layout="wide"
)

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "olist")
DB_USER = os.getenv("POSTGRES_USER", "spark")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "spark")

# ==========================================================
# Style sobre
# ==========================================================
st.markdown(
    """
    <style>
    .main-title {
        font-size: 2rem;
        font-weight: 700;
        margin-bottom: 0.2rem;
    }
    .subtitle {
        color: #5d6778;
        margin-bottom: 1rem;
    }
    .info-box {
        background-color: #f7f9fc;
        padding: 1rem;
        border-radius: 10px;
        border: 1px solid #e6eaf1;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ==========================================================
# Connexion et chargement
# ==========================================================
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


@st.cache_data(ttl=60)
def load_table(query: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql(query, conn)


def load_gold_data():
    try:
        df_region = load_table("SELECT * FROM gold.ca_par_region")
        df_categorie = load_table("SELECT * FROM gold.ca_par_categorie")
        df_delai = load_table("SELECT * FROM gold.delai_livraison_moyen")
        df_satisfaction = load_table("SELECT * FROM gold.score_satisfaction_par_vendeur")

        return df_region, df_categorie, df_delai, df_satisfaction

    except Exception as e:
        st.error(f"Erreur de connexion ou de lecture PostgreSQL : {e}")
        st.info(
            "Vérifie le conteneur PostgreSQL, le schéma gold, et les variables "
            "définies dans le fichier .env."
        )
        st.stop()


df_region, df_categorie, df_delai, df_satisfaction = load_gold_data()

# ==========================================================
# Vérification minimale DAT
# ==========================================================
if df_region.empty or df_categorie.empty or df_delai.empty or df_satisfaction.empty:
    st.warning(
        "Une ou plusieurs tables Gold sont vides. Le DAT prévoit que les tables Gold "
        "doivent alimenter le dashboard avec des agrégats métier."
    )

# ==========================================================
# Préparation des données
# ==========================================================
df_region = df_region.sort_values("total_ca", ascending=False)
df_categorie = df_categorie.sort_values("total_ca", ascending=False)
df_delai = df_delai.sort_values("delai_moyen_jours", ascending=True)
df_satisfaction = df_satisfaction.sort_values(
    by=["note_moyenne", "nb_avis"],
    ascending=[False, False]
)

# KPI globaux
total_ca = float(df_region["total_ca"].sum()) if not df_region.empty else 0
nb_commandes = int(df_region["nb_commandes"].sum()) if not df_region.empty else 0
ca_moyen = total_ca / nb_commandes if nb_commandes else 0
delai_moyen = float(df_delai["delai_moyen_jours"].mean()) if not df_delai.empty else 0
note_moyenne = float(df_satisfaction["note_moyenne"].mean()) if not df_satisfaction.empty else 0

meilleure_region = (
    df_region.iloc[0]["customer_state"]
    if not df_region.empty else "N/A"
)

meilleure_categorie = (
    df_categorie.iloc[0]["product_category_name_english"]
    if not df_categorie.empty else "N/A"
)

# ==========================================================
# Header
# ==========================================================
st.markdown('<div class="main-title">📊 Dashboard Olist — Restitution Gold</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtitle">'
    'Suivi des performances commerciales, des délais de livraison et de la satisfaction '
    'client à partir des tables analytiques du schéma Gold.'
    '</div>',
    unsafe_allow_html=True
)

with st.expander("Contexte du projet", expanded=False):
    st.markdown(
        """
        Ce dashboard s'appuie sur l'architecture définie dans le DAT :
        - source PostgreSQL,
        - pipeline ETL Bronze → Silver → Gold,
        - agrégations Gold exploitées dans Streamlit,
        - indicateurs destinés à l'équipe Data Analytique.

        Les données affichées proviennent directement des 4 tables Gold du projet.
        """
    )

# ==========================================================
# Sidebar
# ==========================================================
st.sidebar.header("Options d'affichage")
top_n_categories = st.sidebar.slider("Top catégories", 5, 20, 10, 10)
top_n_vendeurs = st.sidebar.slider("Top vendeurs", 5, 20, 10, 10)

# ==========================================================
# KPI
# ==========================================================
st.markdown("## Vue d’ensemble")

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("CA total", f"{total_ca:,.2f}")
k2.metric("Nb commandes", f"{nb_commandes:,}")
k3.metric("CA moyen", f"{ca_moyen:,.2f}")
k4.metric("Délai moyen", f"{delai_moyen:.2f} jours")
k5.metric("Note moyenne", f"{note_moyenne:.2f} / 5")
k6.metric("Meilleure région", meilleure_region)

st.markdown(
    f"""
    <div class="info-box">
    <b>Lecture métier :</b> la région la plus contributrice au chiffre d'affaires est <b>{meilleure_region}</b>,
    tandis que la catégorie la plus génératrice de revenus est <b>{meilleure_categorie}</b>.
    </div>
    """,
    unsafe_allow_html=True
)

# ==========================================================
# Analyse commerciale
# ==========================================================
st.markdown("## Analyse commerciale")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Chiffre d'affaires par région")
    if not df_region.empty:
        fig_region = px.bar(
            df_region,
            x="customer_state",
            y="total_ca",
            color="total_ca",
            color_continuous_scale="Blues",
            labels={
                "customer_state": "État",
                "total_ca": "CA total"
            }
        )
        fig_region.update_layout(
            margin=dict(l=20, r=20, t=30, b=20),
            coloraxis_showscale=False
        )
        st.plotly_chart(fig_region, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible.")

with col2:
    st.subheader(f"Top {top_n_categories} catégories par CA")
    if not df_categorie.empty:
        fig_cat = px.bar(
            df_categorie.head(top_n_categories),
            x="product_category_name_english",
            y="total_ca",
            color="total_ca",
            color_continuous_scale="Tealgrn",
            labels={
                "product_category_name_english": "Catégorie",
                "total_ca": "CA total"
            }
        )
        fig_cat.update_layout(
            xaxis_tickangle=-40,
            margin=dict(l=20, r=20, t=30, b=20),
            coloraxis_showscale=False
        )
        st.plotly_chart(fig_cat, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible.")

# ==========================================================
# Logistique & satisfaction
# ==========================================================
st.markdown("## Logistique et satisfaction")

col3, col4 = st.columns(2)

with col3:
    st.subheader("Délai moyen de livraison par région")
    if not df_delai.empty:
        fig_delai = px.bar(
            df_delai,
            x="customer_state",
            y="delai_moyen_jours",
            color="delai_moyen_jours",
            color_continuous_scale="Oranges",
            labels={
                "customer_state": "État",
                "delai_moyen_jours": "Délai moyen (jours)"
            }
        )
        fig_delai.update_layout(
            margin=dict(l=20, r=20, t=30, b=20),
            coloraxis_showscale=False
        )
        st.plotly_chart(fig_delai, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible.")

with col4:
    st.subheader(f"Top {top_n_vendeurs} vendeurs par satisfaction")
    if not df_satisfaction.empty:
        fig_sat = px.bar(
            df_satisfaction.head(top_n_vendeurs),
            x="seller_id",
            y="note_moyenne",
            color="note_moyenne",
            color_continuous_scale="Purples",
            hover_data=["seller_state", "nb_avis", "nb_commandes"],
            labels={
                "seller_id": "Vendeur",
                "note_moyenne": "Note moyenne"
            }
        )
        fig_sat.update_layout(
            xaxis_tickangle=-40,
            margin=dict(l=20, r=20, t=30, b=20),
            coloraxis_showscale=False
        )
        st.plotly_chart(fig_sat, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible.")

# ==========================================================
# Détail des tables Gold
# ==========================================================
st.markdown("## Détail des tables Gold")

tab1, tab2, tab3, tab4 = st.tabs([
    "CA par région",
    "CA par catégorie",
    "Délais de livraison",
    "Satisfaction vendeurs"
])

with tab1:
    st.dataframe(df_region, use_container_width=True, hide_index=True)

with tab2:
    st.dataframe(df_categorie, use_container_width=True, hide_index=True)

with tab3:
    st.dataframe(df_delai, use_container_width=True, hide_index=True)

with tab4:
    st.dataframe(df_satisfaction, use_container_width=True, hide_index=True)

# ==========================================================
# Footer
# ==========================================================
st.markdown("---")
st.caption(
    "Dashboard Streamlit connecté aux tables Gold PostgreSQL du projet ETL Olist. "
    "Pipeline exécuté via Apache Spark et orchestré par Apache Airflow."
)