"""
Dashboard Streamlit - MarketPlace Analytics
--------------------------------------------
Affiche les KPIs business (top vendeurs, CA, categories, clients actifs/
dormants) ainsi que les anomalies detectees par le DAG
marketplace_anomaly_detect_daily.

Se connecte directement au Postgres DWH (schema analytics), alimente par
les DAGs Airflow.
"""
import os
from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="MarketPlace Analytics", layout="wide")

DB_CONFIG = {
    "host": os.environ.get("DWH_HOST", "postgres-dwh"),
    "port": int(os.environ.get("DWH_PORT", 5432)),
    "dbname": os.environ.get("DWH_DBNAME", "dwh"),
    "user": os.environ.get("DWH_USER", "dwh_user"),
    "password": os.environ.get("DWH_PASSWORD", "dwh_password"),
}

# Categorical palette (fixed hue order, validated for CVD-safe adjacency).
CATEGORY_PALETTE = [
    "#3987e5",  # blue
    "#199e70",  # aqua
    "#c98500",  # yellow
    "#008300",  # green
    "#9085e9",  # violet
    "#e66767",  # red
    "#d55181",  # magenta
    "#d95926",  # orange
]
SEQUENTIAL_BLUE = "#3987e5"

CUSTOM_CSS = """
<style>
    .block-container { padding-top: 2.5rem; padding-bottom: 3rem; padding-left: 3rem; padding-right: 3rem; max-width: 100%; }
    [data-testid="stMetric"] {
        background-color: var(--secondary-background-color);
        border: 1px solid rgba(128, 128, 128, 0.18);
        border-radius: 12px;
        padding: 1.1rem 1.3rem;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.8rem;
        font-weight: 600;
        opacity: 0.7;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    [data-testid="stMetricValue"] { font-size: 1.9rem; font-weight: 700; }
    h1 { font-weight: 700; letter-spacing: -0.02em; }
    h2, h3 { font-weight: 600; letter-spacing: -0.01em; margin-top: 0.4rem; }
    [data-testid="stCaptionContainer"] { opacity: 0.7; }
    hr { margin: 2.2rem 0; opacity: 0.25; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


@st.cache_resource
def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(url)


def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


st.title("MarketPlace Analytics")
st.caption("Dashboard alimenté par les DAGs Airflow (schema `analytics`, Postgres DWH)")

# ---------------------------------------------------------------------
# Selecteur de date
# ---------------------------------------------------------------------
max_date_df = run_query("SELECT MAX(dt) AS max_dt FROM analytics.daily_revenue")
default_date = max_date_df["max_dt"].iloc[0] if not max_date_df.empty and max_date_df["max_dt"].iloc[0] else date.today()

selected_date = st.sidebar.date_input("Date analysée", value=default_date)
st.sidebar.caption("Les DAGs tournent en `@daily` : choisissez une date déjà traitée par le pipeline.")

params = {"dt": selected_date}

# ---------------------------------------------------------------------
# KPIs du jour
# ---------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

rev_df = run_query("SELECT revenue, nb_orders FROM analytics.daily_revenue WHERE dt = :dt", params)
if not rev_df.empty:
    col1.metric("Chiffre d'affaires du jour", f"{rev_df['revenue'].iloc[0]:,.2f} €")
    col2.metric("Commandes du jour", f"{int(rev_df['nb_orders'].iloc[0])}")
else:
    col1.metric("Chiffre d'affaires du jour", "—")
    col2.metric("Commandes du jour", "—")

anomalies_count_df = run_query("SELECT COUNT(*) AS n FROM analytics.anomalies WHERE dt = :dt", params)
n_anomalies = int(anomalies_count_df["n"].iloc[0]) if not anomalies_count_df.empty else 0
col3.metric("Anomalies détectées", n_anomalies)

st.divider()

# ---------------------------------------------------------------------
# Evolution du CA (7 derniers jours)
# ---------------------------------------------------------------------
st.subheader("Évolution du chiffre d'affaires (7 derniers jours)")
week_start = selected_date - timedelta(days=6)
evolution_df = run_query(
    "SELECT dt, revenue FROM analytics.daily_revenue WHERE dt BETWEEN :start AND :dt ORDER BY dt",
    {"start": week_start, "dt": selected_date},
)
if not evolution_df.empty:
    line_chart = (
        alt.Chart(evolution_df)
        .mark_line(color=SEQUENTIAL_BLUE, strokeWidth=2.5, point=alt.OverlayMarkDef(color=SEQUENTIAL_BLUE, size=50))
        .encode(
            x=alt.X("dt:T", title=None, axis=alt.Axis(format="%d %b", grid=False)),
            y=alt.Y("revenue:Q", title="Chiffre d'affaires (€)"),
            tooltip=[
                alt.Tooltip("dt:T", title="Date", format="%d %b %Y"),
                alt.Tooltip("revenue:Q", title="CA (€)", format=",.2f"),
            ],
        )
        .properties(height=280)
    )
    st.altair_chart(line_chart, use_container_width=True, theme="streamlit")
else:
    st.info("Pas encore de données sur cette période.")

col_left, col_right = st.columns(2)

# ---------------------------------------------------------------------
# Top vendeurs
# ---------------------------------------------------------------------
with col_left:
    st.subheader("Top 10 vendeurs par revenu")
    top_sellers_df = run_query(
        """SELECT seller_name AS "Vendeur", revenue AS "Revenu (€)", nb_orders AS "Commandes"
           FROM analytics.top_sellers WHERE dt = :dt ORDER BY revenue DESC LIMIT 10""",
        params,
    )
    if not top_sellers_df.empty:
        st.dataframe(top_sellers_df, use_container_width=True, hide_index=True)
    else:
        st.info("Aucune donnée pour cette date.")

# ---------------------------------------------------------------------
# Repartition par categorie
# ---------------------------------------------------------------------
with col_right:
    st.subheader("Répartition des ventes par catégorie")
    category_df = run_query(
        "SELECT category, revenue FROM analytics.category_breakdown WHERE dt = :dt ORDER BY revenue DESC",
        params,
    )
    if not category_df.empty:
        categories_sorted = sorted(category_df["category"].unique())
        color_scale = alt.Scale(domain=categories_sorted, range=CATEGORY_PALETTE[: len(categories_sorted)])
        bar_chart = (
            alt.Chart(category_df)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("category:N", title=None, sort="-y", axis=alt.Axis(labelAngle=-30)),
                y=alt.Y("revenue:Q", title="Chiffre d'affaires (€)"),
                color=alt.Color("category:N", scale=color_scale, legend=None),
                tooltip=[
                    alt.Tooltip("category:N", title="Catégorie"),
                    alt.Tooltip("revenue:Q", title="CA (€)", format=",.2f"),
                ],
            )
            .properties(height=280)
        )
        st.altair_chart(bar_chart, use_container_width=True, theme="streamlit")
    else:
        st.info("Aucune donnée pour cette date.")

st.divider()

# ---------------------------------------------------------------------
# Clients actifs vs dormants
# ---------------------------------------------------------------------
st.subheader("Clients actifs vs dormants (fenêtre glissante 30 jours)")
activity_df = run_query(
    "SELECT active_customers, dormant_customers FROM analytics.customer_activity WHERE dt = :dt",
    params,
)
if not activity_df.empty:
    a, d = int(activity_df["active_customers"].iloc[0]), int(activity_df["dormant_customers"].iloc[0])
    act_col1, act_col2 = st.columns(2)
    act_col1.metric("Clients actifs", a)
    act_col2.metric("Clients dormants", d)
    st.progress(a / max(a + d, 1))
else:
    st.info("Aucune donnée pour cette date.")

st.divider()

# ---------------------------------------------------------------------
# Anomalies detectees
# ---------------------------------------------------------------------
st.subheader("Anomalies détectées (drop de CA > 30% vs moyenne mobile 7 jours)")
anomalies_df = run_query(
    """SELECT seller_name AS "Vendeur", revenue AS "CA du jour (€)",
              moving_avg_7d AS "Moyenne mobile 7j (€)", drop_pct AS "Chute (%)"
       FROM analytics.anomalies WHERE dt = :dt ORDER BY drop_pct DESC""",
    params,
)
if not anomalies_df.empty:
    st.dataframe(anomalies_df, use_container_width=True, hide_index=True)
    st.warning(f"{len(anomalies_df)} vendeur(s) avec une chute de ventes anormale ce jour-là.")
else:
    st.success("Aucune anomalie détectée pour cette date.")