"""
DAG : marketplace_anomaly_detect_daily
-----------------------------------------
Partie 2 - Analytics, option 2 (Streamlit + detection d'anomalies).

- Calcule la moyenne mobile 7 jours du CA par vendeur.
- Flag les vendeurs dont le CA du jour chute de plus de 30% par rapport
  a cette moyenne mobile.
- Ecrit les anomalies dans analytics.anomalies.
- Branching : si au moins une anomalie est detectee, appelle un webhook
  (simule) ; sinon, ne fait rien.

Declenche par l'Asset "dwh_orders" (le meme qui declenche les agregations),
puisque la detection d'anomalies a besoin des faits du jour pour comparer
a l'historique.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

try:
    from airflow.sdk import Asset
except ImportError:  # pragma: no cover
    from airflow.datasets import Dataset as Asset

DWH_ORDERS_ASSET = Asset("postgres://postgres-dwh/dwh/dwh/fact_orders")
DROP_THRESHOLD_PCT = 30.0  # % de chute vs moyenne mobile 7 jours pour etre flag anomalie

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="marketplace_anomaly_detect_daily",
    schedule=[DWH_ORDERS_ASSET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "analytics", "anomaly-detection"],
)
def marketplace_anomaly_detect_daily():

    @task
    def detect_anomalies() -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="postgres_dwh_default")
        conn = pg.get_conn()
        conn.autocommit = False
        cur = conn.cursor()
        try:
            # Asset-triggered run: ds/logical_date is not meaningful, so the
            # target partition is the latest date actually loaded in fact_orders.
            cur.execute("SELECT MAX(dt) FROM dwh.fact_orders")
            ds = cur.fetchone()[0]

            cur.execute("DELETE FROM analytics.anomalies WHERE dt = %s", (ds,))
            cur.execute(
                """
                WITH daily AS (
                    SELECT f.seller_id, f.dt, SUM(f.total_amount) AS revenue
                    FROM dwh.fact_orders f
                    WHERE f.status != 'cancelled'
                      AND f.dt BETWEEN (%s::date - INTERVAL '7 days') AND %s::date
                    GROUP BY f.seller_id, f.dt
                ),
                with_avg AS (
                    SELECT
                        seller_id,
                        dt,
                        revenue,
                        AVG(revenue) OVER (
                            PARTITION BY seller_id
                            ORDER BY dt
                            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                        ) AS moving_avg_7d
                    FROM daily
                )
                INSERT INTO analytics.anomalies (dt, seller_id, seller_name, revenue, moving_avg_7d, drop_pct)
                SELECT
                    w.dt, w.seller_id, s.name, w.revenue, w.moving_avg_7d,
                    ROUND((100 * (w.moving_avg_7d - w.revenue) / NULLIF(w.moving_avg_7d, 0))::numeric, 2)
                FROM with_avg w
                JOIN dwh.dim_seller s ON s.seller_id = w.seller_id
                WHERE w.dt = %s
                  AND w.moving_avg_7d IS NOT NULL
                  AND w.moving_avg_7d > 0
                  AND (100 * (w.moving_avg_7d - w.revenue) / w.moving_avg_7d) > %s
                """,
                (ds, ds, ds, DROP_THRESHOLD_PCT),
            )
            cur.execute("SELECT COUNT(*) FROM analytics.anomalies WHERE dt = %s", (ds,))
            nb_anomalies = cur.fetchone()[0]
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

        return nb_anomalies

    @task.branch
    def branch_on_anomalies(nb_anomalies: int) -> str:
        return "call_webhook" if nb_anomalies > 0 else "no_anomaly"

    @task
    def call_webhook(ds: str | None = None) -> None:
        """Webhook simule (endpoint /webhook-echo de l'API marketplace) :
        dans un contexte reel, on notifierait Slack/Teams/PagerDuty."""
        import logging
        import requests

        logger = logging.getLogger("airflow.task")
        try:
            requests.post(
                "http://api-simulee:5000/webhook-echo",
                json={"event": "anomaly_detected", "dt": ds},
                timeout=5,
            )
        except Exception as exc:  # webhook de demo, on logue sans faire echouer le DAG
            logger.warning("Webhook indisponible (simulation) : %s", exc)

    no_anomaly = EmptyOperator(task_id="no_anomaly")

    n = detect_anomalies()
    branch_on_anomalies(n) >> [call_webhook(), no_anomaly]


marketplace_anomaly_detect_daily()
