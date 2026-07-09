"""
DAG : marketplace_analytics_aggregate_daily
---------------------------------------------
Partie 2 - Analytics (option 2 : Streamlit).

Construit les tables d'agregation lues par le dashboard Streamlit :
    - analytics.top_sellers        : revenu et nb commandes par vendeur et par jour
    - analytics.daily_revenue      : chiffre d'affaires du jour
    - analytics.category_breakdown : repartition des ventes par categorie
    - analytics.customer_activity  : nombre de clients actifs vs dormants

"Client actif" = a commande au cours des 30 derniers jours (glissant depuis ds).
"Client dormant" = deja vu dans l'historique, mais pas de commande sur les 30
derniers jours.

Declenche automatiquement par l'Asset "dwh_orders" produit par
marketplace_dwh_build_daily.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

try:
    from airflow.sdk import Asset
except ImportError:  # pragma: no cover
    from airflow.datasets import Dataset as Asset

DWH_ORDERS_ASSET = Asset("postgres://postgres-dwh/dwh/dwh/fact_orders")

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="marketplace_analytics_aggregate_daily",
    schedule=[DWH_ORDERS_ASSET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "analytics"],
)
def marketplace_analytics_aggregate_daily():

    @task
    def aggregate() -> dict:
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

            # -- Top vendeurs du jour --
            cur.execute("DELETE FROM analytics.top_sellers WHERE dt = %s", (ds,))
            cur.execute(
                """
                INSERT INTO analytics.top_sellers (dt, seller_id, seller_name, revenue, nb_orders)
                SELECT f.dt, f.seller_id, s.name, SUM(f.total_amount), COUNT(*)
                FROM dwh.fact_orders f
                JOIN dwh.dim_seller s ON f.seller_id = s.seller_id
                WHERE f.dt = %s AND f.status != 'cancelled'
                GROUP BY f.dt, f.seller_id, s.name
                """,
                (ds,),
            )

            # -- CA du jour --
            cur.execute("DELETE FROM analytics.daily_revenue WHERE dt = %s", (ds,))
            cur.execute(
                """
                INSERT INTO analytics.daily_revenue (dt, revenue, nb_orders)
                SELECT dt, COALESCE(SUM(total_amount), 0), COUNT(*)
                FROM dwh.fact_orders
                WHERE dt = %s AND status != 'cancelled'
                GROUP BY dt
                """,
                (ds,),
            )
            # S'assure d'avoir une ligne meme si aucune commande valide ce jour-la
            cur.execute(
                """
                INSERT INTO analytics.daily_revenue (dt, revenue, nb_orders)
                VALUES (%s, 0, 0)
                ON CONFLICT (dt) DO NOTHING
                """,
                (ds,),
            )

            # -- Repartition par categorie --
            cur.execute("DELETE FROM analytics.category_breakdown WHERE dt = %s", (ds,))
            cur.execute(
                """
                INSERT INTO analytics.category_breakdown (dt, category, revenue, nb_orders)
                SELECT f.dt, p.category, SUM(f.total_amount), COUNT(*)
                FROM dwh.fact_orders f
                JOIN dwh.dim_product p ON f.product_id = p.product_id
                WHERE f.dt = %s AND f.status != 'cancelled'
                GROUP BY f.dt, p.category
                """,
                (ds,),
            )

            # -- Clients actifs vs dormants (fenetre glissante de 30 jours) --
            cur.execute("DELETE FROM analytics.customer_activity WHERE dt = %s", (ds,))
            cur.execute(
                """
                WITH last_orders AS (
                    SELECT customer_id, MAX(dt) AS last_order_dt
                    FROM dwh.fact_orders
                    WHERE dt <= %s
                    GROUP BY customer_id
                )
                INSERT INTO analytics.customer_activity (dt, active_customers, dormant_customers)
                SELECT
                    %s,
                    COUNT(*) FILTER (WHERE last_order_dt >= (%s::date - INTERVAL '30 days')),
                    COUNT(*) FILTER (WHERE last_order_dt <  (%s::date - INTERVAL '30 days'))
                FROM last_orders
                """,
                (ds, ds, ds, ds),
            )

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

        return {"status": "aggregates_ok", "dt": ds}

    aggregate()


marketplace_analytics_aggregate_daily()
