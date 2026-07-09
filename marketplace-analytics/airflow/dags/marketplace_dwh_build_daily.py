"""
DAG : marketplace_dwh_build_daily
----------------------------------
Partie 1 - Pipeline ELT de base (etape 5 du decoupage).

Construit les dimensions (seller, customer, product, date) et la table de
faits (fact_orders) a partir du schema staging, avec le meme pattern
DELETE + INSERT sur la partition dt = {{ ds }} pour fact_orders (idempotence).

Declenche automatiquement par l'Asset "raw_orders" produit par
marketplace_orders_ingest_daily (une fois la DQ passee).

Produit l'Asset "dwh_orders", qui declenche a son tour :
    - marketplace_analytics_aggregate_daily
    - marketplace_anomaly_detect_daily (option 2)
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

try:
    from airflow.sdk import Asset
except ImportError:  # pragma: no cover
    from airflow.datasets import Dataset as Asset

RAW_ORDERS_ASSET = Asset("s3://raw/marketplace/orders")
DWH_ORDERS_ASSET = Asset("postgres://postgres-dwh/dwh/dwh/fact_orders")

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="marketplace_dwh_build_daily",
    schedule=[RAW_ORDERS_ASSET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "elt", "dwh"],
)
def marketplace_dwh_build_daily():

    @task
    def build_dimensions() -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="postgres_dwh_default")
        conn = pg.get_conn()
        conn.autocommit = False
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO dwh.dim_seller (seller_id, name, country, joined_date)
                SELECT seller_id, name, country, joined_date FROM staging.sellers
                ON CONFLICT (seller_id) DO UPDATE SET
                    name = EXCLUDED.name, country = EXCLUDED.country, joined_date = EXCLUDED.joined_date
            """)
            cur.execute("""
                INSERT INTO dwh.dim_product (product_id, name, category, base_price)
                SELECT product_id, name, category, base_price FROM staging.products
                ON CONFLICT (product_id) DO UPDATE SET
                    name = EXCLUDED.name, category = EXCLUDED.category, base_price = EXCLUDED.base_price
            """)
            # dim_customer : on la peuple au fil de l'eau a partir des commandes recues
            # (l'API ne fournit pas de flux dedie aux clients ; on materialise ceux vus dans staging.orders)
            cur.execute("""
                INSERT INTO dwh.dim_customer (customer_id, email, city, signup_date)
                SELECT DISTINCT o.customer_id, NULL::VARCHAR, NULL::VARCHAR, NULL::DATE
                FROM staging.orders o
                LEFT JOIN dwh.dim_customer d ON o.customer_id = d.customer_id
                WHERE d.customer_id IS NULL
                ON CONFLICT (customer_id) DO NOTHING
            """)
            cur.execute("""
                INSERT INTO dwh.dim_date (dt, year, month, day_of_week)
                SELECT DISTINCT dt, EXTRACT(YEAR FROM dt)::INT, EXTRACT(MONTH FROM dt)::INT,
                       EXTRACT(DOW FROM dt)::INT
                FROM staging.orders
                ON CONFLICT (dt) DO NOTHING
            """)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
        return {"status": "dimensions_ok"}

    @task(outlets=[DWH_ORDERS_ASSET])
    def build_fact_orders(dims_result: dict) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="postgres_dwh_default")
        conn = pg.get_conn()
        conn.autocommit = False
        cur = conn.cursor()
        try:
            # Asset-triggered run: ds/logical_date is not meaningful, so the
            # target partition is the latest date actually loaded in staging.
            cur.execute("SELECT MAX(dt) FROM staging.orders")
            ds = cur.fetchone()[0]

            # Idempotence : on supprime d'abord la partition du jour avant de reinserer
            cur.execute("DELETE FROM dwh.fact_orders WHERE dt = %s", (ds,))
            cur.execute(
                """
                INSERT INTO dwh.fact_orders
                    (order_id, seller_id, customer_id, product_id, dt, quantity, total_amount, status)
                SELECT order_id, seller_id, customer_id, product_id, dt, quantity, total_amount, status
                FROM staging.orders
                WHERE dt = %s
                """,
                (ds,),
            )
            cur.execute("SELECT COUNT(*) FROM dwh.fact_orders WHERE dt = %s", (ds,))
            n = cur.fetchone()[0]
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
        return n

    build_fact_orders(build_dimensions())


marketplace_dwh_build_daily()
