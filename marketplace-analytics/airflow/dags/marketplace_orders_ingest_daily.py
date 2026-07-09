"""
DAG : marketplace_orders_ingest_daily
--------------------------------------
Partie 1 - Pipeline ELT de base (etape 1 a 4 du decoupage).

Chaine :
    1. Extract (Custom Hook API) : commandes, vendeurs, produits
    2. Upload raw JSON dans MinIO, partitionne par dt=
    3. Load staging PostgreSQL (pattern DELETE + INSERT sur la partition dt)
    4. DQ check (Custom Operator) avec 5 regles configurables

Idempotence : relancer ce DAG plusieurs fois pour la meme date logique (ds)
donne toujours le meme resultat, car :
    - l'API marketplace renvoie des donnees deterministes pour une date donnee
    - le chargement en staging supprime d'abord la partition dt=ds avant de
      reinserer (DELETE puis INSERT), donc pas de doublons en cas de retry.

Produit l'Asset "raw_orders", qui declenche le DAG suivant
(marketplace_dwh_build_daily).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

from airflow.decorators import dag, task

try:
    from airflow.sdk import Asset
except ImportError:  # pragma: no cover - compat Airflow < 3.0
    from airflow.datasets import Dataset as Asset

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "plugins", "hooks"))
from marketplace_api_hook import MarketplaceAPIHook  # noqa: E402

RAW_ORDERS_ASSET = Asset("s3://raw/marketplace/orders")

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="marketplace_orders_ingest_daily",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["marketplace", "elt", "ingestion"],
)
def marketplace_orders_ingest_daily():

    @task
    def extract_orders(ds: str | None = None) -> list[dict]:
        hook = MarketplaceAPIHook(marketplace_conn_id="marketplace_api_default")
        return hook.get_orders(ds)

    @task
    def extract_sellers() -> list[dict]:
        hook = MarketplaceAPIHook(marketplace_conn_id="marketplace_api_default")
        return hook.get_sellers()

    @task
    def extract_products() -> list[dict]:
        hook = MarketplaceAPIHook(marketplace_conn_id="marketplace_api_default")
        return hook.get_products()

    @task
    def upload_raw_to_minio(orders: list[dict], sellers: list[dict], products: list[dict], ds: str | None = None) -> dict:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3 = S3Hook(aws_conn_id="minio_default")
        bucket = "raw"
        base_key = f"marketplace/dt={ds}"

        s3.load_string(json.dumps(orders), key=f"{base_key}/orders.json", bucket_name=bucket, replace=True)
        s3.load_string(json.dumps(sellers), key=f"{base_key}/sellers.json", bucket_name=bucket, replace=True)
        s3.load_string(json.dumps(products), key=f"{base_key}/products.json", bucket_name=bucket, replace=True)

        return {"bucket": bucket, "key_prefix": base_key, "nb_orders": len(orders)}

    @task
    def load_staging(orders: list[dict], sellers: list[dict], products: list[dict], ds: str | None = None) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        pg = PostgresHook(postgres_conn_id="postgres_dwh_default")
        conn = pg.get_conn()
        conn.autocommit = False
        cur = conn.cursor()

        try:
            # Referentiels : upsert simple (ils changent peu, pas de notion de partition dt)
            cur.executemany(
                """INSERT INTO staging.sellers (seller_id, name, country, joined_date)
                   VALUES (%s,%s,%s,%s)
                   ON CONFLICT (seller_id) DO UPDATE SET name=EXCLUDED.name,
                       country=EXCLUDED.country, joined_date=EXCLUDED.joined_date""",
                [(s["seller_id"], s["name"], s["country"], s["joined_date"]) for s in sellers],
            )
            cur.executemany(
                """INSERT INTO staging.products (product_id, name, category, base_price)
                   VALUES (%s,%s,%s,%s)
                   ON CONFLICT (product_id) DO UPDATE SET name=EXCLUDED.name,
                       category=EXCLUDED.category, base_price=EXCLUDED.base_price""",
                [(p["product_id"], p["name"], p["category"], p["base_price"]) for p in products],
            )

            # Commandes : DELETE + INSERT sur la partition dt=ds -> idempotent
            cur.execute("DELETE FROM staging.orders WHERE dt = %s", (ds,))
            cur.executemany(
                """INSERT INTO staging.orders
                   (order_id, seller_id, customer_id, product_id, dt, quantity, total_amount, status)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                [(o["order_id"], o["seller_id"], o["customer_id"], o["product_id"],
                  o["dt"], o["quantity"], o["total_amount"], o["status"]) for o in orders],
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

        return len(orders)

    @task(outlets=[RAW_ORDERS_ASSET])
    def dq_check(ds: str | None = None) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import sys as _sys
        import os as _os

        _sys.path.append(_os.path.join(_os.path.dirname(__file__), "..", "plugins", "operators"))
        from data_quality_operator import DataQualityOperator  # noqa: E402

        rules = [
            {
                "name": "not_null_order_id",
                "sql": f"SELECT COUNT(*) FROM staging.orders WHERE dt = '{ds}' AND order_id IS NULL",
                "expected": 0,
            },
            {
                "name": "no_duplicate_orders",
                "sql": f"SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM staging.orders WHERE dt = '{ds}'",
                "expected": 0,
            },
            {
                "name": "positive_amount",
                "sql": f"SELECT COUNT(*) FROM staging.orders WHERE dt = '{ds}' AND total_amount <= 0",
                "expected": 0,
            },
            {
                "name": "valid_status",
                "sql": (
                    f"SELECT COUNT(*) FROM staging.orders WHERE dt = '{ds}' "
                    "AND status NOT IN ('completed','pending','cancelled','refunded')"
                ),
                "expected": 0,
            },
            {
                "name": "referential_seller",
                "sql": (
                    "SELECT COUNT(*) FROM staging.orders o "
                    "LEFT JOIN staging.sellers s ON o.seller_id = s.seller_id "
                    f"WHERE o.dt = '{ds}' AND s.seller_id IS NULL"
                ),
                "expected": 0,
            },
        ]

        pg = PostgresHook(postgres_conn_id="postgres_dwh_default")
        conn = pg.get_conn()
        op = DataQualityOperator(task_id="dq_check_inline", rules=rules, connection=conn)
        return op.execute()

    orders = extract_orders()
    sellers = extract_sellers()
    products = extract_products()

    uploaded = upload_raw_to_minio(orders, sellers, products)
    loaded = load_staging(orders, sellers, products)
    uploaded >> loaded >> dq_check()


marketplace_orders_ingest_daily()
