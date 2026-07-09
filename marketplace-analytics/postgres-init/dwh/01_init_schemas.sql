-- Schema STAGING : donnees brutes chargees telles quelles depuis MinIO,
-- juste typees. Une ligne = un enregistrement source, sans transformation.

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id     VARCHAR(20) PRIMARY KEY,
    name          VARCHAR(200),
    country       VARCHAR(100),
    joined_date   DATE
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id    VARCHAR(20) PRIMARY KEY,
    name          VARCHAR(200),
    category      VARCHAR(100),
    base_price    NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id   VARCHAR(20) PRIMARY KEY,
    email         VARCHAR(255),
    city          VARCHAR(100),
    signup_date   DATE
);

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id      VARCHAR(40) PRIMARY KEY,
    seller_id     VARCHAR(20),
    customer_id   VARCHAR(20),
    product_id    VARCHAR(20),
    dt            DATE,
    quantity      INT,
    total_amount  NUMERIC(12, 2),
    status        VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_staging_orders_dt ON staging.orders (dt);
-- Schema DWH : modelisation dimensionnelle (etoile), prete pour l'analytique.

CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_id     VARCHAR(20) PRIMARY KEY,
    name          VARCHAR(200),
    country       VARCHAR(100),
    joined_date   DATE
);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_id   VARCHAR(20) PRIMARY KEY,
    email         VARCHAR(255),
    city          VARCHAR(100),
    signup_date   DATE
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id    VARCHAR(20) PRIMARY KEY,
    name          VARCHAR(200),
    category      VARCHAR(100),
    base_price    NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    dt            DATE PRIMARY KEY,
    year          INT,
    month         INT,
    day_of_week   INT
);

CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id      VARCHAR(40) PRIMARY KEY,
    seller_id     VARCHAR(20) REFERENCES dwh.dim_seller(seller_id),
    customer_id   VARCHAR(20) REFERENCES dwh.dim_customer(customer_id),
    product_id    VARCHAR(20) REFERENCES dwh.dim_product(product_id),
    dt            DATE REFERENCES dwh.dim_date(dt),
    quantity      INT,
    total_amount  NUMERIC(12, 2),
    status        VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_dt ON dwh.fact_orders (dt);
CREATE INDEX IF NOT EXISTS idx_fact_orders_seller ON dwh.fact_orders (seller_id);
-- Schema ANALYTICS : tables d'agregation pretes a etre lues par le dashboard,
-- + table des anomalies detectees.

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.top_sellers (
    dt              DATE,
    seller_id       VARCHAR(20),
    seller_name     VARCHAR(200),
    revenue         NUMERIC(14, 2),
    nb_orders       INT,
    PRIMARY KEY (dt, seller_id)
);

CREATE TABLE IF NOT EXISTS analytics.daily_revenue (
    dt              DATE PRIMARY KEY,
    revenue         NUMERIC(14, 2),
    nb_orders       INT
);

CREATE TABLE IF NOT EXISTS analytics.category_breakdown (
    dt              DATE,
    category        VARCHAR(100),
    revenue         NUMERIC(14, 2),
    nb_orders       INT,
    PRIMARY KEY (dt, category)
);

CREATE TABLE IF NOT EXISTS analytics.customer_activity (
    dt                  DATE,
    active_customers    INT,
    dormant_customers   INT,
    PRIMARY KEY (dt)
);

CREATE TABLE IF NOT EXISTS analytics.anomalies (
    id              SERIAL PRIMARY KEY,
    dt              DATE,
    seller_id       VARCHAR(20),
    seller_name     VARCHAR(200),
    revenue         NUMERIC(14, 2),
    moving_avg_7d   NUMERIC(14, 2),
    drop_pct        NUMERIC(6, 2),
    detected_at     TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_dt ON analytics.anomalies (dt);
