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
