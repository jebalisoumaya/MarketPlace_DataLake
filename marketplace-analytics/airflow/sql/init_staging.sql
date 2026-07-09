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
