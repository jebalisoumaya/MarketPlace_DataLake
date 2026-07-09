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
