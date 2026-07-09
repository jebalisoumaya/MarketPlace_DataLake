"""
API Marketplace simulee (Flask)
-------------------------------
Simule une marketplace e-commerce : vendeurs, produits, commandes.

Points importants pour l'idempotence du pipeline :
- Les commandes d'une date donnee sont generees de facon DETERMINISTE
  (le generateur aleatoire est "seede" a partir de la date). Rappeler
  GET /orders?date=2024-01-15 plusieurs fois renvoie donc toujours
  exactement le meme resultat.
- L'authentification se fait par Bearer token (variable d'env API_TOKEN).
"""
import hashlib
import os
import random
from datetime import date, datetime, timedelta
from functools import wraps

from flask import Flask, jsonify, request

app = Flask(__name__)

API_TOKEN = os.environ.get("API_TOKEN", "dev-secret-token")

# ---------------------------------------------------------------------------
# Referentiels (vendeurs / produits) : generes une fois au demarrage,
# avec une seed fixe -> toujours les memes vendeurs/produits.
# ---------------------------------------------------------------------------
_rng_ref = random.Random(42)

COUNTRIES = ["France", "Allemagne", "Espagne", "Italie", "Belgique", "Portugal"]
CATEGORIES = ["Mode", "Electronique", "Maison", "Sport", "Beaute", "Livres"]
CITIES = ["Paris", "Lyon", "Marseille", "Lille", "Bordeaux", "Nantes", "Toulouse"]
STATUSES = ["completed", "completed", "completed", "pending", "cancelled", "refunded"]

N_SELLERS = 40
N_PRODUCTS = 200
N_CUSTOMERS = 500


def _gen_sellers():
    sellers = []
    for i in range(1, N_SELLERS + 1):
        joined = date(2020, 1, 1) + timedelta(days=_rng_ref.randint(0, 1400))
        sellers.append({
            "seller_id": f"SEL{i:04d}",
            "name": f"Boutique {i}",
            "country": _rng_ref.choice(COUNTRIES),
            "joined_date": joined.isoformat(),
        })
    return sellers


def _gen_products():
    products = []
    for i in range(1, N_PRODUCTS + 1):
        products.append({
            "product_id": f"PRD{i:05d}",
            "name": f"Produit {i}",
            "category": _rng_ref.choice(CATEGORIES),
            "base_price": round(_rng_ref.uniform(5, 250), 2),
        })
    return products


def _gen_customers():
    customers = []
    for i in range(1, N_CUSTOMERS + 1):
        signup = date(2019, 6, 1) + timedelta(days=_rng_ref.randint(0, 1800))
        customers.append({
            "customer_id": f"CUS{i:05d}",
            "email": f"client{i}@example.com",
            "city": _rng_ref.choice(CITIES),
            "signup_date": signup.isoformat(),
        })
    return customers


SELLERS = _gen_sellers()
PRODUCTS = _gen_products()
CUSTOMERS = _gen_customers()


def _seed_for_date(d: str) -> int:
    """Transforme une date (str) en entier deterministe pour seeder le RNG."""
    h = hashlib.sha256(d.encode("utf-8")).hexdigest()
    return int(h[:8], 16)


def require_bearer_token(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or auth.split(" ", 1)[1] != API_TOKEN:
            return jsonify({"error": "unauthorized"}), 401
        return fn(*args, **kwargs)
    return wrapper


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/sellers")
@require_bearer_token
def get_sellers():
    return jsonify(SELLERS)


@app.route("/products")
@require_bearer_token
def get_products():
    return jsonify(PRODUCTS)


@app.route("/orders")
@require_bearer_token
def get_orders():
    d = request.args.get("date")
    if not d:
        return jsonify({"error": "missing 'date' query param, format YYYY-MM-DD"}), 400
    try:
        datetime.strptime(d, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "invalid date format, expected YYYY-MM-DD"}), 400

    rng = random.Random(_seed_for_date(d))
    n_orders = rng.randint(150, 400)

    # Simule quelques "anomalies" volontaires pour tester la detection :
    # certains vendeurs ont un jour "creux" (peu ou pas de commandes).
    drop_sellers = set(rng.sample([s["seller_id"] for s in SELLERS], k=3))

    orders = []
    for i in range(n_orders):
        seller = rng.choice(SELLERS)
        if seller["seller_id"] in drop_sellers and rng.random() < 0.85:
            continue  # simule un drop de ventes pour ce vendeur ce jour-la
        product = rng.choice(PRODUCTS)
        customer = rng.choice(CUSTOMERS)
        quantity = rng.randint(1, 5)
        price = product["base_price"] * rng.uniform(0.9, 1.15)
        # quelques prix suspects volontaires (anomalie de prix)
        if rng.random() < 0.01:
            price = price * rng.choice([0.05, 8.0])
        orders.append({
            "order_id": f"ORD{d.replace('-', '')}{i:05d}",
            "seller_id": seller["seller_id"],
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "dt": d,
            "quantity": quantity,
            "total_amount": round(price * quantity, 2),
            "status": rng.choice(STATUSES),
        })
    return jsonify(orders)


@app.route("/webhook-echo", methods=["POST"])
def webhook_echo():
    """Webhook simule pour la notification d'anomalies (DAG marketplace_anomaly_detect_daily)."""
    payload = request.get_json(silent=True) or {}
    app.logger.info("Webhook recu : %s", payload)
    return jsonify({"received": True, "payload": payload})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
