# MarketPlace Analytics — Projet Airflow ELT

Projet final : pipeline ELT idempotent pour une marketplace e-commerce (API → MinIO → PostgreSQL → dashboard), avec Custom Hook, Custom Operator, modelisation dimensionnelle et detection d'anomalies (Option 2 : Streamlit).

## Architecture

```
API Marketplace (Flask)  --extract via Custom Hook-->  Airflow (DAGs)
                                                            |
                                            raw JSON        | load + transform
                                                            v
                                    MinIO (raw/dt=...)   PostgreSQL DWH (staging / dwh / analytics)
                                                            |
                                                            v
                                                    Dashboard Streamlit (KPIs + anomalies)
```

4 DAGs, chaines par des **Assets** (asset `raw_orders` puis `dwh_orders`) :

1. `marketplace_orders_ingest_daily` (`@daily`) : extrait (Custom Hook) → upload brut dans MinIO → charge en `staging` → 5 checks de qualite (Custom Operator)
2. `marketplace_dwh_build_daily` (declenche par l'asset `raw_orders`) : construit les dimensions et `fact_orders`
3. `marketplace_analytics_aggregate_daily` (declenche par l'asset `dwh_orders`) : construit les tables lues par Streamlit
4. `marketplace_anomaly_detect_daily` (declenche par l'asset `dwh_orders`) : moyenne mobile 7 jours par vendeur, flag les drops > 30 %, webhook simule si anomalie(s)

## Idempotence

- L'API simulee genere des commandes **deterministes par date** (seed = hash de la date) : rappeler `/orders?date=X` renvoie toujours exactement le meme resultat.
- Le chargement en base suit systematiquement le pattern **DELETE puis INSERT sur la partition `dt`** : relancer un DAG plusieurs fois pour la meme date ne cree jamais de doublon.

## Demarrer le projet

```bash
cp .env.example .env
docker compose up --build
```

- Airflow : http://localhost:8080 (admin / admin)
- Streamlit : http://localhost:8501
- MinIO console : http://localhost:9001 (minioadmin / minioadmin123)
- API marketplace : http://localhost:5000

Les connexions Airflow (`marketplace_api_default`, `postgres_dwh_default`, `minio_default`) sont injectees automatiquement via des variables d'environnement `AIRFLOW_CONN_*` dans `docker-compose.yml` : rien a configurer manuellement dans l'UI.

## Regles de qualite de donnees (Custom Operator)

Le `DataQualityOperator` execute 5 regles configurables sur `staging.orders` :

| Regle | Verifie que... |
|---|---|
| `not_null_order_id` | aucun `order_id` n'est NULL |
| `no_duplicate_orders` | aucun `order_id` n'est duplique |
| `positive_amount` | aucun montant n'est <= 0 |
| `valid_status` | tous les statuts sont dans la liste autorisee |
| `referential_seller` | chaque commande reference un vendeur existant |

## Detection d'anomalies

Pour chaque vendeur, on calcule la moyenne mobile du CA sur les 7 jours precedents (fenetre glissante, sans la journee du jour). Si le CA du jour chute de plus de 30 % par rapport a cette moyenne, le vendeur est ecrit dans `analytics.anomalies`. S'il y a au moins une anomalie, un webhook simule (`POST /webhook-echo` sur l'API marketplace) est appele via un `BranchPythonOperator`.

## Ce qui a ete reellement teste

Docker n'etant pas disponible dans l'environnement ou ce projet a ete prepare, l'integralite de la logique metier a ete testee **en dehors de Docker**, avec un vrai PostgreSQL local et l'API Flask reellement lancee (pas de mocks caches) :

- API simulee : idempotence verifiee (3 appels identiques -> meme reponse), authentification Bearer testee (rejet si mauvais token)
- `MarketplaceAPIHook` : testee en conditions reelles contre l'API (sellers, products, orders, retries, erreur 401)
- `DataQualityOperator` : testee contre un vrai Postgres, cas succes et cas d'echec (une ligne invalide injectee volontairement)
- Logique SQL des 4 DAGs (dimensions, faits, agregations, moyenne mobile / anomalies) : rejouee telle quelle contre 10 jours de donnees chargees via le hook — un vrai bug a d'ailleurs ete trouve et corrige a cette occasion (cast NULL manquant dans `dim_customer`)
- Dashboard Streamlit : lance reellement contre le Postgres de test, toutes les requetes verifiees (code HTTP 200, pas d'exception)
- Suite `pytest` (`tests/`) : 9 tests, hook (via un serveur Flask de test) + operator (via une connexion DB mockee)

**Ce qui n'a pas pu etre teste ici** (pas de Docker dans cet environnement) : le demarrage reel du stack `docker-compose` (Airflow 3.1.8 webserver/scheduler, MinIO, cablage des Assets entre DAGs, upload S3Hook vers MinIO). A verifier en premier lieu au premier `docker compose up --build` :
- la commande exacte du webserver Airflow 3.x (`api-server` a ete utilise dans le compose, a confirmer selon la doc officielle 3.1.8)
- l'extra de la connexion MinIO (`endpoint_url` encode dans l'URI) fonctionne avec `S3Hook`

## Lancer les tests

```bash
pip install pytest flask requests
pytest tests/ -v
```

## Pistes d'amelioration (bonus non implementes)

- Alerting Slack/webhook reel sur echec de DAG
- Backfill manuel sur 1 semaine d'historique
- Dimension `dim_category` enrichie avec un mapping externe
