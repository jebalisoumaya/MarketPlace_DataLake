"""
MarketplaceAPIHook
-------------------
Hook Airflow personnalise pour dialoguer avec l'API de la marketplace
(authentification par Bearer token).

Utilisation attendue dans un DAG :

    hook = MarketplaceAPIHook(marketplace_conn_id="marketplace_api_default")
    orders = hook.get_orders("2024-01-15")
    sellers = hook.get_sellers()
    products = hook.get_products()

La connexion Airflow "marketplace_api_default" doit etre configuree avec :
    - Host : nom du service (ex: http://api-simulee)
    - Port : 5000
    - Password (ou Extra->token) : le token Bearer

En dehors d'Airflow (tests unitaires, scripts locaux), le hook peut aussi
etre instancie directement avec base_url et token, sans passer par une
Connection Airflow.
"""
from __future__ import annotations

import time
from typing import Any

import requests

try:
    from airflow.hooks.base import BaseHook
except ImportError:  # pragma: no cover - permet de tester hors environnement Airflow
    class BaseHook:  # type: ignore
        def get_connection(self, conn_id):
            raise NotImplementedError(
                "airflow n'est pas installe : passez base_url et token explicitement."
            )


class MarketplaceAPIHook(BaseHook):
    """Hook pour l'API Marketplace (auth Bearer)."""

    conn_name_attr = "marketplace_conn_id"
    default_conn_name = "marketplace_api_default"
    conn_type = "http"
    hook_name = "Marketplace API"

    def __init__(
        self,
        marketplace_conn_id: str = default_conn_name,
        base_url: str | None = None,
        token: str | None = None,
        timeout: int = 15,
        max_retries: int = 3,
        backoff_seconds: float = 1.5,
    ):
        super().__init__()
        self.marketplace_conn_id = marketplace_conn_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self._base_url = base_url
        self._token = token
        self._session: requests.Session | None = None

    # ------------------------------------------------------------------
    # Resolution de la connexion (Airflow, ou explicite pour les tests)
    # ------------------------------------------------------------------
    def _resolve_connection(self) -> tuple[str, str]:
        if self._base_url and self._token:
            return self._base_url, self._token

        conn = self.get_connection(self.marketplace_conn_id)
        scheme = "https" if conn.extra_dejson.get("use_ssl") else "http"
        host = conn.host or "localhost"
        base_url = f"{scheme}://{host}"
        if conn.port:
            base_url += f":{conn.port}"
        token = conn.password or conn.extra_dejson.get("token")
        if not token:
            raise ValueError(
                f"Aucun token trouve pour la connexion '{self.marketplace_conn_id}' "
                "(attendu dans le mot de passe ou dans extra.token)."
            )
        return base_url, token

    def get_conn(self) -> requests.Session:
        if self._session is None:
            base_url, token = self._resolve_connection()
            self._base_url = base_url
            self._token = token
            session = requests.Session()
            session.headers.update({"Authorization": f"Bearer {token}"})
            self._session = session
        return self._session

    # ------------------------------------------------------------------
    # Requete generique avec retries + backoff
    # ------------------------------------------------------------------
    def _request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        session = self.get_conn()
        url = f"{self._base_url}{path}"
        last_exc: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = session.get(url, params=params, timeout=self.timeout)
                if response.status_code == 401:
                    raise PermissionError(
                        f"Authentification refusee par l'API marketplace ({url})."
                    )
                response.raise_for_status()
                return response.json()
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    time.sleep(self.backoff_seconds * attempt)
                    continue
                raise ConnectionError(
                    f"Impossible de joindre l'API marketplace apres {self.max_retries} tentatives : {exc}"
                ) from exc

        raise last_exc  # pragma: no cover - defensif, ne devrait pas arriver

    # ------------------------------------------------------------------
    # Methodes metier
    # ------------------------------------------------------------------
    def get_orders(self, order_date: str) -> list[dict]:
        """Recupere les commandes d'une date donnee (format YYYY-MM-DD)."""
        return self._request("/orders", params={"date": order_date})

    def get_sellers(self) -> list[dict]:
        """Recupere la liste complete des vendeurs."""
        return self._request("/sellers")

    def get_products(self) -> list[dict]:
        """Recupere le catalogue complet des produits."""
        return self._request("/products")
