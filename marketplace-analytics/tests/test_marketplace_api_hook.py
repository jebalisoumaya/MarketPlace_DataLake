"""
Tests du Custom Hook MarketplaceAPIHook.

Ces tests demarrent l'API Flask simulee dans un thread local et verifient :
    - l'authentification Bearer (rejet si mauvais token)
    - l'idempotence (meme date -> meme resultat)
    - le contenu des reponses (sellers / products / orders)

Lancer avec : pytest tests/test_marketplace_api_hook.py -v
"""
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "plugins" / "hooks"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "api-simulee"))

from marketplace_api_hook import MarketplaceAPIHook  # noqa: E402

TEST_PORT = 5099
TEST_TOKEN = "test-token-123"


@pytest.fixture(scope="module", autouse=True)
def api_server():
    import os
    os.environ["API_TOKEN"] = TEST_TOKEN
    import app as flask_app_module  # importe apres avoir positionne API_TOKEN

    server_thread = threading.Thread(
        target=lambda: flask_app_module.app.run(host="127.0.0.1", port=TEST_PORT, use_reloader=False),
        daemon=True,
    )
    server_thread.start()
    time.sleep(1.5)
    yield
    # daemon thread : se termine avec le process de test


@pytest.fixture
def hook():
    return MarketplaceAPIHook(base_url=f"http://127.0.0.1:{TEST_PORT}", token=TEST_TOKEN)


def test_get_sellers(hook):
    sellers = hook.get_sellers()
    assert len(sellers) == 40
    assert {"seller_id", "name", "country", "joined_date"} <= sellers[0].keys()


def test_get_products(hook):
    products = hook.get_products()
    assert len(products) == 200


def test_get_orders_idempotent(hook):
    orders1 = hook.get_orders("2024-03-01")
    orders2 = hook.get_orders("2024-03-01")
    assert orders1 == orders2
    assert len(orders1) > 0


def test_get_orders_different_dates_differ(hook):
    orders_a = hook.get_orders("2024-03-01")
    orders_b = hook.get_orders("2024-03-02")
    assert orders_a != orders_b


def test_wrong_token_raises_permission_error():
    bad_hook = MarketplaceAPIHook(base_url=f"http://127.0.0.1:{TEST_PORT}", token="wrong-token")
    with pytest.raises(PermissionError):
        bad_hook.get_orders("2024-03-01")
