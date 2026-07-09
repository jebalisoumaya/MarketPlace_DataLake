"""
Tests du Custom Operator DataQualityOperator, avec une base de donnees
mockee (aucune connexion reelle a Postgres n'est necessaire).

Lancer avec : pytest tests/test_data_quality_operator.py -v
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "airflow" / "plugins" / "operators"))

from data_quality_operator import DataQualityOperator, AirflowException  # noqa: E402


def make_mock_connection(fetchone_values):
    """Cree une fausse connexion DB-API dont chaque appel a cursor().execute()
    puis fetchone() renvoie successivement les valeurs de fetchone_values."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(v,) for v in fetchone_values]
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn


def test_all_rules_pass():
    conn = make_mock_connection([0, 0, 0])
    rules = [
        {"name": "rule_a", "sql": "SELECT 1", "expected": 0},
        {"name": "rule_b", "sql": "SELECT 2", "expected": 0},
        {"name": "rule_c", "sql": "SELECT 3", "expected": 0},
    ]
    op = DataQualityOperator(task_id="dq", rules=rules, connection=conn)
    result = op.execute()
    assert all(r["passed"] for r in result["results"])


def test_one_rule_fails_raises_exception():
    conn = make_mock_connection([0, 5, 0])  # la 2e regle echoue (5 != 0)
    rules = [
        {"name": "rule_a", "sql": "SELECT 1", "expected": 0},
        {"name": "rule_b", "sql": "SELECT 2", "expected": 0},
        {"name": "rule_c", "sql": "SELECT 3", "expected": 0},
    ]
    op = DataQualityOperator(task_id="dq", rules=rules, connection=conn)
    with pytest.raises(AirflowException) as exc_info:
        op.execute()
    assert "rule_b" in str(exc_info.value)


def test_custom_comparison_operator():
    # Regle : le nombre de lignes doit etre >= 100 (pas juste == 0)
    conn = make_mock_connection([150])
    rules = [{"name": "min_rows", "sql": "SELECT COUNT(*) ...", "expected": 100, "comparison": ">="}]
    op = DataQualityOperator(task_id="dq", rules=rules, connection=conn)
    result = op.execute()
    assert result["results"][0]["passed"] is True


def test_fail_on_first_error_stops_early():
    conn = make_mock_connection([10])  # une seule valeur : si on s'arrete bien apres la 1ere regle
    rules = [
        {"name": "rule_a", "sql": "SELECT 1", "expected": 0},
        {"name": "rule_b", "sql": "SELECT 2", "expected": 0},
    ]
    op = DataQualityOperator(task_id="dq", rules=rules, connection=conn, fail_on_first_error=True)
    with pytest.raises(AirflowException):
        op.execute()
    # Un seul execute() a du avoir lieu puisqu'on s'arrete au premier echec
    assert conn.cursor.return_value.__enter__.return_value.execute.call_count == 1
