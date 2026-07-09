"""
DataQualityOperator
--------------------
Operator Airflow personnalise qui execute une liste de regles SQL de
qualite de donnees contre une base Postgres, et fait echouer la tache
si une regle n'est pas respectee.

Chaque regle est un dict :
    {
        "name": "no_null_order_id",
        "sql": "SELECT COUNT(*) FROM staging.orders WHERE dt = '{{ ds }}' AND order_id IS NULL",
        "expected": 0,               # valeur attendue
        "comparison": "==",          # "==", "<=", ">=", "<", ">"
    }

Le SQL peut contenir un template Jinja {{ ds }} qui sera resolu par
Airflow au moment de l'execution de la tache (comme pour les autres
operators). En dehors d'Airflow (tests unitaires), le SQL doit deja
etre complet (pas de {{ ds }} a resoudre).
"""
from __future__ import annotations

import operator as py_operator
from typing import Any

try:
    from airflow.models import BaseOperator
    from airflow.exceptions import AirflowException
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:  # pragma: no cover - permet de tester hors environnement Airflow
    class BaseOperator:  # type: ignore
        template_fields = ()

        def __init__(self, task_id: str = "", **kwargs):
            self.task_id = task_id

    class AirflowException(Exception):
        pass

    PostgresHook = None  # type: ignore


COMPARISONS = {
    "==": py_operator.eq,
    "!=": py_operator.ne,
    "<=": py_operator.le,
    ">=": py_operator.ge,
    "<": py_operator.lt,
    ">": py_operator.gt,
}


class DataQualityOperator(BaseOperator):
    """Execute des regles SQL de qualite de donnees et echoue si l'une d'elles ne passe pas."""

    template_fields = ("rules",)

    def __init__(
        self,
        rules: list[dict[str, Any]],
        postgres_conn_id: str = "postgres_dwh_default",
        connection=None,  # permet d'injecter une connexion DB-API deja ouverte (tests, ou hors Airflow)
        fail_on_first_error: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.rules = rules
        self.postgres_conn_id = postgres_conn_id
        self._connection = connection
        self.fail_on_first_error = fail_on_first_error

    def _get_connection(self):
        if self._connection is not None:
            return self._connection
        if PostgresHook is None:
            raise RuntimeError(
                "airflow-providers-postgres n'est pas installe : "
                "passez une connexion explicite via l'argument 'connection' pour tester hors Airflow."
            )
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        return hook.get_conn()

    def execute(self, context: dict | None = None) -> dict[str, Any]:
        conn = self._get_connection()
        results = []
        failures = []

        for rule in self.rules:
            name = rule["name"]
            sql = rule["sql"]
            expected = rule.get("expected", 0)
            comparison = rule.get("comparison", "==")
            cmp_fn = COMPARISONS[comparison]

            with conn.cursor() as cur:
                cur.execute(sql)
                row = cur.fetchone()
                actual = row[0] if row else None

            passed = cmp_fn(actual, expected)
            results.append({"name": name, "actual": actual, "expected": expected, "comparison": comparison, "passed": passed})

            self.log_result(name, actual, expected, comparison, passed)

            if not passed:
                failures.append(f"[{name}] valeur={actual} attendu {comparison} {expected}")
                if self.fail_on_first_error:
                    break

        if failures:
            raise AirflowException(
                "Data Quality check(s) failed :\n" + "\n".join(failures)
            )

        return {"results": results}

    def log_result(self, name, actual, expected, comparison, passed):
        status = "PASS" if passed else "FAIL"
        message = f"[DataQuality] {status} - {name} : valeur={actual} (attendu {comparison} {expected})"
        if hasattr(self, "log"):
            self.log.info(message)
        else:  # pragma: no cover
            print(message)
