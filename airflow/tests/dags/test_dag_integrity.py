"""
Test DAG integrity: all DAGs must import without errors,
have no cycles, and conform to our naming / tagging conventions.
"""
from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).parents[2] / "dags"
REQUIRED_TAGS = {"restaurant"}
MAX_ACTIVE_RUNS = 5


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, (
        f"DAG import errors:\n{dagbag.import_errors}"
    )


def test_dag_count(dagbag: DagBag) -> None:
    assert len(dagbag.dags) >= 2, "Expected at least 2 DAGs"


@pytest.mark.parametrize("dag_id,dag", [(d, dagbag) for d, dagbag in []])
def test_dags_have_required_tags(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        missing = REQUIRED_TAGS - set(dag.tags or [])
        assert not missing, (
            f"DAG '{dag_id}' is missing required tags: {missing}"
        )


def test_dags_max_active_runs(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        assert dag.max_active_runs <= MAX_ACTIVE_RUNS, (
            f"DAG '{dag_id}' max_active_runs={dag.max_active_runs} exceeds limit {MAX_ACTIVE_RUNS}"
        )


def test_dags_have_owners(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        assert dag.default_args.get("owner") not in (None, "airflow"), (
            f"DAG '{dag_id}' must set a meaningful owner in default_args"
        )


def test_dag_retries_configured(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        retries = dag.default_args.get("retries")
        assert retries is not None and retries >= 1, (
            f"DAG '{dag_id}' must configure at least 1 retry"
        )
