"""Validate every generated DAG (``npm run generate`` writes them to ./generated).

For each sample bundle:
  1. ``py_compile`` succeeds
  2. Airflow's DagBag imports it with no import errors, in every env (dev/stg/prd)
  3. no cycles, and dag_id == file name
  4. every templated field renders (Variables supplied the way Composer would)
  5. project standards: labels, retries, timeouts, callbacks, owner, docs, sensors
"""

from __future__ import annotations

import json
import os
import py_compile
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pendulum
import pytest

REPO = Path(__file__).resolve().parents[2]
GENERATED = Path(os.environ.get("GENERATED_DIR", REPO / "generated"))
DAG_FILES = sorted(GENERATED.glob("*/dags/*.py"))
ENVS = ("dev", "stg", "prd")
REQUIRED_LABELS = {"env", "team", "owner", "cost_center", "dag_id"}
LABEL_VALUE_RE = re.compile(r"^[\w-]{0,63}$")
LABELLED_OPERATOR_PREFIXES = ("BigQuery", "Dataproc", "Dataflow")


def _bundle_id(path: Path) -> str:
    return path.parent.parent.name


def _load_dagbag(dag_file: Path, monkeypatch: pytest.MonkeyPatch, env: str = "dev"):
    from airflow.models import DagBag

    monkeypatch.setenv("DEPLOY_ENV", env)
    return DagBag(dag_folder=str(dag_file), include_examples=False, safe_mode=False)


def _load_dag(dag_file: Path, monkeypatch: pytest.MonkeyPatch, env: str = "dev"):
    bag = _load_dagbag(dag_file, monkeypatch, env)
    assert not bag.import_errors, bag.import_errors
    return bag.dags[dag_file.stem]


def _set_variables(dag_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Expose the bundle's Airflow Variables as AIRFLOW_VAR_* env vars."""
    vars_file = dag_file.parent.parent / "config" / f"{dag_file.stem}_variables.json"
    if vars_file.exists():
        for name, value in json.loads(vars_file.read_text(encoding="utf-8")).items():
            serialized = value if isinstance(value, str) else json.dumps(value)
            monkeypatch.setenv(f"AIRFLOW_VAR_{name.upper()}", serialized)


def _strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from _strings(v)
    elif isinstance(value, list | tuple | set):
        for v in value:
            yield from _strings(v)


def _context(dag, task, logical_date) -> dict[str, Any]:
    from airflow import macros
    from airflow.utils.context import VariableAccessor

    return {
        "dag": dag,
        "task": task,
        "params": {k: task.params[k] for k in task.params},
        "ds": logical_date.to_date_string(),
        "ds_nodash": logical_date.format("YYYYMMDD"),
        "ts": logical_date.isoformat(),
        "ts_nodash": logical_date.format("YYYYMMDDTHHmmss"),
        "logical_date": logical_date,
        "execution_date": logical_date,
        "data_interval_start": logical_date,
        "data_interval_end": logical_date.add(days=1),
        "run_id": f"scheduled__{logical_date.isoformat()}",
        "macros": macros,
        "var": {
            "value": VariableAccessor(deserialize_json=False),
            "json": VariableAccessor(deserialize_json=True),
        },
    }


def _labels(task) -> dict | None:
    labels = getattr(task, "labels", None)
    if labels:
        return labels
    configuration = getattr(task, "configuration", None)
    if isinstance(configuration, dict):
        return configuration.get("labels")
    return None


# ---------------------------------------------------------------------------


def test_samples_were_generated():
    assert DAG_FILES, f"No generated DAGs in {GENERATED}. Run `npm run generate` first."


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=_bundle_id)
def test_py_compile(dag_file: Path):
    py_compile.compile(str(dag_file), doraise=True)


@pytest.mark.parametrize("env", ENVS)
@pytest.mark.parametrize("dag_file", DAG_FILES, ids=_bundle_id)
def test_dagbag_import_no_cycles(dag_file: Path, env: str, monkeypatch):
    from airflow.utils.dag_cycle_tester import check_cycle

    bag = _load_dagbag(dag_file, monkeypatch, env)
    assert not bag.import_errors, bag.import_errors
    assert list(bag.dags) == [dag_file.stem], "dag_id must match the file name"
    dag = bag.dags[dag_file.stem]
    check_cycle(dag)
    assert dag.tasks, "DAG has no tasks"


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=_bundle_id)
def test_templates_render(dag_file: Path, monkeypatch):
    _set_variables(dag_file, monkeypatch)
    dag = _load_dag(dag_file, monkeypatch)
    jinja_env = dag.get_template_env()
    logical_date = pendulum.datetime(2024, 1, 15, tz="UTC")
    for task in dag.tasks:
        task.render_template_fields(_context(dag, task, logical_date), jinja_env)
        for field in task.template_fields:
            for text in _strings(getattr(task, field)):
                assert "{{" not in text and "{%" not in text, (
                    f"{task.task_id}.{field} was not fully rendered: {text!r}"
                )


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=_bundle_id)
def test_dag_standards(dag_file: Path, monkeypatch):
    from airflow.sensors.base import BaseSensorOperator

    dag = _load_dag(dag_file, monkeypatch)
    assert dag.doc_md, "doc_md is required"
    assert dag.tags, "tags are required"
    assert dag.dagrun_timeout is not None
    assert dag.max_active_runs >= 1
    assert dag.user_defined_macros, "env config macros (cfg / cfg_var) are required"

    for task in dag.tasks:
        where = f"{dag.dag_id}.{task.task_id}"
        assert task.owner and task.owner != "airflow", f"{where}: set an owner"
        assert task.execution_timeout is not None, f"{where}: execution_timeout"
        assert task.retry_delay is not None, f"{where}: retry_delay"
        assert task.on_failure_callback, f"{where}: on_failure_callback"

        if type(task).__name__.startswith(LABELLED_OPERATOR_PREFIXES):
            labels = _labels(task)
            assert labels, f"{where}: GCP labels are required"
            missing = REQUIRED_LABELS - set(labels)
            assert not missing, f"{where}: missing labels {missing}"
            for key, value in labels.items():
                assert key == key.lower() and LABEL_VALUE_RE.match(str(value)), (
                    f"{where}: label {key}={value!r} is not a valid lowercase GCP label"
                )

        if isinstance(task, BaseSensorOperator) and not getattr(
            task, "deferrable", False
        ):
            assert task.mode == "reschedule", f"{where}: sensors use mode='reschedule'"
            assert task.timeout, f"{where}: sensors need a timeout"


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=_bundle_id)
def test_no_top_level_variable_access(dag_file: Path):
    source = dag_file.read_text(encoding="utf-8")
    for forbidden in ("Variable.get(", "BaseHook.get_connection(", ".get_conn()"):
        assert forbidden not in source, f"{forbidden} at parse time is not allowed"
