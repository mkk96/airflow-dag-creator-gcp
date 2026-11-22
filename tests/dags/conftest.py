"""Test configuration: isolate Airflow from any local installation."""

from __future__ import annotations

import os
import tempfile

# Must be set before Airflow is imported anywhere.
os.environ.setdefault(
    "AIRFLOW_HOME", os.path.join(tempfile.gettempdir(), "dag-creator-airflow-home")
)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault(
    "AIRFLOW__CORE__DAGS_FOLDER", os.path.join(os.path.dirname(__file__), "_empty")
)
os.environ.setdefault("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "120")
