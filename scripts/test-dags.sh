#!/usr/bin/env bash
# Run the generated-DAG checks locally in a Linux container (same as CI).
# Airflow does not support native Windows, so use this from macOS/Linux/WSL with Docker.
#
#   npm run generate && bash scripts/test-dags.sh
set -euo pipefail
cd "$(dirname "$0")/.."

read_cfg() { node -p "require('./runtime.config.json').$1"; }
AIRFLOW=$(read_cfg airflowVersion)
PYTHON=$(read_cfg pythonVersion)
GOOGLE=$(read_cfg googleProviderVersion)
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW}/constraints-${PYTHON}.txt"

if [ ! -d generated ]; then
  echo "generated/ not found; run 'npm run generate' first." >&2
  exit 1
fi

docker run --rm -v "$PWD":/work -w /work "python:${PYTHON}-slim" bash -euo pipefail -c "
  pip install -q 'apache-airflow==${AIRFLOW}' 'apache-airflow-providers-google==${GOOGLE}' apache-airflow-providers-apache-beam apache-airflow-providers-openlineage pytest --constraint '${CONSTRAINTS}'
  export AIRFLOW_HOME=/tmp/airflow-home
  airflow db migrate > /dev/null
  pytest tests/dags -q
  python -m venv /tmp/lint && /tmp/lint/bin/pip install -q -r requirements-lint.txt
  /tmp/lint/bin/ruff check generated tests
  /tmp/lint/bin/ruff format --check generated tests
  /tmp/lint/bin/sqlfluff lint generated
"
