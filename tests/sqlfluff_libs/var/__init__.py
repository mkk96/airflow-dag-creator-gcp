"""Stub of Airflow's `var` template object so sqlfluff can render generated SQL.

Only used by sqlfluff (see library_path in .sqlfluff); never imported by DAGs.
"""


class _Config(dict):
    """Env config placeholder: any attribute resolves to a lint-friendly value."""

    def __getattr__(self, key):
        return f"sample_{key}".replace("-", "_")


class _JsonAccessor:
    def get(self, name, default=None):
        return _Config()


json = _JsonAccessor()
