"""Tests for {{ }} templating applied by DAGBuilder to YAML and Python nodes.

Covers:
- Transform SQL without a params: block is still resolved
- Date functions in transform SQL follow the --date ("today") override
- Source table: paths are templated (YAML and Python @source)
- Python decorator params reach node params; CLI overrides replace declared keys
"""

from datetime import date, timedelta

from seeknal.workflow.dag import DAGBuilder  # ty: ignore[unresolved-import]

DATE_OVERRIDES = {"date": "2026-10-12", "run_date": "2026-10-12", "today": "2026-10-12"}

ORDERS_SOURCE = """\
kind: source
name: orders
source: csv
table: "data/orders_{{ today }}.csv"
"""


def _build(tmp_path, files, cli_overrides=None):
    """Write files under seeknal/ and build the DAG."""
    for rel_path, content in files.items():
        path = tmp_path / "seeknal" / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    builder = DAGBuilder(project_path=tmp_path, cli_overrides=cli_overrides)
    builder.build()
    return builder


class TestYAMLTemplates:
    def test_transform_without_params_is_resolved(self, tmp_path):
        before = date.today().isoformat()
        builder = _build(tmp_path, {
            "sources/orders.yml": ORDERS_SOURCE,
            "transforms/t.yml": """\
kind: transform
name: t
inputs:
  - ref: source.orders
transform: SELECT '{{ today }}' AS d FROM ref('source.orders')
""",
        })
        sql = builder.get_node("transform.t").yaml_data["transform"]
        assert f"'{before}'" in sql or f"'{date.today().isoformat()}'" in sql

    def test_transform_date_functions_follow_date_override(self, tmp_path):
        builder = _build(tmp_path, {
            "sources/orders.yml": ORDERS_SOURCE,
            "transforms/t.yml": """\
kind: transform
name: t
inputs:
  - ref: source.orders
params:
  run_day: "{{ today }}"
transform: |
  SELECT '{{ run_day }}' AS a, '{{ today(-1) }}' AS b, '{{ yesterday }}' AS c
  FROM ref('source.orders')
""",
        }, cli_overrides=DATE_OVERRIDES)
        sql = builder.get_node("transform.t").yaml_data["transform"]
        assert "'2026-10-12' AS a, '2026-10-11' AS b, '2026-10-11' AS c" in sql

    def test_source_table_is_templated(self, tmp_path):
        builder = _build(
            tmp_path, {"sources/orders.yml": ORDERS_SOURCE}, cli_overrides=DATE_OVERRIDES
        )
        assert builder.get_node("source.orders").yaml_data["table"] == "data/orders_2026-10-12.csv"

    def test_source_table_without_override_uses_today(self, tmp_path):
        builder = _build(tmp_path, {"sources/orders.yml": ORDERS_SOURCE})
        expected = f"data/orders_{date.today().isoformat()}.csv"
        assert builder.get_node("source.orders").yaml_data["table"] == expected


PY_PIPELINE = '''\
from seeknal.pipeline import source, transform


@source(name="py_orders", source="csv", table="data/orders_{{ today(-1) }}.csv")
def py_orders(ctx=None):
    pass


@transform(name="py_t", inputs=["source.orders"], run_day="{{ today }}", region="EU")
def py_t(ctx):
    return ctx.ref("source.orders")


@transform(name="py_plain", inputs=["source.orders"])
def py_plain(ctx):
    return ctx.ref("source.orders")


@transform(name="py_typed", inputs=["source.orders"], flag="1", answer="no", port=5432, limit=10)
def py_typed(ctx):
    return ctx.ref("source.orders")
'''


class TestPythonTemplates:
    def _files(self):
        return {"sources/orders.yml": ORDERS_SOURCE, "pipelines/nodes.py": PY_PIPELINE}

    def test_decorator_params_resolved(self, tmp_path):
        builder = _build(tmp_path, self._files())
        params = builder.get_node("transform.py_t").yaml_data["params"]
        assert params == {"run_day": date.today().isoformat(), "region": "EU"}

    def test_cli_override_replaces_declared_keys_only(self, tmp_path):
        overrides = {**DATE_OVERRIDES, "region": "US", "tier": "gold"}
        builder = _build(tmp_path, self._files(), cli_overrides=overrides)
        params = builder.get_node("transform.py_t").yaml_data["params"]
        # Declared keys follow the CLI; undeclared CLI keys are not injected
        assert params == {"run_day": "2026-10-12", "region": "US"}

    def test_transform_without_params_gets_no_params_key(self, tmp_path):
        builder = _build(tmp_path, self._files(), cli_overrides=DATE_OVERRIDES)
        assert "params" not in builder.get_node("transform.py_plain").yaml_data

    def test_python_source_table_is_templated(self, tmp_path):
        builder = _build(tmp_path, self._files(), cli_overrides=DATE_OVERRIDES)
        assert builder.get_node("source.py_orders").yaml_data["table"] == "data/orders_2026-10-11.csv"

    def test_python_source_table_without_override(self, tmp_path):
        builder = _build(tmp_path, self._files())
        expected = f"data/orders_{(date.today() - timedelta(days=1)).isoformat()}.csv"
        assert builder.get_node("source.py_orders").yaml_data["table"] == expected

    def test_plain_values_keep_their_python_types(self, tmp_path):
        # Only {{ }} strings are resolved; "1"/"no" must not become bools
        builder = _build(tmp_path, self._files(), cli_overrides={"limit": 5})
        params = builder.get_node("transform.py_typed").yaml_data["params"]
        assert params == {"flag": "1", "answer": "no", "port": 5432, "limit": 5}


class TestParamsInFingerprint:
    """A params change (e.g. from --date) re-runs feature groups and SOA nodes."""

    def test_feature_group_and_soa_hash_includes_params(self, tmp_path):
        from seeknal.workflow.state import calculate_node_hash  # ty: ignore[unresolved-import]

        for kind in ("feature_group", "second_order_aggregation"):
            base = {"kind": kind, "name": "n", "inputs": [{"ref": "source.orders"}]}
            day1 = calculate_node_hash({**base, "params": {"run_day": "2026-10-11"}}, tmp_path / "n.py")
            day2 = calculate_node_hash({**base, "params": {"run_day": "2026-10-12"}}, tmp_path / "n.py")
            assert day1 != day2, kind
