import polars as pl
import pytest

from mooncompute.gcp import bq


def test_resolve_project_explicit_wins(monkeypatch):
    monkeypatch.setenv(bq.PROJECT_ENV, "from-env")
    assert bq._resolve_project("explicit") == "explicit"


def test_resolve_project_falls_back_to_env(monkeypatch):
    monkeypatch.setenv(bq.PROJECT_ENV, "from-env")
    assert bq._resolve_project(None) == "from-env"


def test_resolve_project_raises_when_unset(monkeypatch):
    monkeypatch.delenv(bq.PROJECT_ENV, raising=False)
    with pytest.raises(ValueError):
        bq._resolve_project(None)


def test_read_sql_plain(tmp_path):
    p = tmp_path / "q.sql"
    p.write_text("SELECT 1")
    assert bq.read_sql(p) == "SELECT 1"


def test_read_sql_substitution(tmp_path):
    p = tmp_path / "q.sql"
    p.write_text("SELECT * FROM t WHERE d = '{snapshot}'")
    assert (
        bq.read_sql(p, snapshot="2024-10-01")
        == "SELECT * FROM t WHERE d = '2024-10-01'"
    )


def test_sql_hash_is_stable_and_sensitive():
    assert bq._sql_hash("SELECT 1") == bq._sql_hash("SELECT 1")
    assert bq._sql_hash("SELECT 1") != bq._sql_hash("SELECT 2")


def test_decimals_to_float_casts_only_decimal_cols():
    df = pl.DataFrame({"n": [1, 2]}).with_columns(
        pl.col("n").cast(pl.Decimal(scale=2)).alias("amt")
    )
    out = bq._decimals_to_float(df)
    assert out["amt"].dtype == pl.Float64
    assert out["n"].dtype == pl.Int64


def test_decimals_to_float_noop_when_none():
    df = pl.DataFrame({"a": [1], "b": ["x"]})
    assert bq._decimals_to_float(df).equals(df)
