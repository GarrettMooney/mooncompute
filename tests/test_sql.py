import polars as pl

from mooncompute.sql import sql


def test_sql_explicit_frame():
    df = pl.DataFrame({"user_id": [1, 1, 2], "x": [10, 20, 30]})
    out = sql("select user_id, count(*) n from df group by 1 order by user_id", df=df)
    assert out.sort("user_id")["n"].to_list() == [2, 1]


def test_sql_captures_caller_scope():
    df = pl.DataFrame({"a": [1, 2, 3]})  # noqa: F841 - referenced via SQL by name
    out = sql("select sum(a) s from df")
    assert out["s"][0] == 6


def test_sql_lazy_returns_lazyframe():
    df = pl.DataFrame({"a": [1, 2]})
    out = sql("select a from df", lazy=True, df=df)
    assert isinstance(out, pl.LazyFrame)
