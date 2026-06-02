import dswkit.gcp as gcp


def test_public_surface():
    for name in (
        "bq2pl",
        "extract_cached",
        "pl2bq",
        "read_sql",
        "PROJECT_DEV",
        "PROJECT_PROD",
    ):
        assert hasattr(gcp, name), name
    assert hasattr(gcp.gcs, "read_parquet")
    assert hasattr(gcp.gcs, "write_parquet")
