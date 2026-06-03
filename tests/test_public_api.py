import mooncompute.gcp as gcp


def test_public_surface():
    for name in (
        "bq2pl",
        "extract_cached",
        "pl2bq",
        "read_sql",
        "PROJECT_ENV",
    ):
        assert hasattr(gcp, name), name
    assert hasattr(gcp.gcs, "read_parquet")
    assert hasattr(gcp.gcs, "write_parquet")


def test_top_level_gcp_reexport():
    import mooncompute

    assert hasattr(mooncompute, "gcp")
    assert "gcp" in dir(mooncompute)
    # the re-export is declared, not just an import side-effect
    assert "gcp" in mooncompute.__all__
    assert hasattr(mooncompute.gcp, "bq2pl")
    from mooncompute import gcp

    assert gcp is mooncompute.gcp


def test_version_still_exposed():
    import mooncompute

    assert isinstance(mooncompute.__version__, str)


def test_py_typed_marker_present():
    from pathlib import Path

    import mooncompute

    pkg_dir = Path(mooncompute.__file__).parent
    assert (pkg_dir / "py.typed").is_file()
