import mooncompute as mc


def test_public_exports():
    for name in [
        "read",
        "write",
        "dry_run",
        "sql",
        "cache",
        "clear_cache",
        "configure",
        "settings",
        "Settings",
        "llm",
        "__version__",
    ]:
        assert hasattr(mc, name), name


def test_llm_namespace():
    assert hasattr(mc.llm, "map")
    assert hasattr(mc.llm, "embed")


def test_version_matches_metadata():
    from importlib.metadata import version

    assert mc.__version__ == version("mooncompute")
