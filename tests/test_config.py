import importlib

import mooncompute.config as cfg


def test_defaults():
    s = cfg.Settings()
    assert s.location == "US"
    assert s.llm_location == "us-central1"  # Vertex region, distinct from BQ location
    assert s.llm_default_model == "gemini-2.5-flash"
    assert s.llm_embed_model == "gemini-embedding-001"
    assert s.llm_concurrency == 16
    assert s.bq_dry_run_default is True


def test_configure_mutates_singleton():
    cfg.configure(project="p1", location="EU")
    assert cfg.settings.project == "p1"
    assert cfg.settings.location == "EU"


def test_configure_rejects_unknown_key():
    try:
        cfg.configure(nope=1)
    except AttributeError as e:
        assert "nope" in str(e)
    else:
        raise AssertionError("expected AttributeError")


def test_project_env_fallback(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "envproj")
    monkeypatch.delenv("MOONCOMPUTE_PROJECT", raising=False)
    reloaded = importlib.reload(cfg)
    assert reloaded.settings.project == "envproj"
