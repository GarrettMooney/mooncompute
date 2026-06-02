import os
from pathlib import Path

from dswkit.gcp import _creds


def test_noop_when_no_env(monkeypatch, tmp_path):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS_JSON", raising=False)
    monkeypatch.setattr(_creds, "_CREDS_PATH", str(tmp_path / "creds.json"))
    _creds.materialize_gcp_creds()
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ
    assert not Path(tmp_path / "creds.json").exists()


def test_materializes_json_to_file(monkeypatch, tmp_path):
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.setenv(
        "GOOGLE_APPLICATION_CREDENTIALS_JSON", '{"type": "service_account"}'
    )
    target = tmp_path / "creds.json"
    monkeypatch.setattr(_creds, "_CREDS_PATH", str(target))
    _creds.materialize_gcp_creds()
    assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == str(target)
    assert target.read_text() == '{"type": "service_account"}'


def test_noop_when_gac_points_at_existing_file(monkeypatch, tmp_path):
    existing = tmp_path / "already.json"
    existing.write_text("{}")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(existing))
    monkeypatch.setenv(
        "GOOGLE_APPLICATION_CREDENTIALS_JSON", '{"type": "service_account"}'
    )
    monkeypatch.setattr(_creds, "_CREDS_PATH", str(tmp_path / "creds.json"))
    _creds.materialize_gcp_creds()
    assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == str(existing)
    assert not Path(tmp_path / "creds.json").exists()
