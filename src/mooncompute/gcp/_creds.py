"""Materialize service-account JSON (e.g. a Modal secret) into ADC.

Modal secrets expose the key as GOOGLE_APPLICATION_CREDENTIALS_JSON. Write it
to a file and point GOOGLE_APPLICATION_CREDENTIALS at it. Idempotent; a no-op
locally where ADC already points at a file or the JSON env var is absent.
"""

from __future__ import annotations

import os
from pathlib import Path

_CREDS_PATH = "/tmp/gcp-creds.json"


def materialize_gcp_creds() -> None:
    existing = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if existing and Path(existing).exists():
        return
    blob = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if not blob:
        return
    Path(_CREDS_PATH).write_text(blob)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _CREDS_PATH
