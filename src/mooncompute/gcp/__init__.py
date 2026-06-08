"""GCP data-access helpers (BigQuery, GCS).

Submodules and symbols are imported lazily (PEP 562) so each extra stands alone:
``import mooncompute.gcp`` pulls neither polars nor google-cloud-bigquery until
you touch a BigQuery helper. A ``mooncompute[gcs]`` consumer can
``from mooncompute.gcp import gcs`` for JSON/bytes I/O without the BigQuery stack
installed; touching `bq2pl`/`extract_cached`/etc. requires ``mooncompute[bq]``.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

__all__ = [
    "gcs",
    "bq2pl",
    "extract_cached",
    "pl2bq",
    "read_sql",
    "PROJECT_ENV",
]

# names served from the .bq submodule (imported on first access)
_BQ_EXPORTS = frozenset({"bq2pl", "extract_cached", "pl2bq", "read_sql", "PROJECT_ENV"})

if TYPE_CHECKING:
    from . import gcs
    from .bq import PROJECT_ENV, bq2pl, extract_cached, pl2bq, read_sql


def __getattr__(name: str):
    if name == "gcs":
        return importlib.import_module(f"{__name__}.gcs")
    if name in _BQ_EXPORTS:
        bq = importlib.import_module(f"{__name__}.bq")
        return getattr(bq, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
