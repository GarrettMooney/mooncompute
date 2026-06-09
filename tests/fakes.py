"""In-memory fakes for GCS and BigQuery clients used in tests."""

from __future__ import annotations

import datetime as _dt
import io

# ---- GCS ----------------------------------------------------------------


class FakeBlob:
    def __init__(self, store: dict, bucket: str, key: str) -> None:
        self.store = store
        self.bucket = bucket
        self.key = key
        self.name = key  # GCS blob.name is the key (no bucket prefix)

    def _id(self) -> str:
        return f"{self.bucket}/{self.key}"

    def download_to_file(self, buf: io.BytesIO) -> None:
        buf.write(self.store[self._id()])

    def download_as_text(self) -> str:
        return self.store[self._id()].decode()

    def download_as_bytes(self) -> bytes:
        return self.store[self._id()]

    def upload_from_string(self, data, content_type: str | None = None) -> None:
        self.store[self._id()] = data.encode() if isinstance(data, str) else data


class FakeBucket:
    def __init__(self, store: dict, name: str) -> None:
        self.store = store
        self.name = name

    def blob(self, key: str) -> FakeBlob:
        return FakeBlob(self.store, self.name, key)

    def list_blobs(self, prefix: str = ""):
        out = []
        for full in self.store:
            bucket, _, key = full.partition("/")
            if bucket == self.name and key.startswith(prefix):
                out.append(FakeBlob(self.store, bucket, key))
        return out


class FakeStorageClient:
    def __init__(self, store: dict | None = None) -> None:
        self.store = {} if store is None else store

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(self.store, name)


# ---- BigQuery -----------------------------------------------------------


class FakeTable:
    def __init__(self, modified=None, full_id="proj.ds.t"):
        self.modified = modified or _dt.datetime(2024, 1, 1, tzinfo=_dt.UTC)
        self.full_table_id = full_id


class FakeDryRunJob:
    def __init__(self, total_bytes):
        self.total_bytes_processed = total_bytes


class FakeQueryJob:
    def __init__(self, table) -> None:
        self._table = table

    def to_arrow(self, create_bqstorage_client: bool = False):
        return self._table


class FakeLoadJob:
    def result(self):
        return None


class FakeBQClient:
    def __init__(self, project: str = "proj", table=None) -> None:
        self.project = project
        self._table = table
        self.queries: list[str] = []
        self.load_calls: list[dict] = []
        self._modified = None
        self.dry_run_bytes = 0

    def get_table(self, ref):
        return FakeTable(modified=self._modified, full_id=str(ref))

    def query(self, sql, job_config=None):
        self.queries.append(sql)
        if job_config is not None and getattr(job_config, "dry_run", False):
            return FakeDryRunJob(self.dry_run_bytes)
        return FakeQueryJob(self._table)

    def load_table_from_file(self, stream, destination=None, **kwargs) -> FakeLoadJob:
        self.load_calls.append({"destination": destination, **kwargs})
        return FakeLoadJob()
