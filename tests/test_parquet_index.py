"""Smoke test for the parquet `_metadata`-driven spatial index.

This hits the public Source Cooperative bucket. It is skipped automatically
if the network is unreachable so CI without internet still passes.
"""

from __future__ import annotations

import socket

import pytest

from fsq_os_places_cng.parquet_index import ParquetMetadataIndex


def _network_ok() -> bool:
    try:
        socket.create_connection(("s3.us-west-2.amazonaws.com", 443), timeout=2.0)
        return True
    except OSError:
        return False


@pytest.mark.skipif(not _network_ok(), reason="needs network to S3")
def test_build_returns_81_files():
    idx = ParquetMetadataIndex(release="2025-02-06")
    idx.build()
    stats = idx.stats()
    assert stats["ready"] is True
    # Fused 2025-02-06 release is 81 part files.
    assert stats["indexed_files"] == 81


@pytest.mark.skipif(not _network_ok(), reason="needs network to S3")
def test_files_intersecting_manhattan_returns_few():
    idx = ParquetMetadataIndex(release="2025-02-06")
    idx.build()
    # A small bbox over Manhattan.
    files = idx.files_intersecting((-74.02, 40.70, -73.93, 40.80))
    # Geospatial partitioning should put Manhattan in a small handful of
    # files, not all 81.
    assert 1 <= len(files) <= 10
    assert all(f.endswith(".parquet") for f in files)
