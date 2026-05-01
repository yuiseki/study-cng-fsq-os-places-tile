"""Foursquare OS Places category taxonomy, built lazily from `_sample`.

The published Foursquare OS Places dataset stores **leaf** category ids
in `fsq_category_ids` (no parent ids). To filter "all restaurants" or
"everything in Dining and Drinking", we therefore need a `top_label ->
{leaf_id, ...}` mapping so we can translate a single user-facing prefix
into a set of leaf ids and push that as `array_has_any(fsq_category_ids,
[...])` into DuckDB. That equality-set push-down hits DuckDB's
dictionary-encoded VARCHAR fast path and parquet bloom filters where
present, instead of the seq-scan UNNEST + LIKE pattern.

We build the mapping at startup from the dataset's tiny `_sample` part
(~2 MB, ~10k rows). Empirically that yields all 10 top-level labels and
~625 leaf ids in ~2 seconds, which covers the long-tail of categories
people actually filter on. Leaf ids that don't appear in `_sample` (rare
sub-trees) are not selectable via this fast path; the taxonomy is best-
effort, not exhaustive.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict

import duckdb

logger = logging.getLogger("fsq-os-places-cng")


class CategoryTaxonomy:
    """`top_label` (e.g. 'Dining and Drinking') -> `set[leaf_id]`."""

    def __init__(self, sample_s3_uri: str) -> None:
        self.sample_s3_uri = sample_s3_uri
        self._top_to_ids: dict[str, list[str]] = {}
        self._build_lock = threading.Lock()
        self._built = False

    def build(self, con: duckdb.DuckDBPyConnection) -> None:
        """Populate the mapping from the dataset's `_sample` part.

        Reuses the shared DuckDB connection so it inherits the path-style
        S3 settings configured in `duckdb_query.get_connection()`.
        """
        with self._build_lock:
            if self._built:
                return
            t0 = time.time()
            rows = con.execute(
                f"""
                SELECT DISTINCT
                    fsq_category_ids[1] AS leaf_id,
                    split_part(fsq_category_labels[1], ' > ', 1) AS top_label
                FROM read_parquet('{self.sample_s3_uri}')
                WHERE fsq_category_ids IS NOT NULL
                  AND len(fsq_category_ids) >= 1
                  AND fsq_category_labels IS NOT NULL
                  AND len(fsq_category_labels) >= 1
                """
            ).fetchall()
            grouped: dict[str, list[str]] = defaultdict(list)
            for leaf_id, top_label in rows:
                if leaf_id and top_label:
                    grouped[top_label].append(leaf_id)
            self._top_to_ids = dict(grouped)
            self._built = True
            logger.info(
                "Category taxonomy ready: %d top labels, %d leaf ids in %.1fs",
                len(self._top_to_ids),
                sum(len(v) for v in self._top_to_ids.values()),
                time.time() - t0,
            )

    def expand(self, top_label: str) -> list[str] | None:
        """Return the leaf-id list for `top_label`, or None if unknown."""
        return self._top_to_ids.get(top_label)

    def top_labels(self) -> list[str]:
        return sorted(self._top_to_ids.keys())

    def stats(self) -> dict:
        return {
            "ready": self._built,
            "top_labels": len(self._top_to_ids),
            "leaf_ids": sum(len(v) for v in self._top_to_ids.values()),
        }
