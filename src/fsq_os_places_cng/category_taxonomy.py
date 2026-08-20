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

import json
import logging
import threading
import time
from collections import defaultdict

import duckdb

from fsq_os_places_cng.parquet_index import cache_dir

logger = logging.getLogger("fsq-os-places-cng")


class CategoryTaxonomy:
    """`top_label` (e.g. 'Dining and Drinking') -> `set[leaf_id]`."""

    def __init__(self, sample_s3_uri: str, release: str) -> None:
        self.sample_s3_uri = sample_s3_uri
        self.release = release
        self._top_to_ids: dict[str, list[str]] = {}
        self._build_lock = threading.Lock()
        self._built = False

    def _cache_path(self):
        return cache_dir() / f"taxonomy_{self.release}.json"

    def _load_from_disk(self) -> bool:
        path = self._cache_path()
        if not path.exists():
            return False
        try:
            with path.open("r", encoding="utf-8") as f:
                self._top_to_ids = json.load(f)
        except Exception as e:
            logger.warning("taxonomy cache load failed (%s); rebuilding", e)
            return False
        logger.info(
            "Category taxonomy cache hit: %d top labels from %s",
            len(self._top_to_ids), path,
        )
        return True

    def _save_to_disk(self) -> None:
        path = self._cache_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(self._top_to_ids, f)
        tmp.replace(path)
        logger.info("Category taxonomy cache wrote %s", path)

    def build(self, con: duckdb.DuckDBPyConnection) -> None:
        """Populate the mapping from the dataset's `_sample` part.

        Reuses the shared DuckDB connection so it inherits the path-style
        S3 settings configured in `duckdb_query.get_connection()`.
        """
        with self._build_lock:
            if self._built:
                return
            if self._load_from_disk():
                self._built = True
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
            try:
                self._save_to_disk()
            except Exception as e:
                logger.warning("taxonomy cache save failed (%s); continuing", e)

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
