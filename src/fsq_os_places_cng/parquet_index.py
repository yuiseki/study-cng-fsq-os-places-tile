"""Parquet `_metadata`-driven spatial index over Foursquare OS Places.

The Fused-published distribution on Source Cooperative ships a dataset-level
Parquet `_metadata` file (~20 MB) that aggregates the row-group footers of
every `N.parquet` part. pyarrow exposes per-row-group `file_path` plus the
column statistics for the `bbox.{xmin,ymin,xmax,ymax}` struct fields, so we
can derive a `file -> bbox` index by reading exactly one HTTP object at
startup.

This is the Foursquare counterpart to Overture's STAC catalog: same goal
(file selection without DuckDB's per-file footer fetch), different shape
(in-band Parquet metadata vs. out-of-band STAC JSON tree).
"""

from __future__ import annotations

import io
import json
import logging
import os
import threading
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

logger = logging.getLogger("fsq-os-places-cng")

# Source Cooperative serves the bucket via path-style on the AWS endpoint.
S3_BUCKET = "us-west-2.opendata.source.coop"
S3_PREFIX = "fused/fsq-os-places"
S3_REGION = "us-west-2"


def cache_dir() -> Path:
    """Where prewarmed artifacts (file index, category taxonomy) are cached.

    The docker image runs `python -m fsq_os_places_cng.prewarm` at build time
    with this pointed at a directory inside the image, so a cold-started pod
    reads a few KB from local disk instead of pulling 20 MB from S3.
    """
    env = os.environ.get("FSQ_INDEX_CACHE_DIR")
    if env:
        return Path(env)
    return Path.home() / ".cache" / "fsq-os-places-cng"


class ParquetMetadataIndex:
    """In-memory spatial index of Foursquare OS Places Parquet files.

    Holds a flat list of `(bbox, s3_href)` tuples derived from the dataset
    `_metadata` file. `files_intersecting(bbox)` is O(n) over n=81 files,
    which is well under the DuckDB query cost.
    """

    def __init__(self, release: str) -> None:
        self.release = release
        self._items: list[tuple[tuple[float, float, float, float], str]] = []
        self._build_lock = threading.Lock()
        self._built = False

    @property
    def metadata_path(self) -> str:
        # Path-style HTTPS against the regional S3 endpoint. We fetch this
        # ourselves rather than handing the path to pyarrow's S3FileSystem:
        # pyarrow reads the 20 MB object through many small ranged GETs and
        # took 71-95s in the cluster, which blew past the browser's own
        # timeout on every cold start (the tile request failed, and with no
        # response there was no CORS header either, so it surfaced as a CORS
        # error). One plain GET of the same bytes takes ~8s.
        return (
            f"https://s3.{S3_REGION}.amazonaws.com/{S3_BUCKET}"
            f"/{S3_PREFIX}/{self.release}/places/_metadata"
        )

    def _cache_path(self) -> Path:
        return cache_dir() / f"index_{self.release}.json"

    def _load_from_disk(self) -> bool:
        path = self._cache_path()
        if not path.exists():
            return False
        try:
            with path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
            self._items = [(tuple(r["bbox"]), r["href"]) for r in raw]
        except Exception as e:
            logger.warning("index cache load failed (%s); rebuilding", e)
            return False
        logger.info(
            "Parquet index cache hit: %d files from %s", len(self._items), path
        )
        return True

    def _save_to_disk(self) -> None:
        path = self._cache_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(
                [{"bbox": list(bb), "href": href} for bb, href in self._items], f
            )
        tmp.replace(path)
        logger.info("Parquet index cache wrote %d files to %s", len(self._items), path)

    def _s3_href(self, file_name: str) -> str:
        return f"s3://{S3_BUCKET}/{S3_PREFIX}/{self.release}/places/{file_name}"

    def build(self) -> None:
        """Fetch + parse `_metadata` once; populate `_items`."""
        with self._build_lock:
            if self._built:
                return
            if self._load_from_disk():
                self._built = True
                return
            t0 = time.time()
            with urllib.request.urlopen(self.metadata_path, timeout=120) as resp:
                raw = resp.read()
            t_meta = time.time() - t0
            # Parsing the footer out of the buffer is ~0.1s; the fetch is the
            # whole cost.
            md = pq.read_metadata(io.BytesIO(raw))

            # Map column path -> column index (consistent across row groups).
            rg0 = md.row_group(0)
            col_idx: dict[str, int] = {}
            for i in range(rg0.num_columns):
                col_idx[rg0.column(i).path_in_schema] = i
            for required in ("bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax"):
                if required not in col_idx:
                    raise RuntimeError(
                        f"_metadata is missing column {required!r}; "
                        f"this index needs the bbox struct columns"
                    )
            xmin_i = col_idx["bbox.xmin"]
            ymin_i = col_idx["bbox.ymin"]
            xmax_i = col_idx["bbox.xmax"]
            ymax_i = col_idx["bbox.ymax"]

            # Aggregate row-group bboxes per file.
            file_bbox: dict[str, list[float]] = defaultdict(
                lambda: [float("inf"), float("inf"), float("-inf"), float("-inf")]
            )
            for i in range(md.num_row_groups):
                rg = md.row_group(i)
                fp = rg.column(0).file_path
                if not fp:
                    continue
                xs = rg.column(xmin_i).statistics
                ys = rg.column(ymin_i).statistics
                Xs = rg.column(xmax_i).statistics
                Ys = rg.column(ymax_i).statistics
                if not (xs and ys and Xs and Ys):
                    continue
                cur = file_bbox[fp]
                if xs.min < cur[0]:
                    cur[0] = xs.min
                if ys.min < cur[1]:
                    cur[1] = ys.min
                if Xs.max > cur[2]:
                    cur[2] = Xs.max
                if Ys.max > cur[3]:
                    cur[3] = Ys.max

            self._items = [
                (tuple(bb), self._s3_href(fp))
                for fp, bb in file_bbox.items()
            ]
            self._built = True
            logger.info(
                "Parquet _metadata index ready: %d files (%d row groups) in %.1fs "
                "(metadata fetch %.1fs, %.1f MB)",
                len(self._items),
                md.num_row_groups,
                time.time() - t0,
                t_meta,
                len(raw) / 1e6,
            )
            try:
                self._save_to_disk()
            except Exception as e:
                logger.warning("index cache save failed (%s); continuing", e)

    def files_intersecting(
        self,
        query_bbox: tuple[float, float, float, float],
    ) -> list[str]:
        """Return s3 hrefs of files whose bbox intersects `query_bbox`."""
        if not self._built:
            self.build()
        west, south, east, north = query_bbox
        out = []
        for (xmin, ymin, xmax, ymax), s3 in self._items:
            if xmin <= east and xmax >= west and ymin <= north and ymax >= south:
                out.append(s3)
        return out

    def stats(self) -> dict:
        return {
            "release": self.release,
            "indexed_files": len(self._items),
            "ready": self._built,
        }
