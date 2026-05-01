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

import logging
import threading
import time
from collections import defaultdict

import pyarrow.fs as pafs
import pyarrow.parquet as pq

logger = logging.getLogger("fsq-os-places-cng")

# Source Cooperative serves the bucket via path-style on the AWS endpoint.
S3_BUCKET = "us-west-2.opendata.source.coop"
S3_PREFIX = "fused/fsq-os-places"
S3_REGION = "us-west-2"


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
        # pyarrow's S3FileSystem expects a bucket-relative path, no scheme.
        return f"{S3_BUCKET}/{S3_PREFIX}/{self.release}/places/_metadata"

    def _s3_href(self, file_name: str) -> str:
        return f"s3://{S3_BUCKET}/{S3_PREFIX}/{self.release}/places/{file_name}"

    def build(self) -> None:
        """Fetch + parse `_metadata` once; populate `_items`."""
        with self._build_lock:
            if self._built:
                return
            t0 = time.time()
            fs = pafs.S3FileSystem(region=S3_REGION, anonymous=True)
            md = pq.read_metadata(self.metadata_path, filesystem=fs)
            t_meta = time.time() - t0

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
                "Parquet _metadata index ready: %d files (%d row groups) in %.1fs (metadata fetch %.1fs)",
                len(self._items),
                md.num_row_groups,
                time.time() - t0,
                t_meta,
            )

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
