"""DuckDB Spatial layer: read Foursquare OS Places GeoParquet from S3.

We pre-build a spatial index of all Parquet files at startup
(`ParquetMetadataIndex.build()` reads the dataset `_metadata`). At request
time we look up the small set of files whose file-level bbox intersects
the tile bbox and hand that explicit list to `read_parquet`, bypassing
DuckDB's wildcard expansion + per-file metadata fetch.

The Source Cooperative bucket is served path-style on the standard AWS
endpoint, so we configure DuckDB to match (vs. Overture's virtual-hosted
style, which DuckDB defaults to).
"""

from __future__ import annotations

import logging
import threading

import duckdb

from fsq_os_places_cng.category_taxonomy import CategoryTaxonomy
from fsq_os_places_cng.parquet_index import ParquetMetadataIndex

logger = logging.getLogger("fsq-os-places-cng")

# Latest release as of 2025-02-06; Fused publishes monthly snapshots under
# s3://us-west-2.opendata.source.coop/fused/fsq-os-places/<release>/places/.
FSQ_RELEASE = "2025-02-06"
FSQ_SAMPLE_URI = (
    f"s3://us-west-2.opendata.source.coop/fused/fsq-os-places/"
    f"{FSQ_RELEASE}/places/_sample"
)

_conn: duckdb.DuckDBPyConnection | None = None
_conn_lock = threading.Lock()

# DuckDB + spatial extension is not safe under concurrent queries from
# uvicorn's thread pool (we hit SIGSEGV inside _duckdb.cpython-*.so when
# the browser fires several tile requests in parallel). Serialise all
# query() calls with this lock until we move to per-request cursors or
# replace the spatial extension.
_query_lock = threading.Lock()

_pq_index: ParquetMetadataIndex | None = None
_pq_index_lock = threading.Lock()

_taxonomy: CategoryTaxonomy | None = None
_taxonomy_lock = threading.Lock()


def get_connection() -> duckdb.DuckDBPyConnection:
    """Lazy-init a process-wide DuckDB connection with spatial + httpfs loaded."""
    global _conn
    if _conn is not None:
        return _conn
    with _conn_lock:
        if _conn is None:
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute("SET s3_region='us-west-2';")
            # Source Cooperative requires path-style addressing because the
            # bucket name contains dots ('us-west-2.opendata.source.coop'),
            # which breaks SNI on virtual-hosted-style URLs.
            con.execute("SET s3_url_style='path';")
            con.execute("SET s3_endpoint='s3.us-west-2.amazonaws.com';")
            _conn = con
    return _conn


def get_parquet_index() -> ParquetMetadataIndex:
    """Return the singleton parquet metadata index (caller does `build()`)."""
    global _pq_index
    if _pq_index is not None:
        return _pq_index
    with _pq_index_lock:
        if _pq_index is None:
            _pq_index = ParquetMetadataIndex(release=FSQ_RELEASE)
    return _pq_index


def get_taxonomy() -> CategoryTaxonomy:
    """Return the singleton category taxonomy (caller does `build()`)."""
    global _taxonomy
    if _taxonomy is not None:
        return _taxonomy
    with _taxonomy_lock:
        if _taxonomy is None:
            _taxonomy = CategoryTaxonomy(
                sample_s3_uri=FSQ_SAMPLE_URI, release=FSQ_RELEASE
            )
    return _taxonomy


def _expand_top_labels_to_leaf_ids(top_labels: list[str]) -> list[str]:
    """Translate user-facing top labels into the leaf-id set DuckDB filters on.

    Unknown labels (not seen in `_sample`) are silently skipped; callers
    that pass a typo therefore get an empty result instead of a 500.
    """
    tax = get_taxonomy()
    out: list[str] = []
    seen: set[str] = set()
    for top in top_labels:
        ids = tax.expand(top) or []
        for i in ids:
            if i not in seen:
                seen.add(i)
                out.append(i)
    return out


def query_places_in_bbox(
    bbox: tuple[float, float, float, float],
    top_labels: list[str] | None = None,
    limit: int = 5000,
) -> list[dict]:
    """Return place rows whose bbox intersects `bbox`.

    `bbox` is `(west, south, east, north)` in EPSG:4326. `top_labels`,
    if provided, is expanded via the runtime taxonomy to a set of leaf
    `fsq_category_ids` and pushed into DuckDB as `array_has_any` (which
    is dramatically faster than UNNEST + LIKE on the labels array).

    Performance:
    - The parquet metadata index pre-resolves the small set of Parquet
      files whose file-level bbox intersects the tile (typically 1-2 of
      the 81 total).
    - DuckDB then does row-group pruning inside those files using the
      `bbox` struct column statistics, plus dictionary-encoded equality
      pushdown on the `fsq_category_ids` array element column.
    """
    files = get_parquet_index().files_intersecting(bbox)
    if not files:
        return []
    west, south, east, north = bbox
    con = get_connection()
    file_list_sql = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    params: list = [east, west, north, south]
    cat_filter = ""
    if top_labels:
        leaf_ids = _expand_top_labels_to_leaf_ids(top_labels)
        if not leaf_ids:
            # All requested labels were unknown; return nothing rather than
            # the entire tile (matches the user's intent of "filter to X").
            return []
        leaf_list_sql = "[" + ", ".join(f"'{i}'" for i in leaf_ids) + "]"
        cat_filter = f"AND array_has_any(fsq_category_ids, {leaf_list_sql})"
    params.append(limit)
    sql = f"""
      SELECT
        fsq_place_id,
        name,
        ST_AsWKB(geometry)        AS geom_wkb,
        fsq_category_ids,
        fsq_category_labels,
        country
      FROM read_parquet({file_list_sql})
      WHERE bbox.xmin <= ?
        AND bbox.xmax >= ?
        AND bbox.ymin <= ?
        AND bbox.ymax >= ?
        {cat_filter}
      LIMIT ?
    """
    with _query_lock:
        rows = con.execute(sql, params).fetchall()
    cols = (
        "fsq_place_id",
        "name",
        "geom_wkb",
        "fsq_category_ids",
        "fsq_category_labels",
        "country",
    )
    logger.info(
        "duckdb query: files=%d rows=%d bbox=%s top_labels=%s",
        len(files), len(rows), bbox, top_labels,
    )
    return [dict(zip(cols, r, strict=True)) for r in rows]
