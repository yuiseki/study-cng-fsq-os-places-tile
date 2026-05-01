"""fsq-os-places-cng: dynamic MVT server for Foursquare OS Places.

Endpoints:
    GET /health               -> {"ok": true}
    GET /tiles/{z}/{x}/{y}.mvt
        Query parameters:
            filter_by_category   comma-separated fsq_category_ids (optional)
            limit                max features per tile (default 5000)

Run: `uv run python -m fsq_os_places_cng.server`
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import mercantile
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi import Path as PathParam
from fastapi.middleware.cors import CORSMiddleware

from fsq_os_places_cng.duckdb_query import (
    FSQ_RELEASE,
    get_connection,
    get_parquet_index,
    get_taxonomy,
    query_places_in_bbox,
)
from fsq_os_places_cng.filters import parse_top_labels
from fsq_os_places_cng.mvt import encode_places_mvt

logger = logging.getLogger("fsq-os-places-cng")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

MVT_MEDIA_TYPE = "application/vnd.mapbox-vector-tile"
ZOOM_MIN = 0
ZOOM_MAX = 22


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm up DuckDB extensions, the parquet metadata index, and the
    # category taxonomy before the first request lands. Cold start is
    # bounded by the `_metadata` fetch (~6s for 20MB) plus the `_sample`
    # scan (~2s) instead of DuckDB's wildcard expansion at request time.
    try:
        con = get_connection()
        get_parquet_index().build()
        get_taxonomy().build(con)
    except Exception:
        logger.exception("startup warmup failed")
    yield


app = FastAPI(
    title="fsq-os-places-cng",
    version="0.1.0",
    description=(
        "On-the-fly dynamic vector tile server for Foursquare OS Places "
        f"(release {FSQ_RELEASE})."
    ),
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {
        "ok": True,
        "release": FSQ_RELEASE,
        "index": get_parquet_index().stats(),
        "taxonomy": get_taxonomy().stats(),
    }


@app.get("/categories")
def categories():
    """Return the taxonomy's top-level labels for viewer UIs to consume."""
    return {"top_labels": get_taxonomy().top_labels()}


@app.get("/tiles/{z}/{x}/{y}.mvt")
def tile(
    z: int = PathParam(..., ge=ZOOM_MIN, le=ZOOM_MAX),
    x: int = PathParam(..., ge=0),
    y: int = PathParam(..., ge=0),
    filter_by_category: str | None = Query(
        None,
        description=(
            "Comma-separated top-level Foursquare labels to keep, e.g. "
            "'Dining and Drinking' or 'Dining and Drinking,Retail'. "
            "Omit to disable."
        ),
    ),
    limit: int = Query(5000, ge=1, le=50000),
):
    max_xy = (1 << z) - 1
    if x > max_xy or y > max_xy:
        raise HTTPException(400, f"x/y out of range for zoom {z}")

    top_labels: list[str] | None = None
    if filter_by_category is not None:
        try:
            top_labels = parse_top_labels(filter_by_category)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    bounds = mercantile.bounds(x, y, z)
    bbox = (bounds.west, bounds.south, bounds.east, bounds.north)

    try:
        rows = query_places_in_bbox(bbox, top_labels=top_labels, limit=limit)
    except Exception as exc:
        logger.exception("DuckDB query failed for z=%d x=%d y=%d", z, x, y)
        raise HTTPException(502, f"upstream query failed: {exc}") from exc

    mvt_bytes = encode_places_mvt(rows, bbox)
    logger.info(
        "tile z=%d/%d/%d top_labels=%s rows=%d bytes=%d",
        z, x, y, top_labels, len(rows), len(mvt_bytes),
    )
    return Response(content=mvt_bytes, media_type=MVT_MEDIA_TYPE)


if __name__ == "__main__":
    import os

    import uvicorn

    host = os.environ.get("HOST", "127.0.0.1")
    # Different default port from buildings-cng (8006) so both studies can
    # run side-by-side on the same dev box.
    port = int(os.environ.get("PORT", "8007"))
    uvicorn.run(app, host=host, port=port)
