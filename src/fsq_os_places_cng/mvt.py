"""Encode place rows to Mapbox Vector Tile bytes.

`mapbox_vector_tile.encode` quantises geometries to tile-local coordinates
when `quantize_bounds` is given in EPSG:4326. We don't reproject ourselves
since Foursquare geometries are already EPSG:4326 (points).
"""

from __future__ import annotations

import logging

import mapbox_vector_tile
from shapely.geometry import mapping
from shapely.wkb import loads as wkb_loads

logger = logging.getLogger("fsq-os-places-cng")

LAYER_NAME = "places"
EXTENT = 4096


def encode_places_mvt(
    rows: list[dict],
    tile_bounds: tuple[float, float, float, float],
) -> bytes:
    """Encode `rows` (from `query_places_in_bbox`) into MVT bytes.

    Drops rows whose WKB fails to parse (rare, but defensive against any
    degenerate geometries we don't want to 500 on).
    """
    features = []
    for row in rows:
        wkb = row["geom_wkb"]
        if wkb is None:
            continue
        try:
            geom = wkb_loads(bytes(wkb))
        except Exception:
            logger.warning("skip unparseable WKB for id=%s", row.get("fsq_place_id"))
            continue
        # Pick the first category id/label as a single-valued tile property
        # so a MapLibre `match` expression can color by it without the
        # renderer having to walk arrays. `top_category` is the breadcrumb
        # root (e.g. "Dining and Drinking") and drives the color ramp;
        # `category_label` is the full leaf path for popup display.
        cat_ids = row.get("fsq_category_ids") or []
        cat_labels = row.get("fsq_category_labels") or []
        primary_cat_id = cat_ids[0] if cat_ids else None
        primary_cat_label = cat_labels[0] if cat_labels else None
        top_category = (
            primary_cat_label.split(" > ", 1)[0] if primary_cat_label else None
        )
        features.append(
            {
                "geometry": mapping(geom),
                "properties": {
                    "fsq_place_id": row.get("fsq_place_id"),
                    "name": row.get("name"),
                    "country": row.get("country"),
                    "category_id": primary_cat_id,
                    "category_label": primary_cat_label,
                    "top_category": top_category,
                },
            }
        )

    return mapbox_vector_tile.encode(
        [{"name": LAYER_NAME, "features": features}],
        quantize_bounds=tile_bounds,
        extents=EXTENT,
    )
