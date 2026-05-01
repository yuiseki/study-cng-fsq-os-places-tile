"""Query parameter parsing for fsq-os-places-cng.

`filter_by_category` accepts comma-separated **top-level Foursquare
labels** (e.g. `"Dining and Drinking"`, `"Travel and Transportation"`).
The server expands each label to its set of leaf category ids via the
runtime-built `CategoryTaxonomy`, then pushes that set into DuckDB as
`array_has_any(fsq_category_ids, [...])` which hits the dictionary-
encoded VARCHAR fast path instead of a seq-scan UNNEST + LIKE.

We deliberately drop the breadcrumb-prefix mode (`"Dining and
Drinking > Restaurant"`) here. Sub-tree filtering would require a full
taxonomy with intermediate ids, which Foursquare doesn't include in
`fsq_category_ids` (leaf only) and which `_sample` doesn't reveal.
Top-level filtering covers the demoable cases at order-of-magnitude
better latency.
"""

from __future__ import annotations


def parse_top_labels(value: str) -> list[str]:
    """`"A, B"` -> `["A", "B"]`, dropping empty / whitespace-only items.

    Raises `ValueError` if no usable label remains.
    """
    items = [s.strip() for s in value.split(",")]
    items = [s for s in items if s]
    if not items:
        raise ValueError("filter_by_category requires at least one top-level label")
    return items
