"""Build the Parquet index cache ahead of time.

Run at docker build time so a cold-started pod does not pay for the 20 MB
`_metadata` fetch. In the cluster that fetch took 71-95s, long enough that the
browser gave up on its tile request before the server ever answered -- and a
request that never gets a response has no CORS header on it either, so it
surfaced in the console as a CORS failure rather than a timeout.

    FSQ_INDEX_CACHE_DIR=/app/.index-cache python -m fsq_os_places_cng.prewarm
"""

from __future__ import annotations

import logging
import sys

from fsq_os_places_cng.duckdb_query import (
    FSQ_RELEASE,
    get_connection,
    get_taxonomy,
)
from fsq_os_places_cng.parquet_index import ParquetMetadataIndex, cache_dir

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main() -> int:
    index = ParquetMetadataIndex(release=FSQ_RELEASE)
    index.build()
    files = len(index.files_intersecting((-180.0, -90.0, 180.0, 90.0)))
    if files == 0:
        print("prewarm produced an empty index", file=sys.stderr)
        return 1

    # The taxonomy is the other startup cost: a DuckDB scan of `_sample`
    # that measured 2-19s depending on how S3 feels that minute.
    taxonomy = get_taxonomy()
    taxonomy.build(get_connection())
    labels = taxonomy.stats()["top_labels"]
    if labels == 0:
        print("prewarm produced an empty taxonomy", file=sys.stderr)
        return 1

    print(
        f"prewarmed {files} files and {labels} category labels "
        f"for release {FSQ_RELEASE} into {cache_dir()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
