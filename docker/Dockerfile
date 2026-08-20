# Build a small image for the fsq-os-places-cng Knative ksvc.
#
# DuckDB downloads the `spatial` and `httpfs` extensions on first use, so we
# don't bake them into the image. They land in /root/.duckdb on first /tiles
# request; subsequent calls hit the cached extension binaries.

FROM python:3.12-slim

# uv is significantly faster than pip and respects uv.lock.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# libgeos is required by shapely / mapbox-vector-tile at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgeos-c1v5 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first (better build cache).
# README.md is required by hatchling because pyproject.toml declares
# `readme = "README.md"` and the editable install validates that file exists.
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
RUN uv sync --frozen --no-dev

# Bake the derived Parquet index into the image. The 20 MB `_metadata` fetch
# took 71-95s from inside the cluster, so paying it on every cold start meant
# the first tile request timed out in the browser. The cached index is a few
# KB and makes startup effectively instant.
ENV FSQ_INDEX_CACHE_DIR=/app/.index-cache
RUN uv run --no-dev python -m fsq_os_places_cng.prewarm

ENV HOST=0.0.0.0 \
    PORT=8080

EXPOSE 8080

CMD ["uv", "run", "--no-dev", "python", "-m", "fsq_os_places_cng.server"]
