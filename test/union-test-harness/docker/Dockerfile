# Local PostgreSQL + PostGIS for the union harness.
# Built on the official postgres image (multi-arch: runs natively on Apple
# Silicon and Intel). PostGIS comes from the PGDG apt repo the image already uses.
ARG PG_MAJOR=16
FROM postgres:${PG_MAJOR}-bookworm
ARG PG_MAJOR
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        postgresql-${PG_MAJOR}-postgis-3 postgresql-${PG_MAJOR}-postgis-3-scripts \
 && rm -rf /var/lib/apt/lists/*
