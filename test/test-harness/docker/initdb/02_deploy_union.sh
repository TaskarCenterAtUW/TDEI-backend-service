#!/bin/bash
# Runs once, when the database is first created (empty volume).
# Deploys every .sql in union/ (your content.tdei_union_dataset), then the
# harness export helper. Redeploy later with:  ./local_db.sh deploy union/<file>.sql
set -e
shopt -s nullglob
files=(/harness/union/*.sql)
if [ ${#files[@]} -eq 0 ]; then
    echo "harness: union/ is empty — deploy your union later with ./local_db.sh deploy <file>.sql"
fi
for f in "${files[@]}"; do
    echo "harness: deploying $f"
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q -f "$f"
done
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q -f /harness/sql/union_to_geojson.sql
echo "harness: content.tdei_union_dataset_geojson deployed"
