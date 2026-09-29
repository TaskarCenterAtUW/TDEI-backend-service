-- =============================================================================
-- content.tdei_union_dataset_geojson — deploy once per database.
--
-- Runs content.tdei_union_dataset and drains each of its six cursors into a
-- complete OSW 0.3 FeatureCollection. One row per file type:
--
--     file_name | feature_count | geojson
--     node / edge / zone / point / line / polygon
--
-- The union function itself is not modified. Works with the 3-argument union
-- and with the 4-argument one (entity_filters): filters are only passed on
-- when you give them.
--
-- SCALE: each file is assembled as one jsonb value (PostgreSQL caps a jsonb
-- value at ~255 MB) — right for the harness and moderate datasets, not for
-- production-size output.
-- =============================================================================

CREATE OR REPLACE FUNCTION content.tdei_union_dataset_geojson(
    src_one_tdei_dataset_id  CHARACTER VARYING,
    src_two_tdei_dataset_id  CHARACTER VARYING,
    proximity                REAL  DEFAULT 0.5,
    entity_filters           JSONB DEFAULT NULL
)
RETURNS TABLE(file_name TEXT, feature_count BIGINT, geojson JSONB)
LANGUAGE plpgsql
AS $$
DECLARE
    osw_schema CONSTANT TEXT :=
        'https://sidewalks.washington.edu/opensidewalks/0.3/schema.json';
    r     RECORD;
    c     REFCURSOR;   -- FETCH needs a cursor VARIABLE, not a record field
    feat  JSONB;
BEGIN
    -- Stage one file's features, then aggregate once (appending to a jsonb in
    -- a loop is O(n^2)). seq keeps the cursor's order.
    CREATE TEMP TABLE IF NOT EXISTS _union_gj_buf (
        seq      BIGSERIAL,
        feature  JSONB
    ) ON COMMIT DROP;

    IF to_regclass('pg_temp._union_gj_cursors') IS NOT NULL THEN
        DROP TABLE _union_gj_cursors;
    END IF;
    IF entity_filters IS NULL THEN
        CREATE TEMP TABLE _union_gj_cursors ON COMMIT DROP AS
        SELECT u.file_name AS fname, u.cursor_ref AS cref
        FROM content.tdei_union_dataset(src_one_tdei_dataset_id, src_two_tdei_dataset_id,
                                        proximity) u;
    ELSE
        CREATE TEMP TABLE _union_gj_cursors ON COMMIT DROP AS
        SELECT u.file_name AS fname, u.cursor_ref AS cref
        FROM content.tdei_union_dataset(src_one_tdei_dataset_id, src_two_tdei_dataset_id,
                                        proximity, entity_filters) u;
    END IF;

    FOR r IN SELECT fname, cref FROM _union_gj_cursors LOOP
        TRUNCATE _union_gj_buf;
        c := r.cref;
        LOOP
            FETCH c INTO feat;
            EXIT WHEN NOT FOUND;
            INSERT INTO _union_gj_buf (feature) VALUES (feat);
        END LOOP;
        CLOSE c;

        file_name := r.fname;
        SELECT COUNT(*),
               jsonb_build_object(
                   '$schema',  osw_schema,
                   'type',     'FeatureCollection',
                   'features', COALESCE(jsonb_agg(b.feature ORDER BY b.seq), '[]'::jsonb))
          INTO feature_count, geojson
          FROM _union_gj_buf b;
        RETURN NEXT;
    END LOOP;
END;
$$;
