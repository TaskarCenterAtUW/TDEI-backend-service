-- =============================================================================
-- content.tdei_union_dataset_geojson — union 5.0 export helper (harness only).
--
-- Runs content.tdei_union_dataset(src_one, src_two, config) and drains each of
-- its cursors. One row per output:
--
--     file_name | feature_count | geojson
--     node / edge / zone / point / line / polygon   an OSW 0.3 FeatureCollection
--     report                                        the run report (one JSON object)
--
-- The union function itself is not modified. Deploying this drops the 4.x
-- helper signature, like union_5_0.sql drops the 4.x union signatures.
--
-- SCALE: each file is assembled as one jsonb value (PostgreSQL caps a jsonb
-- value at ~255 MB) — right for the harness and moderate datasets, not for
-- production-size output.
-- =============================================================================
DROP FUNCTION IF EXISTS content.tdei_union_dataset_geojson(CHARACTER VARYING, CHARACTER VARYING, REAL, JSONB);

CREATE OR REPLACE FUNCTION content.tdei_union_dataset_geojson(
    src_one_tdei_dataset_id  CHARACTER VARYING,
    src_two_tdei_dataset_id  CHARACTER VARYING,
    config                   JSONB DEFAULT '{}'::jsonb
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
    CREATE TEMP TABLE IF NOT EXISTS _union_gj_buf (
        seq      BIGSERIAL,
        feature  JSONB
    ) ON COMMIT DROP;

    IF to_regclass('pg_temp._union_gj_cursors') IS NOT NULL THEN
        DROP TABLE _union_gj_cursors;
    END IF;
    CREATE TEMP TABLE _union_gj_cursors ON COMMIT DROP AS
    SELECT u.file_name AS fname, u.cursor_ref AS cref
    FROM content.tdei_union_dataset(src_one_tdei_dataset_id, src_two_tdei_dataset_id, config) u;

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
        IF r.fname = 'report' THEN
            SELECT COUNT(*), (SELECT b.feature FROM _union_gj_buf b ORDER BY b.seq LIMIT 1)
              INTO feature_count, geojson FROM _union_gj_buf;
        ELSE
            SELECT COUNT(*),
                   jsonb_build_object(
                       '$schema',  osw_schema,
                       'type',     'FeatureCollection',
                       'features', COALESCE(jsonb_agg(b.feature ORDER BY b.seq), '[]'::jsonb))
              INTO feature_count, geojson
              FROM _union_gj_buf b;
        END IF;
        RETURN NEXT;
    END LOOP;
END;
$$;
