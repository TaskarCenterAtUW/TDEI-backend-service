-- =============================================================================
-- content.tdei_union_dataset — release 5.0 (configurable operations)
--
--   content.tdei_union_dataset(src_one, src_two, config JSONB DEFAULT '{}')
--     RETURNS TABLE(file_name TEXT, cursor_ref REFCURSOR)
--     file_name: node, edge, zone, point, line, polygon, report
--
-- NOTHING IS IMPLICIT. Every snap, dedup and junction comes from `config`; with
-- no config the output is DS1 + DS2 unchanged. The config has four sections,
-- each with mode off / default / custom (a section left out is off):
--
--   snap       join a DS1 node and a DS2 node of the same type within a
--              distance; the non-anchor node moves onto the anchor node.
--   dedup      remove an edge that duplicates an edge of the anchor dataset.
--              A rule's max_angle (degrees, default 30) is how far the edge's
--              direction may differ from the anchor edge's.
--   connect    make junctions between two kinds of way, in rule order:
--              operation 'point'    — lines that cross get a node and both are
--                                     split there; an end that stops short gets
--                                     a node on the other line and a connector;
--              operation 'geometry' — a line END within reach of another line
--                                     gets a node on it (split there) and a
--                                     short connector edge; an end that crosses
--                                     the line and overshoots it meets it where
--                                     it crosses. Nothing moves.
--              A rule may set node_tags (written on the junction node it makes
--              or reuses; existing values win, differing ones are audited) and
--              edge_tags (written on the connector edges it adds). Each set is
--              written only where it is named.
--   extensions zones and extension points/lines/polygons: off, or default =
--              the 4.3.2 merging (no custom mode). In default mode `points`,
--              `lines`, `polygons` and `zones` (all true by default) switch each
--              type's merge on or off; a type that is off keeps both copies.
--              Points join within `proximity`; a DS2 Line merges when
--              `line_overlap` of it lies within `line_proximity` of a DS1 Line
--              (line_proximity defaults to proximity).
--
-- Stage order is fixed: snap → dedup → (snap topology) → connect → stranding
-- guard → zones and extensions → output and report.
-- Inside a stage the first rule listed that matches takes a location.
-- `default` modes run published, versioned rule sets (see
-- content.union5_validate_config); the run report returns the rules actually
-- used, per-rule counts and warnings.
--
-- Requirement: "Union 5.0 — Configurable Operations Requirement Spec".
-- Harness:     test/union-5.0-harness (profiles, cases, run_harness.sh).
--
-- PERFORMANCE (built for millions of features per dataset; every step's cost
-- grows linearly with the data — see the harness's docs/performance.md):
--   • every spatial search is a GIST index probe; no step compares all pairs.
--     Node identity is a hash join on exact coordinates; every lookup by id,
--     node or match is an index or hash join, never a scan per row.
--   • types are matched once per distinct tag signature, while loading.
--   • snap and dedup are one set-based pass per rule, connect one pass per
--     rule; the stranding guard re-checks only edges next to a restoration.
--   • working tables are temp tables, filled in one statement, indexed BEFORE
--     anything updates them (an index built after an update in the same
--     transaction is unusable until it ends), and analysed.
--   • dedup buffers only the anchor edges a candidate can reach, and settles
--     coverage with the nearest anchor edge's buffer when that suffices.
--   • work_mem / maintenance_work_mem are raised for this transaction only
--     (never lowered), JIT is off. The report gives seconds per stage
--     (timings_s). Ties are broken explicitly, so the output is deterministic.
--
-- GUARDS (always on; none of them creates a connection):
--   • anchors: in snap and dedup one side stays fixed (DS1 unless a rule says
--     ds2); a kerb never moves; nothing moves twice.
--   • connect acts on DS1 × DS2 pairs only; lines whose layer/level/bridge/
--     tunnel differ never join; elements a connect rule created are never
--     moved, split or removed by a later rule.
--   • connectivity guard: an edge whose two ends are both on the anchor network
--     is removed as a duplicate only when the anchor edges inside its buffer
--     already join those two ends (or it joins the same two nodes as one).
--   • stranding guard: a removed duplicate is restored when its removal left a
--     surviving edge hanging off a node that is not on the anchor network.
--   • exact node identity: edge ends are matched to nodes by exact coordinate.
--   • nodes the union left unreferenced are dropped; input orphans are kept.
--
-- IDS: existing nodes keep their database id as output _id (as 4.x); created
-- nodes get ids above the highest input node id; edges are renumbered. Audit
-- fields that name other features carry their ORIGINAL GeoJSON _id.
--
-- Deploying this drops the 4.x signatures (no backward compatibility).
-- =============================================================================

DROP FUNCTION IF EXISTS content.tdei_union_dataset(CHARACTER VARYING, CHARACTER VARYING, REAL, JSONB);
DROP FUNCTION IF EXISTS content.tdei_union_dataset(CHARACTER VARYING, CHARACTER VARYING, REAL);

-- ─────────────────────────────────────────────────────────────────────────────
-- Helpers
-- ─────────────────────────────────────────────────────────────────────────────

-- Ground distance in metres between two points (SRID 4326 geometry). Inlined.
CREATE OR REPLACE FUNCTION content.union5_dist_m(a geometry, b geometry, m_per_deg_lon float8)
RETURNS float8 LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT sqrt(power((ST_X(b) - ST_X(a)) * m_per_deg_lon, 2) + power((ST_Y(b) - ST_Y(a)) * 111111.0, 2))
$$;

-- Built-in type names, as normalised clause lists (see union5_normalize_type).
CREATE OR REPLACE FUNCTION content.union5_builtin_type(type_name text)
RETURNS jsonb LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT (CASE type_name
        WHEN 'crossing'      THEN '[{"all":{"highway":["footway"],"footway":["crossing","traffic_island"]}}]'
        WHEN 'sidewalk'      THEN '[{"all":{"highway":["footway"],"footway":["sidewalk"]}}]'
        WHEN 'pedestrian'    THEN '[{"all":{"highway":["footway","pedestrian","steps"]},"none":{"footway":["crossing","traffic_island"]}}]'
        WHEN 'living_street' THEN '[{"all":{"highway":["living_street"]}}]'
        WHEN 'road'          THEN '[{"all":{"highway":["primary","secondary","tertiary","residential","service","unclassified","trunk","motorway"]}}]'
        WHEN 'other'         THEN '[{"none":{"highway":["footway","pedestrian","steps","living_street","primary","secondary","tertiary","residential","service","unclassified","trunk","motorway"]}}]'
    END)::jsonb
$$;

-- Validate a type spec and normalise it to a clause list:
--   [ {"all": {key: [values] | "*"}, "none": {key: [values]}} , ... ]   (clauses OR-ed)
-- A spec is a built-in name, a tag-match object (keys AND-ed; a list value = any
-- of; "*" = key present), or a non-empty list of those (OR-ed).
CREATE OR REPLACE FUNCTION content.union5_normalize_type(spec jsonb, path text)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    out_clauses jsonb := '[]'::jsonb;
    el jsonb; kv record; obj jsonb := '{}'::jsonb; b jsonb;
BEGIN
    IF spec IS NULL OR jsonb_typeof(spec) = 'null' THEN
        RAISE EXCEPTION 'config.%: a type is required', path USING ERRCODE = '22023';
    ELSIF jsonb_typeof(spec) = 'string' THEN
        b := content.union5_builtin_type(spec #>> '{}');
        IF b IS NULL THEN
            RAISE EXCEPTION 'config.%: unknown type name "%" — use a tag match such as {"highway": "footway"} or one of crossing, sidewalk, pedestrian, living_street, road, other', path, spec #>> '{}'
                USING ERRCODE = '22023';
        END IF;
        RETURN b;
    ELSIF jsonb_typeof(spec) = 'object' THEN
        IF spec = '{}'::jsonb THEN
            RAISE EXCEPTION 'config.%: an empty tag match {} would match everything — name at least one key', path USING ERRCODE = '22023';
        END IF;
        FOR kv IN SELECT key, value FROM jsonb_each(spec) LOOP
            IF kv.key = '' THEN
                RAISE EXCEPTION 'config.%: empty key in a tag match', path USING ERRCODE = '22023';
            ELSIF jsonb_typeof(kv.value) = 'string' AND kv.value #>> '{}' = '*' THEN
                obj := obj || jsonb_build_object(kv.key, '*');
            ELSIF jsonb_typeof(kv.value) = 'string' THEN
                obj := obj || jsonb_build_object(kv.key, jsonb_build_array(kv.value));
            ELSIF jsonb_typeof(kv.value) = 'array' AND jsonb_array_length(kv.value) > 0
                  AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(kv.value) x WHERE jsonb_typeof(x) <> 'string') THEN
                obj := obj || jsonb_build_object(kv.key, kv.value);
            ELSE
                RAISE EXCEPTION 'config.%.%: a tag value must be a string, a non-empty list of strings, or "*"', path, kv.key
                    USING ERRCODE = '22023';
            END IF;
        END LOOP;
        RETURN jsonb_build_array(jsonb_build_object('all', obj));
    ELSIF jsonb_typeof(spec) = 'array' THEN
        IF jsonb_array_length(spec) = 0 THEN
            RAISE EXCEPTION 'config.%: an empty list of types matches nothing', path USING ERRCODE = '22023';
        END IF;
        FOR el IN SELECT value FROM jsonb_array_elements(spec) LOOP
            IF jsonb_typeof(el) NOT IN ('string', 'object') THEN
                RAISE EXCEPTION 'config.%: each list entry must be a type name or a tag match', path USING ERRCODE = '22023';
            END IF;
            out_clauses := out_clauses || content.union5_normalize_type(el, path);
        END LOOP;
        RETURN out_clauses;
    END IF;
    RAISE EXCEPTION 'config.%: a type must be a type name, a tag match object, or a list of them', path USING ERRCODE = '22023';
END $$;

-- Does a property set match a normalised clause list? With positive_only, a
-- clause needs at least one 'all' condition (a node's own tags can only
-- identify it positively; 'other' is a pure negative and never matches a node
-- by its own tags).
CREATE OR REPLACE FUNCTION content.union5_tag_match(p jsonb, clauses jsonb, positive_only boolean)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT EXISTS (
        SELECT 1 FROM jsonb_array_elements(clauses) c
        WHERE (NOT positive_only OR COALESCE(c->'all', '{}'::jsonb) <> '{}'::jsonb)
          AND NOT EXISTS (
                SELECT 1 FROM jsonb_each(COALESCE(c->'all', '{}'::jsonb)) a
                WHERE NOT CASE WHEN a.value = '"*"'::jsonb THEN p ? a.key
                               ELSE COALESCE(p->>a.key IN (SELECT jsonb_array_elements_text(a.value)), FALSE) END)
          AND NOT EXISTS (
                SELECT 1 FROM jsonb_each(COALESCE(c->'none', '{}'::jsonb)) n
                WHERE COALESCE(p->>n.key IN (SELECT jsonb_array_elements_text(n.value)), FALSE)))
$$;

-- OSW keys written as given on created elements; any other key becomes ext:<key>.
-- Taken from the TDEI content schema's generated columns (node / edge).
CREATE OR REPLACE FUNCTION content.union5_osw_key_ok(kind text, k text)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT k LIKE 'ext:%' OR CASE kind
        WHEN 'node' THEN k IN ('barrier', 'kerb', 'tactile_paving')
        WHEN 'edge' THEN k IN ('highway', 'footway', 'name', 'description', 'surface', 'width', 'incline',
                               'length', 'foot', 'service', 'tactile_paving', 'crossing:markings',
                               'step_count', 'climb', 'building', 'opening_hours')
        ELSE FALSE END
$$;

-- Rule tags as written on a created/reused element of a kind.
CREATE OR REPLACE FUNCTION content.union5_tags_for(kind text, tags jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT COALESCE(jsonb_object_agg(CASE WHEN content.union5_osw_key_ok(kind, t.key) THEN t.key
                                          ELSE 'ext:' || t.key END,
                                     CASE WHEN jsonb_typeof(t.value) = 'string' THEN t.value
                                          ELSE to_jsonb(t.value #>> '{}') END ORDER BY t.key), '{}'::jsonb)
    FROM jsonb_each(COALESCE(tags, '{}'::jsonb)) t
$$;

-- ── config validation building blocks ───────────────────────────────────────
CREATE OR REPLACE FUNCTION content.union5_cfg_keys(obj jsonb, path text, allowed text[])
RETURNS void LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE k text; where_ text := CASE WHEN path = '' THEN 'config' ELSE 'config.' || path END;
BEGIN
    IF obj IS NULL OR jsonb_typeof(obj) <> 'object' THEN
        RAISE EXCEPTION '%: must be a JSON object', where_ USING ERRCODE = '22023';
    END IF;
    FOR k IN SELECT jsonb_object_keys(obj) LOOP
        IF NOT k = ANY(allowed) THEN
            IF allowed = ARRAY['mode'] THEN
                RAISE EXCEPTION '%: mode off takes no other settings (got "%")', where_, k USING ERRCODE = '22023';
            END IF;
            RAISE EXCEPTION '%: unknown % "%" — expected one of %', where_,
                CASE WHEN path = '' THEN 'section' ELSE 'setting' END, k, array_to_string(allowed, ', ')
                USING ERRCODE = '22023';
        END IF;
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION content.union5_cfg_num(v jsonb, path text, lo float8, lo_inclusive boolean, hi float8, dflt float8)
RETURNS float8 LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE x float8;
BEGIN
    IF v IS NULL THEN RETURN dflt; END IF;
    IF jsonb_typeof(v) <> 'number' THEN
        RAISE EXCEPTION 'config.%: must be a number (got %)', path, v USING ERRCODE = '22023';
    END IF;
    x := (v #>> '{}')::float8;
    IF (lo_inclusive AND x < lo) OR (NOT lo_inclusive AND x <= lo) OR x > hi THEN
        RAISE EXCEPTION 'config.%: must be % % and at most % (got %)', path,
            CASE WHEN lo_inclusive THEN 'at least' ELSE 'greater than' END, lo, hi, x USING ERRCODE = '22023';
    END IF;
    RETURN x;
END $$;

CREATE OR REPLACE FUNCTION content.union5_cfg_enum(v jsonb, path text, allowed text[], dflt text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF v IS NULL THEN
        IF dflt IS NULL THEN
            RAISE EXCEPTION 'config.%: is required — one of %', path, array_to_string(allowed, ', ') USING ERRCODE = '22023';
        END IF;
        RETURN dflt;
    END IF;
    IF jsonb_typeof(v) <> 'string' OR NOT (v #>> '{}') = ANY(allowed) THEN
        RAISE EXCEPTION 'config.%: expected one of %, got %', path, array_to_string(allowed, ', '), v USING ERRCODE = '22023';
    END IF;
    RETURN v #>> '{}';
END $$;

CREATE OR REPLACE FUNCTION content.union5_cfg_bool(v jsonb, path text, dflt boolean)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF v IS NULL THEN RETURN dflt; END IF;
    IF jsonb_typeof(v) <> 'boolean' THEN
        RAISE EXCEPTION 'config.%: must be true or false (got %)', path, v USING ERRCODE = '22023';
    END IF;
    RETURN (v #>> '{}')::boolean;
END $$;

CREATE OR REPLACE FUNCTION content.union5_cfg_name(v jsonb, path text, dflt text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF v IS NULL THEN
        IF dflt IS NULL THEN RAISE EXCEPTION 'config.%: a rule name is required', path USING ERRCODE = '22023'; END IF;
        RETURN dflt;
    END IF;
    IF jsonb_typeof(v) <> 'string' OR length(v #>> '{}') = 0 OR length(v #>> '{}') > 64
       OR (v #>> '{}') ~ '[,]' THEN
        RAISE EXCEPTION 'config.%: a rule name must be 1-64 characters, without commas (got %)', path, v USING ERRCODE = '22023';
    END IF;
    RETURN v #>> '{}';
END $$;

CREATE OR REPLACE FUNCTION content.union5_cfg_tags(v jsonb, path text)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE kv record;
BEGIN
    IF v IS NULL THEN RETURN '{}'::jsonb; END IF;
    IF jsonb_typeof(v) <> 'object' THEN
        RAISE EXCEPTION 'config.%: tags must be an object of key: value', path USING ERRCODE = '22023';
    END IF;
    FOR kv IN SELECT key, value FROM jsonb_each(v) LOOP
        IF kv.key = '' OR kv.key IN ('_id', '_u_id', '_v_id', '_w_id') OR kv.key LIKE 'ext:union\_%' THEN
            RAISE EXCEPTION 'config.%.%: this key is reserved and cannot be set by a rule', path, kv.key USING ERRCODE = '22023';
        END IF;
        IF jsonb_typeof(kv.value) NOT IN ('string', 'number', 'boolean') THEN
            RAISE EXCEPTION 'config.%.%: a tag value must be a string, number or boolean', path, kv.key USING ERRCODE = '22023';
        END IF;
    END LOOP;
    RETURN v;
END $$;

-- ─────────────────────────────────────────────────────────────────────────────
-- content.union5_validate_config(config) → effective config
--
-- Rejects anything unknown or out of range with a message naming the path
-- (config.<section>.rules[<n>].<field>), and returns the EFFECTIVE config:
-- every default filled in and every `default` mode expanded into its published
-- rule set. Usable on its own to check a config before submitting a job.
--
-- Published default rule sets (release 5.0):
--   snap    coincident on; then crossing, pedestrian, living_street, road,
--           other — each to its own type at `proximity`, anchor ds1, merge
--   dedup   crossing, pedestrian, living_street, road, other — each against its
--           own type, buffer `proximity`, min_overlap 0.8, max_angle 30, anchor ds1
--   connect road-crossing (crossing → road, point); crossing-sidewalk,
--           crossing-living-street, pedestrian-pedestrian,
--           living-street-living-street, crossing-crossing (geometry) — all at
--           `proximity`, no node_tags / edge_tags
--   `proximity` defaults to 0.5 m everywhere.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION content.union5_validate_config(config jsonb)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    c jsonb := COALESCE(config, '{}'::jsonb);
    st jsonb; r jsonb; i int; md text; path text; nm text;
    prox float8; anchor text; attrs text; min_ov float8; max_ang float8; coincident boolean;
    rules jsonb; eff jsonb := '{}'::jsonb; names text[]; g text;
    groups CONSTANT text[] := ARRAY['crossing', 'pedestrian', 'living_street', 'road', 'other'];
    max_rules CONSTANT int := 50;
BEGIN
    PERFORM content.union5_cfg_keys(c, '', ARRAY['snap', 'dedup', 'connect', 'extensions']);

    -- ── snap ──
    st := c->'snap';
    IF st IS NULL THEN
        eff := eff || jsonb_build_object('snap', jsonb_build_object('mode', 'off', 'coincident', false, 'rules', '[]'::jsonb));
    ELSE
        IF jsonb_typeof(st) <> 'object' THEN RAISE EXCEPTION 'config.snap: must be a JSON object' USING ERRCODE = '22023'; END IF;
        md := content.union5_cfg_enum(st->'mode', 'snap.mode', ARRAY['off', 'default', 'custom'], NULL);
        IF md = 'off' THEN
            PERFORM content.union5_cfg_keys(st, 'snap', ARRAY['mode']);
            eff := eff || jsonb_build_object('snap', jsonb_build_object('mode', 'off', 'coincident', false, 'rules', '[]'::jsonb));
        ELSE
            PERFORM content.union5_cfg_keys(st, 'snap', CASE WHEN md = 'default'
                THEN ARRAY['mode', 'proximity', 'anchor', 'attributes', 'coincident']
                ELSE ARRAY['mode', 'proximity', 'anchor', 'attributes', 'coincident', 'others', 'rules'] END);
            prox   := content.union5_cfg_num(st->'proximity', 'snap.proximity', 0, true, 5, 0.5);
            anchor := content.union5_cfg_enum(st->'anchor', 'snap.anchor', ARRAY['ds1', 'ds2'], 'ds1');
            attrs  := content.union5_cfg_enum(st->'attributes', 'snap.attributes', ARRAY['merge', 'anchor_only'], 'merge');
            coincident := content.union5_cfg_bool(st->'coincident', 'snap.coincident', md = 'default');
            rules := '[]'::jsonb;
            IF md = 'default' THEN
                FOREACH g IN ARRAY groups LOOP
                    rules := rules || jsonb_build_array(jsonb_build_object(
                        'name', g, 'type', g, 'proximity', prox, 'anchor', anchor, 'attributes', attrs));
                END LOOP;
                eff := eff || jsonb_build_object('snap', jsonb_build_object(
                    'mode', md, 'coincident', coincident, 'anchor', anchor, 'attributes', attrs,
                    'rules', rules, 'others', NULL));
            ELSE
                IF st ? 'rules' AND jsonb_typeof(st->'rules') <> 'array' THEN
                    RAISE EXCEPTION 'config.snap.rules: must be a list of rules' USING ERRCODE = '22023';
                END IF;
                IF COALESCE(jsonb_array_length(st->'rules'), 0) = 0 AND NOT coincident AND NOT st ? 'others' THEN
                    RAISE EXCEPTION 'config.snap.rules: custom mode needs at least one rule (or coincident / others)' USING ERRCODE = '22023';
                END IF;
                IF COALESCE(jsonb_array_length(st->'rules'), 0) > max_rules THEN
                    RAISE EXCEPTION 'config.snap.rules: at most % rules per stage (got %)', max_rules, jsonb_array_length(st->'rules') USING ERRCODE = '22023';
                END IF;
                names := ARRAY[]::text[];
                FOR i IN 0 .. COALESCE(jsonb_array_length(st->'rules'), 0) - 1 LOOP
                    r := st->'rules'->i; path := format('snap.rules[%s]', i + 1);
                    PERFORM content.union5_cfg_keys(r, path, ARRAY['name', 'type', 'proximity', 'anchor', 'attributes']);
                    nm := content.union5_cfg_name(r->'name', path || '.name', 'snap-' || (i + 1));
                    IF nm = ANY(names) THEN RAISE EXCEPTION 'config.%.name: "%" is used by an earlier snap rule', path, nm USING ERRCODE = '22023'; END IF;
                    names := names || nm;
                    PERFORM content.union5_normalize_type(r->'type', path || '.type');
                    rules := rules || jsonb_build_array(jsonb_build_object(
                        'name', nm, 'type', r->'type',
                        'proximity', content.union5_cfg_num(r->'proximity', path || '.proximity', 0, true, 5, prox),
                        'anchor', content.union5_cfg_enum(r->'anchor', path || '.anchor', ARRAY['ds1', 'ds2'], anchor),
                        'attributes', content.union5_cfg_enum(r->'attributes', path || '.attributes', ARRAY['merge', 'anchor_only'], attrs)));
                END LOOP;
                eff := eff || jsonb_build_object('snap', jsonb_build_object(
                    'mode', md, 'coincident', coincident, 'anchor', anchor, 'attributes', attrs, 'rules', rules,
                    'others', CASE WHEN st ? 'others' THEN jsonb_build_object(
                        'proximity', content.union5_cfg_num(st->'others', 'snap.others', 0, true, 5, NULL),
                        'anchor', anchor, 'attributes', attrs) END));
            END IF;
        END IF;
    END IF;

    -- ── dedup ──
    st := c->'dedup';
    IF st IS NULL THEN
        eff := eff || jsonb_build_object('dedup', jsonb_build_object('mode', 'off', 'rules', '[]'::jsonb));
    ELSE
        IF jsonb_typeof(st) <> 'object' THEN RAISE EXCEPTION 'config.dedup: must be a JSON object' USING ERRCODE = '22023'; END IF;
        md := content.union5_cfg_enum(st->'mode', 'dedup.mode', ARRAY['off', 'default', 'custom'], NULL);
        IF md = 'off' THEN
            PERFORM content.union5_cfg_keys(st, 'dedup', ARRAY['mode']);
            eff := eff || jsonb_build_object('dedup', jsonb_build_object('mode', 'off', 'rules', '[]'::jsonb));
        ELSE
            PERFORM content.union5_cfg_keys(st, 'dedup', CASE WHEN md = 'default'
                THEN ARRAY['mode', 'proximity', 'min_overlap', 'max_angle', 'anchor']
                ELSE ARRAY['mode', 'proximity', 'min_overlap', 'max_angle', 'anchor', 'rules'] END);
            prox   := content.union5_cfg_num(st->'proximity', 'dedup.proximity', 0, false, 5, 0.5);
            min_ov := content.union5_cfg_num(st->'min_overlap', 'dedup.min_overlap', 0, false, 1, 0.8);
            -- max_angle: degrees between the edge and the anchor edge (above 0, at most 90)
            max_ang := content.union5_cfg_num(st->'max_angle', 'dedup.max_angle', 0, false, 90, 30);
            anchor := content.union5_cfg_enum(st->'anchor', 'dedup.anchor', ARRAY['ds1', 'ds2'], 'ds1');
            rules := '[]'::jsonb;
            IF md = 'default' THEN
                FOREACH g IN ARRAY groups LOOP
                    rules := rules || jsonb_build_array(jsonb_build_object(
                        'name', g, 'from', g, 'to', g, 'proximity', prox, 'min_overlap', min_ov, 'max_angle', max_ang, 'anchor', anchor));
                END LOOP;
            ELSE
                IF jsonb_typeof(st->'rules') IS DISTINCT FROM 'array' OR jsonb_array_length(st->'rules') = 0 THEN
                    RAISE EXCEPTION 'config.dedup.rules: custom mode needs a non-empty list of rules' USING ERRCODE = '22023';
                END IF;
                IF jsonb_array_length(st->'rules') > max_rules THEN
                    RAISE EXCEPTION 'config.dedup.rules: at most % rules per stage (got %)', max_rules, jsonb_array_length(st->'rules') USING ERRCODE = '22023';
                END IF;
                names := ARRAY[]::text[];
                FOR i IN 0 .. jsonb_array_length(st->'rules') - 1 LOOP
                    r := st->'rules'->i; path := format('dedup.rules[%s]', i + 1);
                    PERFORM content.union5_cfg_keys(r, path, ARRAY['name', 'from', 'to', 'proximity', 'min_overlap', 'max_angle', 'anchor']);
                    nm := content.union5_cfg_name(r->'name', path || '.name', 'dedup-' || (i + 1));
                    IF nm = ANY(names) THEN RAISE EXCEPTION 'config.%.name: "%" is used by an earlier dedup rule', path, nm USING ERRCODE = '22023'; END IF;
                    names := names || nm;
                    PERFORM content.union5_normalize_type(r->'from', path || '.from');
                    PERFORM content.union5_normalize_type(r->'to', path || '.to');
                    rules := rules || jsonb_build_array(jsonb_build_object(
                        'name', nm, 'from', r->'from', 'to', r->'to',
                        'proximity', content.union5_cfg_num(r->'proximity', path || '.proximity', 0, false, 5, prox),
                        'min_overlap', content.union5_cfg_num(r->'min_overlap', path || '.min_overlap', 0, false, 1, min_ov),
                        'max_angle', content.union5_cfg_num(r->'max_angle', path || '.max_angle', 0, false, 90, max_ang),
                        'anchor', content.union5_cfg_enum(r->'anchor', path || '.anchor', ARRAY['ds1', 'ds2'], anchor)));
                END LOOP;
            END IF;
            eff := eff || jsonb_build_object('dedup', jsonb_build_object('mode', md, 'rules', rules));
        END IF;
    END IF;

    -- ── connect ──
    st := c->'connect';
    IF st IS NULL THEN
        eff := eff || jsonb_build_object('connect', jsonb_build_object('mode', 'off', 'rules', '[]'::jsonb));
    ELSE
        IF jsonb_typeof(st) <> 'object' THEN RAISE EXCEPTION 'config.connect: must be a JSON object' USING ERRCODE = '22023'; END IF;
        md := content.union5_cfg_enum(st->'mode', 'connect.mode', ARRAY['off', 'default', 'custom'], NULL);
        IF md = 'off' THEN
            PERFORM content.union5_cfg_keys(st, 'connect', ARRAY['mode']);
            eff := eff || jsonb_build_object('connect', jsonb_build_object('mode', 'off', 'rules', '[]'::jsonb));
        ELSE
            PERFORM content.union5_cfg_keys(st, 'connect', CASE WHEN md = 'default'
                THEN ARRAY['mode', 'proximity'] ELSE ARRAY['mode', 'proximity', 'rules'] END);
            prox := content.union5_cfg_num(st->'proximity', 'connect.proximity', 0, false, 5, 0.5);
            IF md = 'default' THEN
                rules := jsonb_build_array(
                    jsonb_build_object('name', 'road-crossing',               'from', 'crossing',      'to', 'road',          'operation', 'point',    'proximity', prox, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb),
                    jsonb_build_object('name', 'crossing-sidewalk',           'from', 'crossing',      'to', 'sidewalk',      'operation', 'geometry', 'proximity', prox, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb),
                    jsonb_build_object('name', 'crossing-living-street',      'from', 'crossing',      'to', 'living_street', 'operation', 'geometry', 'proximity', prox, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb),
                    jsonb_build_object('name', 'pedestrian-pedestrian',       'from', 'pedestrian',    'to', 'pedestrian',    'operation', 'geometry', 'proximity', prox, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb),
                    jsonb_build_object('name', 'living-street-living-street', 'from', 'living_street', 'to', 'living_street', 'operation', 'geometry', 'proximity', prox, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb),
                    jsonb_build_object('name', 'crossing-crossing',           'from', 'crossing',      'to', 'crossing',      'operation', 'geometry', 'proximity', prox, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb));
            ELSE
                IF jsonb_typeof(st->'rules') IS DISTINCT FROM 'array' OR jsonb_array_length(st->'rules') = 0 THEN
                    RAISE EXCEPTION 'config.connect.rules: custom mode needs a non-empty list of rules' USING ERRCODE = '22023';
                END IF;
                IF jsonb_array_length(st->'rules') > max_rules THEN
                    RAISE EXCEPTION 'config.connect.rules: at most % rules per stage (got %)', max_rules, jsonb_array_length(st->'rules') USING ERRCODE = '22023';
                END IF;
                rules := '[]'::jsonb; names := ARRAY[]::text[];
                FOR i IN 0 .. jsonb_array_length(st->'rules') - 1 LOOP
                    r := st->'rules'->i; path := format('connect.rules[%s]', i + 1);
                    IF r ? 'tags' THEN
                        RAISE EXCEPTION 'config.%.tags: a connect rule takes node_tags (for the junction node) and edge_tags (for the connector edge) instead of tags', path USING ERRCODE = '22023';
                    END IF;
                    PERFORM content.union5_cfg_keys(r, path, ARRAY['name', 'from', 'to', 'operation', 'proximity', 'node_tags', 'edge_tags']);
                    nm := content.union5_cfg_name(r->'name', path || '.name', NULL);
                    IF nm = ANY(names) THEN RAISE EXCEPTION 'config.%.name: "%" is used by an earlier connect rule — names must be unique', path, nm USING ERRCODE = '22023'; END IF;
                    names := names || nm;
                    PERFORM content.union5_normalize_type(r->'from', path || '.from');
                    PERFORM content.union5_normalize_type(r->'to', path || '.to');
                    rules := rules || jsonb_build_array(jsonb_build_object(
                        'name', nm, 'from', r->'from', 'to', r->'to',
                        'operation', content.union5_cfg_enum(r->'operation', path || '.operation', ARRAY['point', 'geometry'], NULL),
                        'proximity', content.union5_cfg_num(r->'proximity', path || '.proximity', 0, false, 5, prox),
                        'node_tags', content.union5_cfg_tags(r->'node_tags', path || '.node_tags'),
                        'edge_tags', content.union5_cfg_tags(r->'edge_tags', path || '.edge_tags')));
                END LOOP;
            END IF;
            eff := eff || jsonb_build_object('connect', jsonb_build_object('mode', md, 'rules', rules));
        END IF;
    END IF;

    -- ── extensions ──
    st := c->'extensions';
    IF st IS NULL THEN
        eff := eff || jsonb_build_object('extensions', jsonb_build_object('mode', 'off'));
    ELSE
        IF jsonb_typeof(st) <> 'object' THEN RAISE EXCEPTION 'config.extensions: must be a JSON object' USING ERRCODE = '22023'; END IF;
        md := content.union5_cfg_enum(st->'mode', 'extensions.mode', ARRAY['off', 'default'], NULL);
        IF md = 'off' THEN
            PERFORM content.union5_cfg_keys(st, 'extensions', ARRAY['mode']);
            eff := eff || jsonb_build_object('extensions', jsonb_build_object('mode', 'off'));
        ELSE
            PERFORM content.union5_cfg_keys(st, 'extensions', ARRAY['mode', 'points', 'lines', 'polygons', 'zones',
                                                                    'proximity', 'line_proximity', 'line_overlap', 'polygon_overlap', 'zone_overlap']);
            -- each type can be switched off on its own (both copies kept); all four on by default
            IF NOT (content.union5_cfg_bool(st->'points', 'extensions.points', true) OR content.union5_cfg_bool(st->'lines', 'extensions.lines', true)
                    OR content.union5_cfg_bool(st->'polygons', 'extensions.polygons', true) OR content.union5_cfg_bool(st->'zones', 'extensions.zones', true)) THEN
                RAISE EXCEPTION 'config.extensions: points, lines, polygons and zones are all off — use "mode": "off" instead' USING ERRCODE = '22023';
            END IF;
            -- proximity: how far apart two Points may be; line_proximity: the width of the
            -- buffer around a DS1 Line (defaults to proximity)
            prox := content.union5_cfg_num(st->'proximity', 'extensions.proximity', 0, false, 5, 0.5);
            eff := eff || jsonb_build_object('extensions', jsonb_build_object('mode', 'default',
                'points',          content.union5_cfg_bool(st->'points', 'extensions.points', true),
                'lines',           content.union5_cfg_bool(st->'lines', 'extensions.lines', true),
                'polygons',        content.union5_cfg_bool(st->'polygons', 'extensions.polygons', true),
                'zones',           content.union5_cfg_bool(st->'zones', 'extensions.zones', true),
                'proximity',       prox,
                'line_proximity',  content.union5_cfg_num(st->'line_proximity', 'extensions.line_proximity', 0, false, 5, prox),
                'line_overlap',    content.union5_cfg_num(st->'line_overlap', 'extensions.line_overlap', 0, false, 1, 0.7),
                'polygon_overlap', content.union5_cfg_num(st->'polygon_overlap', 'extensions.polygon_overlap', 0, false, 1, 0.7),
                'zone_overlap',    content.union5_cfg_num(st->'zone_overlap', 'extensions.zone_overlap', 0, false, 1, 0.7)));
        END IF;
    END IF;
    RETURN eff;
END $$;

-- Merge two audit objects: a key present in both keeps both values, comma-separated.
CREATE OR REPLACE FUNCTION content.union5_audit_add(a jsonb, b jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT COALESCE(a, '{}'::jsonb) || COALESCE((
        SELECT jsonb_object_agg(k, CASE WHEN a ? k AND (a->>k) <> (b->>k) THEN to_jsonb((a->>k) || ',' || (b->>k))
                                        ELSE b->k END)
        FROM jsonb_object_keys(COALESCE(b, '{}'::jsonb)) k), '{}'::jsonb)
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- content.union5_apply_splits(next_eid) — split edges at the requests queued in
-- the running union's temp table u5_split(eid, frac, nid). Each piece runs
-- between consecutive split fractions and its end vertices are set EXACTLY to
-- the node positions, so an edge end always equals the node it references.
-- Pieces keep the edge's tags, type and dataset (they gain nothing); the split
-- edge is retired. Returns the next free edge id.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION content.union5_apply_splits(next_eid bigint)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE n_new bigint;
BEGIN
    DROP TABLE IF EXISTS u5_sp;
    CREATE TEMP TABLE u5_sp ON COMMIT DROP AS
    SELECT DISTINCT ON (s.eid, s.nid) s.eid, s.frac, s.nid
    FROM u5_split s JOIN u5_e e ON e.eid = s.eid AND e.alive
    WHERE s.frac > 1e-12 AND s.frac < 1 - 1e-12 AND s.nid <> e.u AND s.nid <> e.v
    ORDER BY s.eid, s.nid, s.frac;
    DELETE FROM u5_sp a USING u5_sp b                      -- one node per fraction
    WHERE a.eid = b.eid AND a.frac = b.frac AND a.nid > b.nid;
    IF NOT EXISTS (SELECT 1 FROM u5_sp) THEN
        TRUNCATE u5_split;
        RETURN next_eid;
    END IF;

    DROP TABLE IF EXISTS u5_sp_pieces;
    CREATE TEMP TABLE u5_sp_pieces ON COMMIT DROP AS
    SELECT p.eid, p.frac AS f0, p.nid AS n0,
           LEAD(p.frac) OVER w AS f1, LEAD(p.nid) OVER w AS n1,
           ROW_NUMBER() OVER w AS k
    FROM (SELECT e.eid, 0.0::float8 AS frac, e.u AS nid FROM u5_e e WHERE e.eid IN (SELECT eid FROM u5_sp)
          UNION ALL
          SELECT e.eid, 1.0::float8, e.v FROM u5_e e WHERE e.eid IN (SELECT eid FROM u5_sp)
          UNION ALL
          SELECT eid, frac, nid FROM u5_sp) p
    WINDOW w AS (PARTITION BY p.eid ORDER BY p.frac);

    INSERT INTO u5_e (eid, src, orig_id, geom, props, u, v, alive, tm, is_connector, rule, parent, levelkey)
    SELECT next_eid + ROW_NUMBER() OVER (ORDER BY x.eid, x.k), x.src, x.orig_id, x.g, x.props, x.n0, x.n1,
           true, x.tm, x.is_connector, x.rule, x.parent, x.levelkey
    FROM (
        SELECT pc.eid, pc.k, e.src, e.orig_id, e.props, pc.n0, pc.n1, e.tm, e.is_connector, e.rule,
               COALESCE(e.parent, e.eid) AS parent, e.levelkey,
               ST_SetPoint(ST_SetPoint(sub.g, 0, n0.geom), ST_NPoints(sub.g) - 1, n1.geom) AS g
        FROM u5_sp_pieces pc
        JOIN u5_e e  ON e.eid = pc.eid
        JOIN u5_n n0 ON n0.nid = pc.n0
        JOIN u5_n n1 ON n1.nid = pc.n1
        CROSS JOIN LATERAL (SELECT ST_RemoveRepeatedPoints(ST_LineSubstring(e.geom, pc.f0, pc.f1), 1e-10) AS g) sub
        WHERE pc.f1 IS NOT NULL AND pc.f1 > pc.f0 AND pc.n0 <> pc.n1
          AND GeometryType(sub.g) = 'LINESTRING' AND ST_NPoints(sub.g) >= 2) x;
    GET DIAGNOSTICS n_new = ROW_COUNT;

    UPDATE u5_e SET alive = false WHERE eid IN (SELECT eid FROM u5_sp);
    TRUNCATE u5_split;
    RETURN next_eid + n_new + 1;
END $$;

-- ─────────────────────────────────────────────────────────────────────────────
-- content.tdei_union_dataset — union 5.0
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION content.tdei_union_dataset(
    src_one_tdei_dataset_id  CHARACTER VARYING,
    src_two_tdei_dataset_id  CHARACTER VARYING,
    config                   JSONB DEFAULT '{}'::jsonb
)
RETURNS TABLE(file_name TEXT, cursor_ref REFCURSOR)
LANGUAGE plpgsql
VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_ds1 CONSTANT text := src_one_tdei_dataset_id;
    v_ds2 CONSTANT text := src_two_tdei_dataset_id;
    v_cfg       jsonb;
    v_rule      jsonb;
    v_i         int;
    r           record;
    v_mlat      CONSTANT float8 := 111111.0;
    v_mlon      float8;
    v_tol       CONSTANT float8 := 1e-8;    -- "lies on": vertex identity for 7 dp data
    v_grid      CONSTANT float8 := 1e-7;    -- created nodes are placed on the 7 dp grid
    v_next_eid  bigint;
    v_next_nid  bigint;
    v_first_nid bigint;
    v_n         bigint;
    v_g_tids    int[];                      -- the five type groups (snap 'others')
    v_keys      text[];
    v_norm      jsonb;
    v_tid       int;
    v_tid2      int;
    v_ntags     jsonb;
    v_warnings  text[] := ARRAY[]::text[];
    v_report    jsonb;
    v_ext       jsonb;
    v_ext_prox  float8;
    v_ext_deg   float8;
    v_ext_ldeg  float8;
    v_cur       refcursor;
    v_mixed     jsonb;
    v_t         text;
    v_in        jsonb;
    v_ts        timestamptz := clock_timestamp();   -- start of the current stage
    v_timings   jsonb := '{}'::jsonb;               -- seconds per stage, for the report
BEGIN
    IF v_ds1 = v_ds2 THEN
        RAISE EXCEPTION 'the two datasets must be different (both are %)', v_ds1 USING ERRCODE = '22023';
    END IF;
    v_cfg := content.union5_validate_config(config);       -- rejects a bad config before any data is read
    v_ext := v_cfg->'extensions';
    RAISE NOTICE 'union 5.0: % + %', v_ds1, v_ds2;
    RAISE NOTICE 'effective config: %', v_cfg::text;

    -- Working memory for this transaction only, so hashes and sorts over millions
    -- of rows stay in memory and index builds are quick. A higher server setting
    -- is kept. JIT is off: compiling each of the union's statements costs more
    -- than it saves (the time is spent inside PostGIS functions).
    IF pg_size_bytes(current_setting('work_mem')) < pg_size_bytes('128MB') THEN
        PERFORM set_config('work_mem', '128MB', true);
    END IF;
    IF pg_size_bytes(current_setting('maintenance_work_mem')) < pg_size_bytes('512MB') THEN
        PERFORM set_config('maintenance_work_mem', '512MB', true);
    END IF;
    PERFORM set_config('jit', 'off', true);

    -- =========================================================================
    -- PHASE 1 — compile the rules (no data is read yet)
    --   Every type a rule names is normalised once into u5_types. Distances
    --   become degrees once the data's latitude is known (phase 2).
    -- =========================================================================
    RAISE NOTICE 'Phase 1: rules at %', clock_timestamp();
    DROP TABLE IF EXISTS u5_split;
    CREATE TEMP TABLE u5_split (eid bigint, frac float8, nid bigint) ON COMMIT DROP;
    DROP TABLE IF EXISTS u5_metrics;
    CREATE TEMP TABLE u5_metrics (stage text, seq int, rule text, k text, v bigint,
                                  PRIMARY KEY (stage, rule, k)) ON COMMIT DROP;
    DROP TABLE IF EXISTS u5_types;
    CREATE TEMP TABLE u5_types (tid serial PRIMARY KEY, clauses jsonb UNIQUE) ON COMMIT DROP;
    INSERT INTO u5_types (clauses)
    SELECT content.union5_builtin_type(g) FROM unnest(ARRAY['crossing', 'pedestrian', 'living_street', 'road', 'other']) g;
    v_g_tids := ARRAY(SELECT tid FROM u5_types ORDER BY tid);

    DROP TABLE IF EXISTS u5_snap_rules;
    CREATE TEMP TABLE u5_snap_rules (seq int, name text, kind text, tid int, prox float8, prox_deg float8,
                                     anchor smallint, attrs text) ON COMMIT DROP;
    IF (v_cfg->'snap'->>'coincident')::boolean THEN
        INSERT INTO u5_snap_rules VALUES (0, 'coincident', 'coincident', NULL, 0, 0,
            CASE WHEN v_cfg->'snap'->>'anchor' = 'ds2' THEN 2 ELSE 1 END, COALESCE(v_cfg->'snap'->>'attributes', 'merge'));
    END IF;
    FOR v_i IN 0 .. jsonb_array_length(v_cfg->'snap'->'rules') - 1 LOOP
        v_rule := v_cfg->'snap'->'rules'->v_i;
        v_norm := content.union5_normalize_type(v_rule->'type', 'snap');
        INSERT INTO u5_types (clauses) VALUES (v_norm) ON CONFLICT (clauses) DO NOTHING;
        SELECT tid INTO v_tid FROM u5_types WHERE clauses = v_norm;
        INSERT INTO u5_snap_rules VALUES (v_i + 1, v_rule->>'name', 'rule', v_tid, (v_rule->>'proximity')::float8, NULL,
            CASE WHEN v_rule->>'anchor' = 'ds2' THEN 2 ELSE 1 END, v_rule->>'attributes');
    END LOOP;
    IF jsonb_typeof(v_cfg->'snap'->'others') = 'object' THEN
        INSERT INTO u5_snap_rules VALUES (jsonb_array_length(v_cfg->'snap'->'rules') + 1, 'others', 'others', NULL,
            (v_cfg->'snap'->'others'->>'proximity')::float8, NULL,
            CASE WHEN v_cfg->'snap'->'others'->>'anchor' = 'ds2' THEN 2 ELSE 1 END, v_cfg->'snap'->'others'->>'attributes');
    END IF;

    DROP TABLE IF EXISTS u5_dedup_rules;
    CREATE TEMP TABLE u5_dedup_rules (seq int, name text, from_tid int, to_tid int, prox float8, buf_deg float8,
                                      min_ov float8, anchor smallint, cos_ang float8) ON COMMIT DROP;
    FOR v_i IN 0 .. jsonb_array_length(v_cfg->'dedup'->'rules') - 1 LOOP
        v_rule := v_cfg->'dedup'->'rules'->v_i;
        v_norm := content.union5_normalize_type(v_rule->'from', 'dedup');
        INSERT INTO u5_types (clauses) VALUES (v_norm) ON CONFLICT (clauses) DO NOTHING;
        SELECT tid INTO v_tid FROM u5_types WHERE clauses = v_norm;
        v_norm := content.union5_normalize_type(v_rule->'to', 'dedup');
        INSERT INTO u5_types (clauses) VALUES (v_norm) ON CONFLICT (clauses) DO NOTHING;
        SELECT tid INTO v_tid2 FROM u5_types WHERE clauses = v_norm;
        INSERT INTO u5_dedup_rules VALUES (v_i + 1, v_rule->>'name', v_tid, v_tid2, (v_rule->>'proximity')::float8,
            NULL, (v_rule->>'min_overlap')::float8, CASE WHEN v_rule->>'anchor' = 'ds2' THEN 2 ELSE 1 END,
            cos(radians(COALESCE((v_rule->>'max_angle')::float8, 30))));
    END LOOP;

    DROP TABLE IF EXISTS u5_connect_rules;
    CREATE TEMP TABLE u5_connect_rules (seq int, name text, from_tid int, to_tid int, op text, prox float8,
                                        prox_deg float8, node_tags jsonb, edge_tags jsonb) ON COMMIT DROP;
    FOR v_i IN 0 .. jsonb_array_length(v_cfg->'connect'->'rules') - 1 LOOP
        v_rule := v_cfg->'connect'->'rules'->v_i;
        v_norm := content.union5_normalize_type(v_rule->'from', 'connect');
        INSERT INTO u5_types (clauses) VALUES (v_norm) ON CONFLICT (clauses) DO NOTHING;
        SELECT tid INTO v_tid FROM u5_types WHERE clauses = v_norm;
        v_norm := content.union5_normalize_type(v_rule->'to', 'connect');
        INSERT INTO u5_types (clauses) VALUES (v_norm) ON CONFLICT (clauses) DO NOTHING;
        SELECT tid INTO v_tid2 FROM u5_types WHERE clauses = v_norm;
        INSERT INTO u5_connect_rules VALUES (v_i + 1, v_rule->>'name', v_tid, v_tid2, v_rule->>'operation',
            (v_rule->>'proximity')::float8, NULL, COALESCE(v_rule->'node_tags', '{}'::jsonb),
            COALESCE(v_rule->'edge_tags', '{}'::jsonb));
    END LOOP;

    -- every rule appears in the report, even one that does nothing
    INSERT INTO u5_metrics SELECT 'snap', seq, name, k, 0 FROM u5_snap_rules,
        unnest(ARRAY['snapped', 'edges_repointed', 'edges_split']) k;
    INSERT INTO u5_metrics SELECT 'dedup', seq, name, k, 0 FROM u5_dedup_rules,
        unnest(ARRAY['candidates', 'removed', 'restored']) k;
    INSERT INTO u5_metrics SELECT 'connect', seq, name, k, 0 FROM u5_connect_rules,
        unnest(ARRAY['candidates', 'junctions', 'nodes_created', 'nodes_reused', 'joined_earlier_node', 'connectors',
                     'edges_split', 'skipped_taken', 'skipped_nearer_end', 'skipped_angle', 'skipped_level',
                     'skipped_mutual']) k;

    -- the tag keys any type mentions: a feature's signature is its values for these
    v_keys := ARRAY(SELECT DISTINCT k FROM u5_types t, jsonb_array_elements(t.clauses) c,
                    LATERAL (SELECT jsonb_object_keys(COALESCE(c->'all', '{}'::jsonb))
                             UNION SELECT jsonb_object_keys(COALESCE(c->'none', '{}'::jsonb))) x(k));

    -- =========================================================================
    -- PHASE 2 — load nodes and edges of both datasets; match types
    --   Each edge and node gets the ids of the types it matches (tm), computed
    --   once per DISTINCT tag signature while loading — spatial steps never
    --   re-read tags. Indexes are built once the tables are complete.
    -- =========================================================================
    RAISE NOTICE 'Phase 2: load at %', clock_timestamp();
    -- Every working table is filled in one INSERT and indexed before anything
    -- updates it: an index built after a row was updated in the same transaction
    -- cannot be used until that transaction ends (PostgreSQL's broken-HOT-chain
    -- rule), and the union runs in one transaction.
    DROP TABLE IF EXISTS u5_nload;
    CREATE TEMP TABLE u5_nload ON COMMIT DROP AS
    SELECT n.id AS nid, (CASE WHEN n.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END)::smallint AS src,
           n.p->>'_id' AS orig_id, n.node_loc AS geom, n.p - '_id' AS props,
           COALESCE(n.p->>'barrier' = 'kerb', false) AS is_kerb
    FROM (SELECT id, tdei_dataset_id, node_loc, feature::jsonb->'properties' AS p   -- parse each feature once
          FROM content.node WHERE tdei_dataset_id IN (v_ds1, v_ds2) OFFSET 0) n;
    -- edge ends → nodes by EXACT coordinate in the same dataset (no tolerance in node
    -- identity): a hash join on (dataset, x, y)
    DROP TABLE IF EXISTS u5_nkey;
    CREATE TEMP TABLE u5_nkey ON COMMIT DROP AS
    SELECT src, ST_X(geom) AS x, ST_Y(geom) AS y, min(nid) AS nid FROM u5_nload GROUP BY 1, 2, 3;
    DROP TABLE IF EXISTS u5_eload;
    CREATE TEMP TABLE u5_eload ON COMMIT DROP AS
    SELECT e.id, e.src, e.orig_id, e.geom, e.props, e.levelkey, e.sx, e.sy, e.ex, e.ey,
           (SELECT COALESCE(jsonb_object_agg(k, e.props->k), '{}'::jsonb) FROM unnest(v_keys) k WHERE e.props ? k) AS sig
    FROM (SELECT e.id,
                 (CASE WHEN e.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END)::smallint AS src,
                 e.p->>'_id' AS orig_id,
                 e.edge_loc AS geom,
                 e.p - '_id' - '_u_id' - '_v_id' AS props,
                 -- vertical position: lines on different layers / levels / bridge / tunnel never join (G1)
                 concat_ws('|', COALESCE(e.p->>'layer', '0'), COALESCE(e.p->>'level', ''),
                     CASE WHEN COALESCE(e.p->>'bridge', 'no') IN ('no', 'false', '0') THEN '' ELSE 'bridge' END,
                     CASE WHEN COALESCE(e.p->>'tunnel', 'no') IN ('no', 'false', '0') THEN '' ELSE 'tunnel' END) AS levelkey,
                 ST_X(ST_StartPoint(e.edge_loc)) AS sx, ST_Y(ST_StartPoint(e.edge_loc)) AS sy,
                 ST_X(ST_EndPoint(e.edge_loc)) AS ex, ST_Y(ST_EndPoint(e.edge_loc)) AS ey
          FROM (SELECT id, tdei_dataset_id, edge_loc, feature::jsonb->'properties' AS p    -- parse each feature once
                FROM content.edge WHERE tdei_dataset_id IN (v_ds1, v_ds2) AND ST_NPoints(edge_loc) >= 2 OFFSET 0) e
          OFFSET 0) e;
    ANALYZE u5_nkey;
    ANALYZE u5_eload;

    -- an edge end with no node at its coordinate gets one, so the output stays valid
    SELECT COALESCE(max(nid), 0) + 1 INTO v_next_nid FROM u5_nload;
    WITH ends AS (
        SELECT e.src, e.sx AS x, e.sy AS y FROM u5_eload e
        UNION SELECT e.src, e.ex, e.ey FROM u5_eload e),
    missing AS (
        SELECT d.src, d.x, d.y, v_next_nid + ROW_NUMBER() OVER (ORDER BY d.src, d.x, d.y) AS nid
        FROM ends d LEFT JOIN u5_nkey k ON k.src = d.src AND k.x = d.x AND k.y = d.y
        WHERE k.nid IS NULL),
    ins AS (
        INSERT INTO u5_nload (nid, src, orig_id, geom, props, is_kerb)
        SELECT nid, src, NULL, ST_SetSRID(ST_MakePoint(x, y), 4326), '{}'::jsonb, false FROM missing
        RETURNING nid, src, geom)
    INSERT INTO u5_nkey SELECT src, ST_X(geom), ST_Y(geom), nid FROM ins;
    GET DIAGNOSTICS v_n = ROW_COUNT;
    IF v_n > 0 THEN
        v_warnings := v_warnings || format('%s edge end(s) had no node at their coordinate; a node was added for each', v_n);
        ANALYZE u5_nkey;
    END IF;

    -- edges, with their end nodes and the types of their signature
    DROP TABLE IF EXISTS u5_sigtm;
    CREATE TEMP TABLE u5_sigtm ON COMMIT DROP AS
    SELECT 'e'::text AS kind, s.sig,
           ARRAY(SELECT t.tid FROM u5_types t WHERE content.union5_tag_match(s.sig, t.clauses, false) ORDER BY t.tid) AS tm
    FROM (SELECT DISTINCT sig FROM u5_eload) s;
    DROP TABLE IF EXISTS u5_e;
    CREATE TEMP TABLE u5_e (
        eid          bigint PRIMARY KEY,
        src          smallint NOT NULL,       -- 1 = DS1, 2 = DS2, 0 = connector
        orig_id      text,
        geom         geometry(LineString, 4326) NOT NULL,
        props        jsonb NOT NULL DEFAULT '{}'::jsonb,
        u            bigint,
        v            bigint,
        alive        boolean NOT NULL DEFAULT true,
        tm           int[] NOT NULL DEFAULT '{}',
        is_connector boolean NOT NULL DEFAULT false,
        rule         text,                    -- the connect rule that made a connector
        removed_by   text,                    -- the dedup rule that removed this edge
        removed_anchor smallint,
        absorbed_by  bigint,                  -- the anchor edge it duplicated (root id)
        parent       bigint,                  -- the original edge of a split piece
        levelkey     text NOT NULL DEFAULT ''
    ) WITH (fillfactor = 70) ON COMMIT DROP;
    INSERT INTO u5_e (eid, src, orig_id, geom, props, levelkey, u, v, tm)
    SELECT e.id, e.src, e.orig_id, e.geom, e.props, e.levelkey, ks.nid, ke.nid, COALESCE(st.tm, '{}')
    FROM u5_eload e
    LEFT JOIN u5_nkey ks ON ks.src = e.src AND ks.x = e.sx AND ks.y = e.sy
    LEFT JOIN u5_nkey ke ON ke.src = e.src AND ke.x = e.ex AND ke.y = e.ey
    LEFT JOIN u5_sigtm st ON st.kind = 'e' AND st.sig = e.sig;
    DROP TABLE u5_eload;
    CREATE INDEX ON u5_e (u);
    CREATE INDEX ON u5_e (v);
    CREATE INDEX ON u5_e (parent);

    -- nodes: a node matches on its own identifying tags, and also takes the types of
    -- every edge it lies on: edges ending at it (by id) and edges passing through it
    -- (an interior vertex at its exact coordinate, in its own dataset)
    DROP TABLE IF EXISTS u5_nsig;
    CREATE TEMP TABLE u5_nsig ON COMMIT DROP AS
    SELECT nid, (SELECT COALESCE(jsonb_object_agg(k, props->k), '{}'::jsonb) FROM unnest(v_keys) k WHERE props ? k) AS sig
    FROM u5_nload;
    INSERT INTO u5_sigtm
    SELECT 'n', s.sig,
           ARRAY(SELECT t.tid FROM u5_types t WHERE content.union5_tag_match(s.sig, t.clauses, true) ORDER BY t.tid)
    FROM (SELECT DISTINCT sig FROM u5_nsig) s;
    DROP TABLE IF EXISTS u5_ivx;
    CREATE TEMP TABLE u5_ivx ON COMMIT DROP AS
    SELECT e.eid, e.src, ST_X(dp.geom) AS x, ST_Y(dp.geom) AS y
    FROM u5_e e, LATERAL ST_DumpPoints(e.geom) dp
    WHERE ST_NPoints(e.geom) > 2 AND dp.path[1] > 1 AND dp.path[1] < ST_NPoints(e.geom);
    ANALYZE u5_ivx;
    DROP TABLE IF EXISTS u5_vtx;
    CREATE TEMP TABLE u5_vtx ON COMMIT DROP AS
    SELECT DISTINCT d.eid, k.nid FROM u5_ivx d JOIN u5_nkey k ON k.src = d.src AND k.x = d.x AND k.y = d.y;
    DROP TABLE u5_ivx;
    CREATE INDEX ON u5_vtx (nid);
    CREATE INDEX ON u5_vtx (eid);
    ANALYZE u5_vtx;
    DROP TABLE IF EXISTS u5_ntm;
    CREATE TEMP TABLE u5_ntm ON COMMIT DROP AS
    SELECT nid, array_agg(DISTINCT t ORDER BY t) AS tm FROM (
        SELECT s.nid, unnest(st.tm) AS t
        FROM u5_nsig s JOIN u5_sigtm st ON st.kind = 'n' AND st.sig = s.sig
        WHERE st.tm <> '{}'
        UNION ALL
        SELECT e.u, unnest(e.tm) FROM u5_e e WHERE e.tm <> '{}'
        UNION ALL
        SELECT e.v, unnest(e.tm) FROM u5_e e WHERE e.tm <> '{}'
        UNION ALL
        SELECT x.nid, unnest(e.tm) FROM u5_vtx x JOIN u5_e e ON e.eid = x.eid WHERE e.tm <> '{}') x
    GROUP BY nid;
    DROP TABLE u5_nsig;
    ANALYZE u5_ntm;
    DROP TABLE IF EXISTS u5_n;
    CREATE TEMP TABLE u5_n (
        nid          bigint PRIMARY KEY,      -- output _id (the database id for input nodes)
        src          smallint NOT NULL,       -- 1 = DS1, 2 = DS2, 0 = created by a connect rule
        orig_id      text,                    -- original GeoJSON _id
        geom         geometry(Point, 4326) NOT NULL,
        props        jsonb NOT NULL DEFAULT '{}'::jsonb,
        is_kerb      boolean NOT NULL DEFAULT false,
        alive        boolean NOT NULL DEFAULT true,
        merged_into  bigint,                  -- set when this node joined another
        tm           int[] NOT NULL DEFAULT '{}',   -- ids of the rule types the node matches
        audit        jsonb NOT NULL DEFAULT '{}'::jsonb,
        absorbed     text,
        union_rules  text,
        confidence   float8
    ) WITH (fillfactor = 70) ON COMMIT DROP;
    INSERT INTO u5_n (nid, src, orig_id, geom, props, is_kerb, tm)
    SELECT n.nid, n.src, n.orig_id, n.geom, n.props, n.is_kerb, COALESCE(x.tm, '{}')
    FROM u5_nload n LEFT JOIN u5_ntm x ON x.nid = n.nid;
    DROP TABLE u5_nload;
    DROP TABLE u5_ntm;
    CREATE INDEX ON u5_n USING GIST (geom);
    CREATE INDEX ON u5_n (src, orig_id);

    -- nodes an input edge uses (zone rings are added in phase 7); the rest are input orphans
    DROP TABLE IF EXISTS u5_inref;
    CREATE TEMP TABLE u5_inref ON COMMIT DROP AS SELECT u AS nid FROM u5_e UNION SELECT v FROM u5_e;
    CREATE UNIQUE INDEX ON u5_inref (nid);

    ANALYZE u5_n;                       -- temp tables are never auto-analysed; plans need the stats
    ANALYZE u5_e;
    SELECT COALESCE(max(eid), 0) + 1 INTO v_next_eid FROM u5_e;
    SELECT COALESCE(max(nid), 0) + 1 INTO v_next_nid FROM u5_n;
    v_mlon := v_mlat * COS(RADIANS(COALESCE((SELECT AVG(ST_Y(geom)) FROM u5_n), 0)));
    v_in := jsonb_build_object('nodes', (SELECT count(*) FROM u5_n WHERE orig_id IS NOT NULL),
                               'edges', (SELECT count(*) FROM u5_e));
    -- rule distances in degrees of longitude at the data's latitude (the larger
    -- span: an index search box that is never too small)
    UPDATE u5_snap_rules SET prox_deg = prox / v_mlon WHERE kind <> 'coincident';
    UPDATE u5_dedup_rules SET buf_deg = prox / v_mlon;
    UPDATE u5_connect_rules SET prox_deg = prox / v_mlon;

    RAISE NOTICE 'Phase 2 complete: ds1 nodes=%, ds2 nodes=%, ds1 edges=%, ds2 edges=%; % types, % tag signatures',
        (SELECT count(*) FROM u5_n WHERE src = 1), (SELECT count(*) FROM u5_n WHERE src = 2),
        (SELECT count(*) FROM u5_e WHERE src = 1), (SELECT count(*) FROM u5_e WHERE src = 2),
        (SELECT count(*) FROM u5_types), (SELECT count(*) FROM u5_sigtm);
    v_timings := v_timings || jsonb_build_object('load', round(extract(epoch FROM clock_timestamp() - v_ts)::numeric, 2));
    v_ts := clock_timestamp();

    -- =========================================================================
    -- PHASE 3 — SNAP: a DS1 node and a DS2 node of the same type within the
    -- governing rule's proximity join; the non-anchor node moves onto the anchor.
    --   coincident first (any types), then the rules in order, then 'others'.
    --   A pair is governed by the FIRST rule whose type both nodes match.
    --   Nearest wins (tie: lowest original _id). Anchors never move, a node that
    --   moved is never a target, a kerb never moves (except coincident).
    -- =========================================================================
    DROP TABLE IF EXISTS u5_snap;
    CREATE TEMP TABLE u5_snap (mover bigint PRIMARY KEY, target bigint, rule text, seq int, dist float8,
                               attrs text, prox float8) ON COMMIT DROP;
    CREATE INDEX ON u5_snap (target);
    IF EXISTS (SELECT 1 FROM u5_snap_rules) THEN
        RAISE NOTICE 'Phase 3: snap at %', clock_timestamp();
        FOR r IN SELECT * FROM u5_snap_rules ORDER BY seq LOOP
            INSERT INTO u5_snap (mover, target, rule, seq, dist, attrs, prox)
            SELECT DISTINCT ON (m.nid) m.nid, a.nid, r.name, r.seq,
                   content.union5_dist_m(m.geom, a.geom, v_mlon), r.attrs, r.prox
            FROM u5_n m
            JOIN u5_n a ON a.src = r.anchor AND a.alive
                       AND ST_DWithin(a.geom, m.geom, GREATEST(r.prox_deg, 1e-12))
            WHERE m.src = 3 - r.anchor AND m.alive
              AND content.union5_dist_m(m.geom, a.geom, v_mlon) <= r.prox
              AND (r.kind <> 'coincident' OR ST_Equals(m.geom, a.geom))
              AND (r.kind = 'coincident' OR NOT m.is_kerb)
              AND (r.kind <> 'rule' OR (
                        r.tid = ANY(m.tm) AND r.tid = ANY(a.tm)
                    AND NOT EXISTS (SELECT 1 FROM u5_snap_rules q
                                    WHERE q.kind = 'rule' AND q.seq < r.seq AND q.tid = ANY(m.tm) AND q.tid = ANY(a.tm))))
              AND (r.kind <> 'others' OR (
                        NOT EXISTS (SELECT 1 FROM u5_snap_rules q WHERE q.kind = 'rule' AND q.tid = ANY(m.tm) AND q.tid = ANY(a.tm))
                    AND EXISTS (SELECT 1 FROM unnest(m.tm) t WHERE t = ANY(a.tm) AND t = ANY(v_g_tids))))
              AND NOT EXISTS (SELECT 1 FROM u5_snap s WHERE s.mover = m.nid)
              AND NOT EXISTS (SELECT 1 FROM u5_snap s WHERE s.target = m.nid)
              AND NOT EXISTS (SELECT 1 FROM u5_snap s WHERE s.mover = a.nid)
            ORDER BY m.nid, content.union5_dist_m(m.geom, a.geom, v_mlon), a.orig_id, a.nid;
        END LOOP;

        -- never collapse an edge: when both ends of an edge would land on one
        -- node, the farther end stays where it is
        LOOP
            DELETE FROM u5_snap s
            USING (SELECT CASE WHEN su.dist >= sv.dist THEN su.mover ELSE sv.mover END AS drop_mover
                   FROM u5_e e
                   JOIN u5_snap su ON su.mover = e.u
                   JOIN u5_snap sv ON sv.mover = e.v
                   WHERE e.alive AND e.u <> e.v AND su.target = sv.target) b
            WHERE s.mover = b.drop_mover;
            GET DIAGNOSTICS v_n = ROW_COUNT;
            EXIT WHEN v_n = 0;
        END LOOP;

        -- Attributes onto the anchor node. Every mover value that differs from the
        -- anchor's own (including keys the anchor lacks) is audited as
        -- ext:union_audit_<key> = "<original _id>-<value>". merge: anchor values
        -- win and keys only the mover has are added; anchor_only: nothing is added.
        DROP TABLE IF EXISTS u5_snap_kv;
        CREATE TEMP TABLE u5_snap_kv ON COMMIT DROP AS
        SELECT s.target, s.attrs, m.orig_id, kv.key, kv.value
        FROM u5_snap s JOIN u5_n m ON m.nid = s.mover, jsonb_each(m.props) kv;
        UPDATE u5_n t SET audit = content.union5_audit_add(t.audit, x.aud)
        FROM (SELECT target, jsonb_object_agg('ext:union_audit_' || regexp_replace(key, '^ext:', ''), to_jsonb(s) ORDER BY key) AS aud
              FROM (SELECT k.target, k.key,
                           string_agg(COALESCE(k.orig_id, '?') || '-' || (k.value #>> '{}'), ',' ORDER BY k.orig_id) AS s
                    FROM u5_snap_kv k JOIN u5_n t2 ON t2.nid = k.target
                    WHERE (t2.props->k.key) IS DISTINCT FROM k.value
                    GROUP BY k.target, k.key) y
              GROUP BY target) x
        WHERE t.nid = x.target;
        UPDATE u5_n t SET props = t.props || x.add
        FROM (SELECT target, jsonb_object_agg(key, value) AS add FROM (
                  SELECT DISTINCT ON (k.target, k.key) k.target, k.key, k.value
                  FROM u5_snap_kv k JOIN u5_n t2 ON t2.nid = k.target
                  WHERE k.attrs = 'merge' AND NOT (t2.props ? k.key)
                  ORDER BY k.target, k.key, k.orig_id) y
              GROUP BY target) x
        WHERE t.nid = x.target;
        UPDATE u5_n t SET
            absorbed = x.ids,
            confidence = x.conf
        FROM (SELECT s.target,
                     string_agg(COALESCE(m.orig_id, m.nid::text), ',' ORDER BY m.orig_id) AS ids,
                     max(CASE WHEN s.dist = 0 OR s.prox = 0 THEN 1.0 ELSE GREATEST(0.0, 1.0 - s.dist / s.prox) END) AS conf
              FROM u5_snap s JOIN u5_n m ON m.nid = s.mover GROUP BY s.target) x
        WHERE t.nid = x.target;

        -- Movers retire. Their edges follow: an edge ending at a mover is
        -- re-pointed (its end set onto the anchor node) …
        UPDATE u5_n m SET alive = false, merged_into = s.target FROM u5_snap s WHERE s.mover = m.nid;
        UPDATE u5_metrics mt SET v = x.n
        FROM (SELECT s.rule, count(*) AS n
              FROM u5_snap s JOIN (SELECT u AS nid FROM u5_e WHERE alive UNION ALL SELECT v FROM u5_e WHERE alive) ends
                   ON ends.nid = s.mover
              GROUP BY s.rule) x
        WHERE mt.stage = 'snap' AND mt.rule = x.rule AND mt.k = 'edges_repointed';
        UPDATE u5_e e SET u = s.target, geom = ST_SetPoint(e.geom, 0, t.geom)
        FROM u5_snap s JOIN u5_n t ON t.nid = s.target WHERE e.u = s.mover;
        UPDATE u5_e e SET v = s.target, geom = ST_SetPoint(e.geom, ST_NPoints(e.geom) - 1, t.geom)
        FROM u5_snap s JOIN u5_n t ON t.nid = s.target WHERE e.v = s.mover;
        -- … and an edge passing through a mover as an interior vertex is split there
        INSERT INTO u5_split (eid, frac, nid)
        SELECT e.eid, ST_LineLocatePoint(e.geom, m.geom), s.target
        FROM u5_snap s
        JOIN u5_vtx x ON x.nid = s.mover
        JOIN u5_e e ON e.eid = x.eid AND e.alive
        JOIN u5_n m ON m.nid = s.mover
        WHERE e.u <> s.target AND e.v <> s.target;
        UPDATE u5_metrics mt SET v = x.n
        FROM (SELECT s.rule, count(DISTINCT sp.eid) AS n FROM u5_split sp JOIN u5_snap s ON s.target = sp.nid GROUP BY s.rule) x
        WHERE mt.stage = 'snap' AND mt.rule = x.rule AND mt.k = 'edges_split';
        v_next_eid := content.union5_apply_splits(v_next_eid);

        UPDATE u5_metrics mt SET v = x.n FROM (SELECT rule, count(*) AS n FROM u5_snap GROUP BY rule) x
        WHERE mt.stage = 'snap' AND mt.rule = x.rule AND mt.k = 'snapped';
        RAISE NOTICE 'Phase 3 complete: % nodes joined onto % anchor nodes',
            (SELECT count(*) FROM u5_snap), (SELECT count(DISTINCT target) FROM u5_snap);
    END IF;
    v_timings := v_timings || jsonb_build_object('snap', round(extract(epoch FROM clock_timestamp() - v_ts)::numeric, 2));
    v_ts := clock_timestamp();

    -- =========================================================================
    -- PHASE 4 — DEDUP: remove an edge that duplicates an edge of the anchor
    -- dataset (whole edges only; first rule whose 'from' the edge matches).
    --   duplicate = at least min_overlap of its length inside the buffer AND
    --     either it joins the same two nodes as a 'to' anchor edge, or it runs
    --     within the rule's max_angle (30° by default) of the anchor edge (angle guard) and — when its two ends are
    --     both already on the anchor network — the anchor edges inside its buffer
    --     already join those two ends (connectivity guard).
    -- =========================================================================
    DROP TABLE IF EXISTS u5_dedup_seen;
    CREATE TEMP TABLE u5_dedup_seen (eid bigint PRIMARY KEY) ON COMMIT DROP;
    FOR r IN SELECT * FROM u5_dedup_rules ORDER BY seq LOOP
        IF r.seq = 1 THEN RAISE NOTICE 'Phase 4: dedup at %', clock_timestamp(); END IF;
        DROP TABLE IF EXISTS u5_dc;
        CREATE TEMP TABLE u5_dc ON COMMIT DROP AS
        SELECT e.eid, e.geom, e.u, e.v FROM u5_e e
        WHERE e.alive AND NOT e.is_connector AND e.src = 3 - r.anchor AND r.from_tid = ANY(e.tm)
          AND NOT EXISTS (SELECT 1 FROM u5_dedup_seen s WHERE s.eid = e.eid);
        INSERT INTO u5_dedup_seen SELECT eid FROM u5_dc;
        UPDATE u5_metrics SET v = (SELECT count(*) FROM u5_dc) WHERE stage = 'dedup' AND rule = r.name AND k = 'candidates';
        CONTINUE WHEN NOT EXISTS (SELECT 1 FROM u5_dc);
        CREATE INDEX ON u5_dc USING GIST (geom);
        ANALYZE u5_dc;

        -- the anchor edges of the 'to' type within reach of a candidate (only those
        -- can cover one, join the same nodes, or be walked by the connectivity guard),
        -- with their buffers
        DROP TABLE IF EXISTS u5_db;
        CREATE TEMP TABLE u5_db ON COMMIT DROP AS
        SELECT e.eid, COALESCE(e.parent, e.eid) AS root, e.geom, e.u, e.v,
               LEAST(e.u, e.v) AS lo, GREATEST(e.u, e.v) AS hi, ST_Buffer(e.geom, r.buf_deg) AS buf
        FROM u5_e e
        WHERE e.alive AND NOT e.is_connector AND e.src = r.anchor AND r.to_tid = ANY(e.tm)
          AND EXISTS (SELECT 1 FROM u5_dc c WHERE ST_DWithin(c.geom, e.geom, r.buf_deg));
        CREATE INDEX ON u5_db USING GIST (buf);
        CREATE INDEX ON u5_db USING GIST (geom);
        CREATE INDEX ON u5_db (lo, hi);
        ANALYZE u5_db;

        DROP TABLE IF EXISTS u5_dd;
        CREATE TEMP TABLE u5_dd ON COMMIT DROP AS
        SELECT x.eid, x.geom, x.u, x.v, x.same_pair, x.near_root, x.along, x.sec_u, x.sec_v,
               -- the share of the edge inside the anchor buffers. It decides only an edge that
               -- could be removed (same two nodes, or along an anchor edge), and only against
               -- min_overlap: the nearest anchor edge's buffer alone usually settles it (a copy
               -- of one edge); otherwise the union of every buffer the edge touches.
               CASE WHEN NOT (x.same_pair OR x.along) THEN NULL
                    ELSE (SELECT CASE WHEN q.c1 >= r.min_ov THEN q.c1
                                      ELSE COALESCE(ST_Length(ST_CollectionExtract(ST_Intersection(x.geom,
                                               (SELECT ST_Union(b.buf) FROM u5_db b WHERE ST_DWithin(x.geom, b.buf, 0))), 2))
                                             / NULLIF(ST_Length(x.geom), 0), 0.0) END
                          FROM (SELECT COALESCE(ST_Length(ST_CollectionExtract(ST_Intersection(x.geom, x.near_buf), 2))
                                                / NULLIF(ST_Length(x.geom), 0), 0.0) AS c1) q)
               END AS covered
        FROM (
            SELECT c.eid, c.geom, c.u, c.v,
                   EXISTS (SELECT 1 FROM u5_db b WHERE b.lo = LEAST(c.u, c.v) AND b.hi = GREATEST(c.u, c.v)) AS same_pair,
                   nb.root AS near_root, nb.buf AS near_buf,
                   COALESCE(nb.along, true) AS along,
                   (EXISTS (SELECT 1 FROM u5_e a WHERE a.u = c.u AND a.alive AND NOT a.is_connector AND a.src = r.anchor)
                    OR EXISTS (SELECT 1 FROM u5_e a WHERE a.v = c.u AND a.alive AND NOT a.is_connector AND a.src = r.anchor)) AS sec_u,
                   (EXISTS (SELECT 1 FROM u5_e a WHERE a.u = c.v AND a.alive AND NOT a.is_connector AND a.src = r.anchor)
                    OR EXISTS (SELECT 1 FROM u5_e a WHERE a.v = c.v AND a.alive AND NOT a.is_connector AND a.src = r.anchor)) AS sec_v
            FROM u5_dc c
            LEFT JOIN LATERAL (
                SELECT b.root, b.buf,
                       CASE WHEN content.union5_dist_m(ST_StartPoint(c.geom), ST_EndPoint(c.geom), v_mlon) < 0.1 THEN true
                            ELSE abs(  ((ST_X(q.pb) - ST_X(q.pa)) * v_mlon) * ((ST_X(ST_EndPoint(c.geom)) - ST_X(ST_StartPoint(c.geom))) * v_mlon)
                                     + ((ST_Y(q.pb) - ST_Y(q.pa)) * v_mlat) * ((ST_Y(ST_EndPoint(c.geom)) - ST_Y(ST_StartPoint(c.geom))) * v_mlat))
                                 >= r.cos_ang * content.union5_dist_m(q.pa, q.pb, v_mlon)
                                             * content.union5_dist_m(ST_StartPoint(c.geom), ST_EndPoint(c.geom), v_mlon)
                       END AS along
                FROM (SELECT b.root, b.geom, b.buf, ST_LineLocatePoint(b.geom, ST_LineInterpolatePoint(c.geom, 0.5)) AS f
                      FROM u5_db b
                      WHERE ST_DWithin(b.geom, c.geom, r.buf_deg)
                      ORDER BY b.geom <-> ST_LineInterpolatePoint(c.geom, 0.5), b.eid
                      LIMIT 1) b,
                     LATERAL (SELECT ST_LineInterpolatePoint(b.geom, GREATEST(b.f - 1e-4, 0.0)) AS pa,
                                     ST_LineInterpolatePoint(b.geom, LEAST(b.f + 1e-4, 1.0)) AS pb) q
            ) nb ON true
            OFFSET 0) x;

        -- connectivity guard: an edge whose two ends are both on the anchor network is a
        -- duplicate only when the anchor network already joins those two ends through
        -- anchor edges lying inside its buffer (the same line, drawn as more pieces).
        -- The walk runs only for the edges it can decide (cheap tests first).
        ALTER TABLE u5_dd ADD COLUMN joined boolean NOT NULL DEFAULT false;
        UPDATE u5_dd d SET joined = EXISTS (
              WITH RECURSIVE corr AS (
                  SELECT b.u, b.v FROM u5_db b
                  WHERE ST_DWithin(b.geom, d.geom, r.buf_deg)
                    AND ST_Length(ST_CollectionExtract(ST_Intersection(b.geom, ST_Buffer(d.geom, r.buf_deg)), 2))
                        >= r.min_ov * ST_Length(b.geom)),
              walk(nid) AS (
                  SELECT d.u
                  UNION
                  SELECT CASE WHEN k.u = w.nid THEN k.v ELSE k.u END
                  FROM walk w JOIN corr k ON k.u = w.nid OR k.v = w.nid)
              SELECT 1 FROM walk WHERE walk.nid = d.v)
        WHERE d.sec_u AND d.sec_v AND NOT d.same_pair AND d.along AND d.covered >= r.min_ov;
        UPDATE u5_e e SET alive = false, removed_by = r.name, removed_anchor = r.anchor, absorbed_by = d.near_root
        FROM u5_dd d
        WHERE d.eid = e.eid AND d.covered >= r.min_ov
          AND (d.same_pair OR (d.along AND (NOT (d.sec_u AND d.sec_v) OR d.joined)));
        GET DIAGNOSTICS v_n = ROW_COUNT;
        UPDATE u5_metrics SET v = v_n WHERE stage = 'dedup' AND rule = r.name AND k = 'removed';
    END LOOP;
    v_timings := v_timings || jsonb_build_object('dedup', round(extract(epoch FROM clock_timestamp() - v_ts)::numeric, 2));
    v_ts := clock_timestamp();

    -- Snap topology: an anchor edge passing through a node that a moved edge now
    -- ENDS at is split there, so the join is a real connection. Done after
    -- dedup, so a duplicate that was removed never splits the anchor.
    DROP TABLE IF EXISTS u5_tgt;
    CREATE TEMP TABLE u5_tgt ON COMMIT DROP AS
    SELECT DISTINCT t.nid, t.src, t.geom
    FROM (SELECT DISTINCT target FROM u5_snap) s
    JOIN u5_n t ON t.nid = s.target AND t.alive
    JOIN u5_vtx x ON x.nid = t.nid
    WHERE EXISTS (SELECT 1 FROM u5_e f WHERE f.u = t.nid AND f.alive AND f.src <> t.src)
       OR EXISTS (SELECT 1 FROM u5_e f WHERE f.v = t.nid AND f.alive AND f.src <> t.src);
    -- the edge itself, or a piece of it after a split (two index joins, not one OR join)
    INSERT INTO u5_split (eid, frac, nid)
    SELECT e.eid, ST_LineLocatePoint(e.geom, t.geom), t.nid
    FROM u5_tgt t
    JOIN u5_vtx x ON x.nid = t.nid
    JOIN u5_e e ON e.eid = x.eid AND e.alive AND NOT e.is_connector
    WHERE e.u <> t.nid AND e.v <> t.nid AND ST_DWithin(e.geom, t.geom, v_tol)
    UNION ALL
    SELECT e.eid, ST_LineLocatePoint(e.geom, t.geom), t.nid
    FROM u5_tgt t
    JOIN u5_vtx x ON x.nid = t.nid
    JOIN u5_e e ON e.parent = x.eid AND e.alive AND NOT e.is_connector
    WHERE e.u <> t.nid AND e.v <> t.nid AND ST_DWithin(e.geom, t.geom, v_tol);
    UPDATE u5_metrics mt SET v = mt.v + x.n
    FROM (SELECT y.rule, count(DISTINCT y.eid) AS n FROM (
              SELECT DISTINCT ON (sp.eid, sp.nid) sp.eid, s.rule FROM u5_split sp JOIN u5_snap s ON s.target = sp.nid
              ORDER BY sp.eid, sp.nid, s.seq) y GROUP BY y.rule) x
    WHERE mt.stage = 'snap' AND mt.rule = x.rule AND mt.k = 'edges_split';
    v_next_eid := content.union5_apply_splits(v_next_eid);
    ANALYZE u5_e;
    v_timings := v_timings || jsonb_build_object('snap_topology', round(extract(epoch FROM clock_timestamp() - v_ts)::numeric, 2));
    v_ts := clock_timestamp();

    -- =========================================================================
    -- PHASE 5 — CONNECT: junctions between two kinds of way, rule by rule.
    --   A = edges matching 'from', B = edges matching 'to'; always one from
    --   DS1 and one from DS2. Nothing moves: a node is made on B (B is split
    --   there) and, across a gap, a connector edge joins A's end to it.
    --   O1 an A end an earlier rule used is taken; O2 a junction landing within
    --   reach of an earlier rule's node on B uses that node; O3 created nodes
    --   and connectors are never A or B.
    -- =========================================================================
    DROP TABLE IF EXISTS u5_taken;
    CREATE TEMP TABLE u5_taken (nid bigint PRIMARY KEY) ON COMMIT DROP;
    DROP TABLE IF EXISTS u5_jn;
    CREATE TEMP TABLE u5_jn (nid bigint PRIMARY KEY) ON COMMIT DROP;
    FOR r IN SELECT * FROM u5_connect_rules ORDER BY seq LOOP
        IF r.seq = 1 THEN RAISE NOTICE 'Phase 5: connect at %', clock_timestamp(); END IF;

        DROP TABLE IF EXISTS u5_cb;
        CREATE TEMP TABLE u5_cb ON COMMIT DROP AS
        SELECT eid, src, orig_id, geom, u, v, levelkey FROM u5_e
        WHERE alive AND NOT is_connector AND src IN (1, 2) AND r.to_tid = ANY(tm);
        CREATE INDEX ON u5_cb USING GIST (geom) WHERE src = 1;        -- one index per dataset: a probe
        CREATE INDEX ON u5_cb USING GIST (geom) WHERE src = 2;        -- only sees the other dataset's lines
        CREATE INDEX ON u5_cb (u);
        CREATE INDEX ON u5_cb (v);
        DROP TABLE IF EXISTS u5_cae;
        CREATE TEMP TABLE u5_cae ON COMMIT DROP AS
        SELECT eid, src, orig_id, geom, u, v, levelkey, props FROM u5_e
        WHERE alive AND NOT is_connector AND src IN (1, 2) AND r.from_tid = ANY(tm);
        CREATE INDEX ON u5_cae USING GIST (geom);
        CREATE INDEX ON u5_cae (u);
        CREATE INDEX ON u5_cae (v);
        ANALYZE u5_cb;
        ANALYZE u5_cae;

        -- ── an END of A within reach of B ──
        DROP TABLE IF EXISTS u5_cand;
        CREATE TEMP TABLE u5_cand ON COMMIT DROP AS
        SELECT a.eid AS a_eid, a.src AS a_src, a.orig_id AS a_orig, a.levelkey, x.e_nid, x.e_pt, x.prev, x.at_start,
               b.eid AS b_eid, b.src AS b_src, b.orig_id AS b_orig, b.u AS bu, b.v AS bv, b.levelkey AS b_level,
               b.geom AS b_geom, ST_ClosestPoint(b.geom, x.e_pt) AS q, NULL::float8 AS d,
               NULL::geometry AS xp, false AS split_a
        FROM u5_cae a
        CROSS JOIN LATERAL (VALUES (a.u, ST_StartPoint(a.geom), ST_PointN(a.geom, 2), true),
                                   (a.v, ST_EndPoint(a.geom), ST_PointN(a.geom, ST_NPoints(a.geom) - 1), false))
                   x(e_nid, e_pt, prev, at_start)
        JOIN u5_cb b ON b.src = 2 AND ST_DWithin(b.geom, x.e_pt, r.prox_deg)
        WHERE a.src = 1 AND b.u <> x.e_nid AND b.v <> x.e_nid
          AND EXISTS (SELECT 1 FROM u5_cb WHERE src = 2)
        UNION ALL
        SELECT a.eid, a.src, a.orig_id, a.levelkey, x.e_nid, x.e_pt, x.prev, x.at_start,
               b.eid, b.src, b.orig_id, b.u, b.v, b.levelkey,
               b.geom, ST_ClosestPoint(b.geom, x.e_pt), NULL::float8, NULL::geometry, false
        FROM u5_cae a
        CROSS JOIN LATERAL (VALUES (a.u, ST_StartPoint(a.geom), ST_PointN(a.geom, 2), true),
                                   (a.v, ST_EndPoint(a.geom), ST_PointN(a.geom, ST_NPoints(a.geom) - 1), false))
                   x(e_nid, e_pt, prev, at_start)
        JOIN u5_cb b ON b.src = 1 AND ST_DWithin(b.geom, x.e_pt, r.prox_deg)
        WHERE a.src = 2 AND b.u <> x.e_nid AND b.v <> x.e_nid
          AND EXISTS (SELECT 1 FROM u5_cb WHERE src = 1);
        DELETE FROM u5_cand c USING u5_n en WHERE en.nid = c.e_nid AND en.src = 0;   -- O3: a created node is never an A end
        -- an end already joined to a B line of the other dataset is a junction already
        DELETE FROM u5_cand c
        WHERE EXISTS (SELECT 1 FROM u5_cb b2 WHERE b2.u = c.e_nid AND b2.src <> c.a_src)
           OR EXISTS (SELECT 1 FROM u5_cb b2 WHERE b2.v = c.e_nid AND b2.src <> c.a_src);
        UPDATE u5_cand SET d = content.union5_dist_m(e_pt, q, v_mlon);
        DELETE FROM u5_cand WHERE d > r.prox;
        -- A crossing B near this end (an overshoot): the crossing nearest the end
        UPDATE u5_cand c SET xp = (
            SELECT dp.geom FROM ST_Dump(ST_Intersection(a.geom, c.b_geom)) dp
            WHERE GeometryType(dp.geom) = 'POINT' AND NOT ST_Equals(dp.geom, c.e_pt)
            ORDER BY abs(ST_LineLocatePoint(a.geom, dp.geom) - CASE WHEN c.at_start THEN 0.0 ELSE 1.0 END),
                     ST_X(dp.geom), ST_Y(dp.geom) LIMIT 1)
        FROM u5_cae a
        WHERE a.eid = c.a_eid AND ST_Intersects(a.geom, c.b_geom);
        IF r.op = 'point' THEN
            DELETE FROM u5_cand WHERE xp IS NOT NULL;           -- the crossing itself makes the junction (below)
        ELSE
            -- geometry: an end that overshoots B meets it where it crosses (A and B split there)
            UPDATE u5_cand SET q = xp, d = content.union5_dist_m(e_pt, xp, v_mlon), split_a = true WHERE xp IS NOT NULL;
        END IF;
        UPDATE u5_metrics SET v = (SELECT count(DISTINCT e_nid) FROM u5_cand) WHERE stage = 'connect' AND rule = r.name AND k = 'candidates';

        WITH del AS (DELETE FROM u5_cand WHERE levelkey <> b_level RETURNING e_nid)
        SELECT count(DISTINCT e_nid) INTO v_n FROM del;
        UPDATE u5_metrics SET v = v_n WHERE stage = 'connect' AND rule = r.name AND k = 'skipped_level';
        WITH del AS (DELETE FROM u5_cand c WHERE EXISTS (SELECT 1 FROM u5_taken t WHERE t.nid = c.e_nid) RETURNING e_nid)
        SELECT count(DISTINCT e_nid) INTO v_n FROM del;
        UPDATE u5_metrics SET v = v_n WHERE stage = 'connect' AND rule = r.name AND k = 'skipped_taken';
        -- W4 NEARER END ONLY: an end does not join B when the far end of an A edge
        -- at that node is nearer to B (that end makes the junction)
        WITH del AS (
            DELETE FROM u5_cand c
            WHERE NOT c.split_a AND EXISTS (
                SELECT 1 FROM (SELECT ST_EndPoint(a2.geom) AS far FROM u5_cae a2 WHERE a2.u = c.e_nid AND a2.src = c.a_src
                               UNION ALL
                               SELECT ST_StartPoint(a2.geom) FROM u5_cae a2 WHERE a2.v = c.e_nid AND a2.src = c.a_src) f
                WHERE content.union5_dist_m(f.far, ST_ClosestPoint(c.b_geom, f.far), v_mlon) < c.d - 1e-9)
            RETURNING e_nid)
        SELECT count(DISTINCT e_nid) INTO v_n FROM del;
        UPDATE u5_metrics SET v = v_n WHERE stage = 'connect' AND rule = r.name AND k = 'skipped_nearer_end';
        -- W5 ANGLE GUARD: a T onto B's span needs 30° or more; a gap to B's end node does not
        WITH del AS (
            DELETE FROM u5_cand c
            WHERE NOT c.split_a AND c.d > 0
              AND NOT ST_Equals(c.q, ST_StartPoint(c.b_geom)) AND NOT ST_Equals(c.q, ST_EndPoint(c.b_geom))
              AND (SELECT abs(  ((ST_X(c.e_pt) - ST_X(c.prev)) * v_mlon) * ((ST_X(g.pb) - ST_X(g.pa)) * v_mlon)
                              + ((ST_Y(c.e_pt) - ST_Y(c.prev)) * v_mlat) * ((ST_Y(g.pb) - ST_Y(g.pa)) * v_mlat))
                          / NULLIF(content.union5_dist_m(c.prev, c.e_pt, v_mlon) * content.union5_dist_m(g.pa, g.pb, v_mlon), 0)
                   FROM (SELECT ST_LineInterpolatePoint(c.b_geom, GREATEST(ST_LineLocatePoint(c.b_geom, c.q) - 1e-4, 0.0)) AS pa,
                                ST_LineInterpolatePoint(c.b_geom, LEAST(ST_LineLocatePoint(c.b_geom, c.q) + 1e-4, 1.0)) AS pb) g)
                  > 0.866025
            RETURNING e_nid)
        SELECT count(DISTINCT e_nid) INTO v_n FROM del;
        UPDATE u5_metrics SET v = v_n WHERE stage = 'connect' AND rule = r.name AND k = 'skipped_angle';

        -- W2: one junction per A end, to the nearest B (tie: lowest original _id); when
        -- several A edges end at that node, the connector takes the tags of the one
        -- with the lowest original _id
        DROP TABLE IF EXISTS u5_cj;
        CREATE TEMP TABLE u5_cj ON COMMIT DROP AS
        SELECT DISTINCT ON (e_nid) a_eid, a_src, e_nid, e_pt, b_eid, b_src, bu, bv, b_geom, q, d, split_a,
               NULL::text AS kind, NULL::bigint AS target
        FROM u5_cand
        ORDER BY e_nid, d, b_orig, b_eid, a_orig, a_eid;
        ANALYZE u5_cj;
        -- W3: two ends facing across one gap make ONE junction (the shorter gap; tie: DS1's end)
        WITH pairs AS (
            SELECT c1.e_nid AS n1, c2.e_nid AS n2 FROM u5_cj c1 JOIN u5_cj c2 ON c1.b_eid = c2.a_eid AND c2.b_eid = c1.a_eid
            UNION SELECT c1.e_nid, c2.e_nid FROM u5_cj c1 JOIN u5_cj c2 ON c2.e_nid = c1.bu AND (c1.e_nid = c2.bu OR c1.e_nid = c2.bv)
            UNION SELECT c1.e_nid, c2.e_nid FROM u5_cj c1 JOIN u5_cj c2 ON c2.e_nid = c1.bv AND (c1.e_nid = c2.bu OR c1.e_nid = c2.bv)),
        del AS (
            DELETE FROM u5_cj c1 USING pairs p, u5_cj c2
            WHERE p.n1 = c1.e_nid AND p.n2 = c2.e_nid AND c1.e_nid <> c2.e_nid AND NOT c1.split_a AND NOT c2.split_a
              AND (c1.d, c1.a_src, c1.e_nid) > (c2.d, c2.a_src, c2.e_nid)
            RETURNING c1.e_nid)
        INSERT INTO u5_taken SELECT DISTINCT e_nid FROM del ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS v_n = ROW_COUNT;
        UPDATE u5_metrics SET v = v_n WHERE stage = 'connect' AND rule = r.name AND k = 'skipped_mutual';

        -- ── point: lines that cross with no shared node ──
        IF r.op = 'point' THEN
            DROP TABLE IF EXISTS u5_cx;
            CREATE TEMP TABLE u5_cx ON COMMIT DROP AS
            SELECT a.eid AS a_eid, a.src AS a_src, a.geom AS a_geom, b.eid AS b_eid, b.src AS b_src, b.u AS bu, b.v AS bv,
                   b.geom AS b_geom, (ST_Dump(ST_Intersection(a.geom, b.geom))).geom AS p, (a.levelkey = b.levelkey) AS same_level
            FROM u5_cae a JOIN u5_cb b ON b.src = 2 AND a.geom && b.geom AND ST_Intersects(a.geom, b.geom) WHERE a.src = 1
            UNION ALL
            SELECT a.eid, a.src, a.geom, b.eid, b.src, b.u, b.v, b.geom,
                   (ST_Dump(ST_Intersection(a.geom, b.geom))).geom, (a.levelkey = b.levelkey)
            FROM u5_cae a JOIN u5_cb b ON b.src = 1 AND a.geom && b.geom AND ST_Intersects(a.geom, b.geom) WHERE a.src = 2;
            DELETE FROM u5_cx WHERE GeometryType(p) <> 'POINT'
                OR ST_Equals(p, ST_StartPoint(a_geom)) OR ST_Equals(p, ST_EndPoint(a_geom));   -- an A end on B: handled above
            UPDATE u5_metrics SET v = v + (SELECT count(*) FROM u5_cx) WHERE stage = 'connect' AND rule = r.name AND k = 'candidates';
            WITH del AS (DELETE FROM u5_cx WHERE NOT same_level RETURNING 1) SELECT count(*) INTO v_n FROM del;
            UPDATE u5_metrics SET v = v + v_n WHERE stage = 'connect' AND rule = r.name AND k = 'skipped_level';
            INSERT INTO u5_cj (a_eid, a_src, e_nid, e_pt, b_eid, b_src, bu, bv, b_geom, q, d, split_a, kind, target)
            SELECT a_eid, a_src, NULL, NULL, b_eid, b_src, bu, bv, b_geom, p, 0, true, 'cross', NULL FROM u5_cx;
        END IF;

        IF EXISTS (SELECT 1 FROM u5_cj) THEN
            -- O2: within reach of a node an earlier rule made or used on B → that node
            UPDATE u5_cj c SET kind = 'reuse', target = (
                SELECT n.nid FROM u5_jn jn JOIN u5_n n ON n.nid = jn.nid AND n.alive
                WHERE n.nid <> c.e_nid AND n.geom && ST_Expand(c.q, r.prox_deg)
                  AND content.union5_dist_m(n.geom, c.q, v_mlon) <= r.prox
                  AND (EXISTS (SELECT 1 FROM u5_cb b WHERE b.u = n.nid AND b.src = c.b_src)
                       OR EXISTS (SELECT 1 FROM u5_cb b WHERE b.v = n.nid AND b.src = c.b_src))
                ORDER BY content.union5_dist_m(n.geom, c.q, v_mlon), n.nid LIMIT 1)
            WHERE c.kind IS NULL AND c.d > 0 AND NOT c.split_a;
            UPDATE u5_cj SET kind = NULL WHERE kind = 'reuse' AND target IS NULL;
            -- gap of zero: A's end lies on B, so A's end node is the junction
            UPDATE u5_cj SET kind = 'onspan', target = e_nid WHERE kind IS NULL AND d = 0 AND NOT split_a;
            -- a gap to B's end → B's end node
            UPDATE u5_cj SET kind = 'end', target = CASE WHEN ST_Equals(q, ST_StartPoint(b_geom)) THEN bu ELSE bv END
            WHERE kind IS NULL AND (ST_Equals(q, ST_StartPoint(b_geom)) OR ST_Equals(q, ST_EndPoint(b_geom)));
            -- a node already at the point (one of B's vertex nodes) → that node
            UPDATE u5_cj c SET target = (SELECT n.nid FROM u5_n n WHERE n.alive AND n.geom && c.q AND ST_Equals(n.geom, c.q)
                                         ORDER BY n.src, n.nid LIMIT 1)
            WHERE (c.kind IS NULL OR c.kind = 'cross') AND c.target IS NULL;
            UPDATE u5_cj SET kind = 'vertex' WHERE kind IS NULL AND target IS NOT NULL;
            -- otherwise a new node on B, on the 7 dp grid (one per distinct point)
            v_first_nid := v_next_nid;
            UPDATE u5_cj SET q = ST_SnapToGrid(q, v_grid) WHERE target IS NULL;
            INSERT INTO u5_n (nid, src, orig_id, geom, props, alive)
            SELECT v_next_nid + ROW_NUMBER() OVER (ORDER BY ST_X(p), ST_Y(p)) - 1, 0, NULL, p, '{}'::jsonb, true
            FROM (SELECT DISTINCT q AS p FROM u5_cj WHERE target IS NULL) x;
            GET DIAGNOSTICS v_n = ROW_COUNT;
            v_next_nid := v_next_nid + v_n;
            UPDATE u5_cj c SET kind = COALESCE(c.kind, 'new'),
                               target = (SELECT n.nid FROM u5_n n WHERE n.nid >= v_first_nid AND n.geom && c.q
                                         AND ST_Equals(n.geom, c.q) LIMIT 1)
            WHERE c.target IS NULL;

            -- an A end lying on B where another node already sits (left apart when
            -- coincident snapping is off): they become one node, kept by priority
            -- created > DS1 > DS2, then lowest id
            DROP TABLE IF EXISTS u5_merge;
            CREATE TEMP TABLE u5_merge ON COMMIT DROP AS
            SELECT DISTINCT ON (gone) gone, keep FROM (
                SELECT CASE WHEN (x.src, x.nid) < (e.src, e.nid) THEN e.nid ELSE x.nid END AS gone,
                       CASE WHEN (x.src, x.nid) < (e.src, e.nid) THEN x.nid ELSE e.nid END AS keep
                FROM u5_cj c
                JOIN u5_n e ON e.nid = c.e_nid
                JOIN u5_n x ON x.alive AND x.nid <> c.e_nid AND x.geom && c.e_pt AND ST_Equals(x.geom, c.e_pt)
                WHERE c.kind = 'onspan') m
            ORDER BY gone, keep;
            IF EXISTS (SELECT 1 FROM u5_merge) THEN
                UPDATE u5_n n SET alive = false, merged_into = m.keep FROM u5_merge m WHERE n.nid = m.gone;
                UPDATE u5_e e SET u = m.keep FROM u5_merge m WHERE e.u = m.gone;
                UPDATE u5_e e SET v = m.keep FROM u5_merge m WHERE e.v = m.gone;
                UPDATE u5_cj c SET target = m.keep FROM u5_merge m WHERE c.target = m.gone;
                UPDATE u5_cj c SET e_nid = m.keep FROM u5_merge m WHERE c.e_nid = m.gone;
                UPDATE u5_cb b SET u = m.keep FROM u5_merge m WHERE b.u = m.gone;
                UPDATE u5_cb b SET v = m.keep FROM u5_merge m WHERE b.v = m.gone;
            END IF;

            -- splits: B where the junction lies on its span; A where it crosses B
            INSERT INTO u5_split (eid, frac, nid)
            SELECT c.b_eid, ST_LineLocatePoint(c.b_geom, n.geom), c.target
            FROM u5_cj c JOIN u5_n n ON n.nid = c.target
            WHERE c.kind IN ('onspan', 'vertex', 'new', 'cross');
            INSERT INTO u5_split (eid, frac, nid)
            SELECT c.a_eid, ST_LineLocatePoint(a.geom, n.geom), c.target
            FROM u5_cj c JOIN u5_e a ON a.eid = c.a_eid JOIN u5_n n ON n.nid = c.target
            WHERE c.split_a;
            SELECT count(DISTINCT s.eid) INTO v_n FROM u5_split s JOIN u5_e e ON e.eid = s.eid
            WHERE s.frac > 1e-12 AND s.frac < 1 - 1e-12 AND s.nid <> e.u AND s.nid <> e.v;
            UPDATE u5_metrics SET v = v_n WHERE stage = 'connect' AND rule = r.name AND k = 'edges_split';
            v_next_eid := content.union5_apply_splits(v_next_eid);

            -- connectors across a gap: A's highway/footway + the rule's edge_tags
            INSERT INTO u5_e (eid, src, orig_id, geom, props, u, v, alive, tm, is_connector, rule, levelkey)
            SELECT v_next_eid + ROW_NUMBER() OVER (ORDER BY c.e_nid), 0, NULL,
                   ST_MakeLine(en.geom, tn.geom),
                   jsonb_strip_nulls(jsonb_build_object('highway', a.props->'highway', 'footway', a.props->'footway'))
                     || content.union5_tags_for('edge', r.edge_tags)
                     || jsonb_build_object('ext:union_rule', r.name,
                                           'ext:union_confidence', round(GREATEST(0.0, 1.0 - c.d / r.prox)::numeric, 3)),
                   c.e_nid, c.target, true, '{}', true, r.name, a.levelkey
            FROM u5_cj c
            JOIN u5_e a ON a.eid = c.a_eid
            JOIN u5_n en ON en.nid = c.e_nid
            JOIN u5_n tn ON tn.nid = c.target
            WHERE c.kind IN ('reuse', 'end', 'vertex', 'new') AND NOT c.split_a AND c.e_nid IS NOT NULL
              AND c.target <> c.e_nid AND NOT ST_Equals(en.geom, tn.geom);
            GET DIAGNOSTICS v_n = ROW_COUNT;
            v_next_eid := v_next_eid + v_n + 1;
            UPDATE u5_metrics SET v = v_n WHERE stage = 'connect' AND rule = r.name AND k = 'connectors';

            -- the junction node: the rule's node_tags (existing values win; differing
            -- ones audited), the rules that used it (in rule order), a confidence
            v_ntags := content.union5_tags_for('node', r.node_tags);
            UPDATE u5_n n SET
                props = n.props || COALESCE((SELECT jsonb_object_agg(t.key, t.value) FROM jsonb_each(v_ntags) t
                                             WHERE NOT n.props ? t.key), '{}'::jsonb),
                audit = content.union5_audit_add(n.audit, (
                            SELECT jsonb_object_agg('ext:union_audit_' || regexp_replace(t.key, '^ext:', ''),
                                                    to_jsonb(r.name || '-' || (t.value #>> '{}')) ORDER BY t.key)
                            FROM jsonb_each(v_ntags) t WHERE n.props ? t.key AND n.props->t.key IS DISTINCT FROM t.value)),
                union_rules = CASE WHEN n.union_rules IS NULL THEN r.name
                                   WHEN r.name = ANY(string_to_array(n.union_rules, ',')) THEN n.union_rules
                                   ELSE n.union_rules || ',' || r.name END,
                confidence = GREATEST(COALESCE(n.confidence, 0.0), x.conf)
            FROM (SELECT target, max(CASE WHEN kind = 'cross' OR d = 0 THEN 1.0 ELSE GREATEST(0.0, 1.0 - d / r.prox) END) AS conf
                  FROM u5_cj GROUP BY target) x
            WHERE n.nid = x.target;

            INSERT INTO u5_taken SELECT DISTINCT e_nid FROM u5_cj WHERE e_nid IS NOT NULL ON CONFLICT DO NOTHING;
            INSERT INTO u5_jn SELECT DISTINCT target FROM u5_cj ON CONFLICT DO NOTHING;
            UPDATE u5_metrics SET v = (SELECT count(*) FROM u5_cj) WHERE stage = 'connect' AND rule = r.name AND k = 'junctions';
            UPDATE u5_metrics SET v = (SELECT count(DISTINCT target) FROM u5_cj WHERE target >= v_first_nid)
            WHERE stage = 'connect' AND rule = r.name AND k = 'nodes_created';
            UPDATE u5_metrics SET v = (SELECT count(DISTINCT target) FROM u5_cj WHERE target < v_first_nid)
            WHERE stage = 'connect' AND rule = r.name AND k = 'nodes_reused';
            UPDATE u5_metrics SET v = (SELECT count(*) FROM u5_cj WHERE kind = 'reuse')
            WHERE stage = 'connect' AND rule = r.name AND k = 'joined_earlier_node';
        END IF;
        RAISE NOTICE '  connect rule % (%): % junction(s) at %', r.seq, r.name,
            (SELECT v FROM u5_metrics WHERE stage = 'connect' AND rule = r.name AND k = 'junctions'), clock_timestamp();
    END LOOP;
    IF EXISTS (SELECT 1 FROM u5_connect_rules) THEN
        RAISE NOTICE 'Phase 5 complete: % junctions, % connectors',
            (SELECT COALESCE(sum(v), 0) FROM u5_metrics WHERE stage = 'connect' AND k = 'junctions'),
            (SELECT count(*) FROM u5_e WHERE alive AND is_connector);
    END IF;
    v_timings := v_timings || jsonb_build_object('connect', round(extract(epoch FROM clock_timestamp() - v_ts)::numeric, 2));
    v_ts := clock_timestamp();
    -- O1: a rule that found locations but was beaten to all of them by earlier rules
    v_warnings := v_warnings || ARRAY(
        SELECT format('connect rule "%s" matched %s location(s), all taken by earlier rules — it made no junction',
                      j.rule, t.v)
        FROM u5_metrics j JOIN u5_metrics t ON t.stage = j.stage AND t.rule = j.rule AND t.k = 'skipped_taken'
        WHERE j.stage = 'connect' AND j.k = 'junctions' AND j.v = 0 AND t.v > 0 ORDER BY j.seq);

    -- =========================================================================
    -- PHASE 6 — STRANDING GUARD: restore a removed duplicate when its removal
    -- left a surviving edge hanging at a node that is not on the anchor network
    -- (no anchor edge and no connector uses it). Repeated until stable.
    -- =========================================================================
    -- Round by round: the first round looks at every removed duplicate; a later
    -- round only at those sharing a node with an edge the round before restored
    -- (nothing else changed for the rest), so the work follows the restorations.
    DROP TABLE IF EXISTS u5_rm;
    CREATE TEMP TABLE u5_rm ON COMMIT DROP AS
    SELECT eid, u, v FROM u5_e WHERE removed_by IS NOT NULL AND NOT alive;
    CREATE INDEX ON u5_rm (u);
    CREATE INDEX ON u5_rm (v);
    ANALYZE u5_rm;
    DROP TABLE IF EXISTS u5_wl;
    CREATE TEMP TABLE u5_wl (eid bigint PRIMARY KEY) ON COMMIT DROP;
    INSERT INTO u5_wl SELECT eid FROM u5_rm;
    DROP TABLE IF EXISTS u5_rs;
    CREATE TEMP TABLE u5_rs (eid bigint, u bigint, v bigint) ON COMMIT DROP;
    LOOP
        TRUNCATE u5_rs;
        WITH up AS (
            UPDATE u5_e x SET alive = true
            FROM u5_wl w
            WHERE x.eid = w.eid AND x.removed_by IS NOT NULL AND NOT x.alive
              AND EXISTS (
                  SELECT 1 FROM (VALUES (x.u), (x.v)) ends(nid)
                  WHERE (EXISTS (SELECT 1 FROM u5_e s WHERE s.u = ends.nid AND s.alive)
                         OR EXISTS (SELECT 1 FROM u5_e s WHERE s.v = ends.nid AND s.alive))
                    AND NOT EXISTS (SELECT 1 FROM u5_e a WHERE a.u = ends.nid AND a.alive AND (a.is_connector OR a.src = x.removed_anchor))
                    AND NOT EXISTS (SELECT 1 FROM u5_e a WHERE a.v = ends.nid AND a.alive AND (a.is_connector OR a.src = x.removed_anchor)))
            RETURNING x.eid, x.u, x.v)
        INSERT INTO u5_rs SELECT eid, u, v FROM up;
        GET DIAGNOSTICS v_n = ROW_COUNT;
        EXIT WHEN v_n = 0;
        TRUNCATE u5_wl;
        INSERT INTO u5_wl
        SELECT m.eid FROM u5_rs s JOIN u5_rm m ON m.u = s.u
        UNION SELECT m.eid FROM u5_rs s JOIN u5_rm m ON m.u = s.v
        UNION SELECT m.eid FROM u5_rs s JOIN u5_rm m ON m.v = s.u
        UNION SELECT m.eid FROM u5_rs s JOIN u5_rm m ON m.v = s.v;
    END LOOP;
    UPDATE u5_metrics mt SET v = x.n
    FROM (SELECT removed_by, count(*) AS n FROM u5_e WHERE removed_by IS NOT NULL AND alive GROUP BY removed_by) x
    WHERE mt.stage = 'dedup' AND mt.rule = x.removed_by AND mt.k = 'restored';
    UPDATE u5_metrics mt SET v = mt.v - x.n
    FROM (SELECT removed_by, count(*) AS n FROM u5_e WHERE removed_by IS NOT NULL AND alive GROUP BY removed_by) x
    WHERE mt.stage = 'dedup' AND mt.rule = x.removed_by AND mt.k = 'removed';
    UPDATE u5_e SET removed_by = NULL, absorbed_by = NULL WHERE removed_by IS NOT NULL AND alive;
    v_timings := v_timings || jsonb_build_object('stranding_guard', round(extract(epoch FROM clock_timestamp() - v_ts)::numeric, 2));
    v_ts := clock_timestamp();

    -- =========================================================================
    -- PHASE 7 — zones and extension features ('extensions': off = pass through;
    -- default = the 4.3.2 merging). Zone rings reference the final nodes.
    -- =========================================================================
    RAISE NOTICE 'Phase 7: zones and extensions (%) at %', v_ext->>'mode', clock_timestamp();
    v_ext_prox := COALESCE((v_ext->>'proximity')::float8, 0.5);
    v_ext_deg  := v_ext_prox / v_mlon;
    v_ext_ldeg := COALESCE((v_ext->>'line_proximity')::float8, v_ext_prox) / v_mlon;

    -- zones: each ring vertex → its node (by _w_id position, else by coordinate) → final node
    DROP TABLE IF EXISTS u5_zones;
    CREATE TEMP TABLE u5_zones ON COMMIT DROP AS
    SELECT z.id AS zid, CASE WHEN z.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END::smallint AS src,
           z.zone_loc AS geom, z.node_ids, (z.feature::jsonb->'properties') - '_id' - '_w_id' AS props
    FROM content.zone z WHERE z.tdei_dataset_id IN (v_ds1, v_ds2);
    DROP TABLE IF EXISTS u5_zone_pts;
    CREATE TEMP TABLE u5_zone_pts ON COMMIT DROP AS
    SELECT z.zid, z.src, d.path[1] AS ring, d.path[2] AS pos, d.geom AS vgeom,
           CASE WHEN cardinality(z.node_ids) = ST_NPoints(z.geom) THEN z.node_ids[d.ord] END AS w_orig,
           NULL::bigint AS nid
    FROM u5_zones z, LATERAL ST_DumpPoints(z.geom) WITH ORDINALITY AS d(path, geom, ord);
    UPDATE u5_zone_pts p SET nid = (SELECT n.nid FROM u5_n n WHERE n.src = p.src AND n.orig_id = p.w_orig::text
                                    ORDER BY n.nid LIMIT 1)
    WHERE p.w_orig IS NOT NULL;
    UPDATE u5_zone_pts p SET nid = (SELECT n.nid FROM u5_n n WHERE n.src = p.src AND n.geom && p.vgeom
                                    AND ST_Equals(n.geom, p.vgeom) ORDER BY n.nid LIMIT 1)
    WHERE p.nid IS NULL;
    INSERT INTO u5_inref SELECT DISTINCT nid FROM u5_zone_pts WHERE nid IS NOT NULL ON CONFLICT DO NOTHING;

    -- the final node of each zone node (follows joins)
    DROP TABLE IF EXISTS u5_final;
    CREATE TEMP TABLE u5_final ON COMMIT DROP AS
    WITH RECURSIVE ch(nid, cur, depth) AS (
        SELECT nid, nid, 0 FROM (SELECT DISTINCT nid FROM u5_zone_pts WHERE nid IS NOT NULL) z
        UNION ALL
        SELECT ch.nid, n.merged_into, ch.depth + 1 FROM ch JOIN u5_n n ON n.nid = ch.cur
        WHERE n.merged_into IS NOT NULL AND ch.depth < 16)
    SELECT DISTINCT ON (nid) nid, cur AS final FROM ch ORDER BY nid, depth DESC;
    CREATE INDEX ON u5_final (nid);

    DROP TABLE IF EXISTS u5_zone_rings;
    CREATE TEMP TABLE u5_zone_rings ON COMMIT DROP AS
    SELECT p.zid, p.ring,
           ST_MakeLine(COALESCE(fn.geom, p.vgeom) ORDER BY p.pos) AS line,
           array_agg(f.final ORDER BY p.pos) FILTER (WHERE f.final IS NOT NULL) AS ids
    FROM u5_zone_pts p
    LEFT JOIN u5_final f ON f.nid = p.nid
    LEFT JOIN u5_n fn ON fn.nid = f.final
    GROUP BY p.zid, p.ring;
    DROP TABLE IF EXISTS u5_zone_out;
    CREATE TEMP TABLE u5_zone_out ON COMMIT DROP AS
    SELECT z.zid, z.src, z.props,
           CASE WHEN array_length(rr.inner_rings, 1) > 0 THEN ST_MakePolygon(rr.outer_ring, rr.inner_rings)
                ELSE ST_MakePolygon(rr.outer_ring) END AS geom,
           -- _w_id: every ring's node ids, outer ring first; adjacent repeats dropped
           ARRAY(SELECT val::text FROM (
                     SELECT val, idx, LAG(val) OVER (ORDER BY idx) AS prev
                     FROM unnest(rr.ids) WITH ORDINALITY t(val, idx)) s
                 WHERE prev IS DISTINCT FROM val ORDER BY idx) AS w_ids
    FROM u5_zones z
    JOIN (SELECT r1.zid,
                 (array_agg(r1.line ORDER BY r1.ring) FILTER (WHERE r1.ring = 1))[1] AS outer_ring,
                 array_agg(r1.line ORDER BY r1.ring) FILTER (WHERE r1.ring > 1) AS inner_rings,
                 ARRAY(SELECT u.x FROM u5_zone_rings r2, unnest(r2.ids) WITH ORDINALITY u(x, o)
                       WHERE r2.zid = r1.zid ORDER BY r2.ring, u.o) AS ids
          FROM u5_zone_rings r1 GROUP BY r1.zid) rr ON rr.zid = z.zid
    WHERE ST_NPoints(rr.outer_ring) >= 4 AND ST_IsClosed(rr.outer_ring);
    IF (v_ext->>'zones')::boolean THEN        -- default mode with zones on
        -- a DS2 zone covering more than zone_overlap of a DS1 zone's area is the same zone (DS1's kept)
        CREATE INDEX ON u5_zone_out USING GIST (geom);
        ANALYZE u5_zone_out;
        DELETE FROM u5_zone_out zd USING (
            SELECT DISTINCT ON (z1.zid) z1.zid, z2.zid AS ds2_zid
            FROM u5_zone_out z1 JOIN u5_zone_out z2 ON z1.src = 1 AND z2.src = 2
                 AND ST_IsValid(z1.geom) AND ST_IsValid(z2.geom) AND ST_Intersects(z1.geom, z2.geom)
                 AND ST_Area(ST_Intersection(z1.geom, z2.geom)) / NULLIF(ST_Area(z1.geom), 0) > (v_ext->>'zone_overlap')::float8
            ORDER BY z1.zid, ST_Area(ST_Intersection(z1.geom, z2.geom)) / NULLIF(ST_Area(z1.geom), 0) DESC, z2.zid) m
        WHERE zd.zid = m.ds2_zid;
    END IF;

    -- extension points: a DS2 point within proximity of a SAME-TYPE DS1 point is the
    -- same feature (DS1 position; DS2-only keys added; every DS2 value audited)
    DROP TABLE IF EXISTS u5_pts;
    CREATE TEMP TABLE u5_pts ON COMMIT DROP AS
    SELECT p.id, CASE WHEN p.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END::smallint AS src,
           p.feature::jsonb->'properties'->>'_id' AS orig_id, p.point_loc AS geom,
           (p.feature::jsonb->'properties') - '_id' AS props,
           COALESCE((SELECT string_agg(kv.key || '=' || kv.value, ',' ORDER BY kv.key)
                     FROM jsonb_each_text(p.feature::jsonb->'properties') kv
                     WHERE kv.key <> '_id' AND kv.key NOT LIKE 'ext:%'), 'other') AS ptype
    FROM content.extension_point p WHERE p.tdei_dataset_id IN (v_ds1, v_ds2);
    CREATE INDEX ON u5_pts USING GIST (geom);
    CREATE INDEX ON u5_pts (id);
    ANALYZE u5_pts;
    DROP TABLE IF EXISTS u5_pt_match;
    CREATE TEMP TABLE u5_pt_match (ds2_id bigint, ds1_id bigint) ON COMMIT DROP;
    IF (v_ext->>'points')::boolean THEN        -- default mode with points on
        INSERT INTO u5_pt_match
        SELECT DISTINCT ON (p2.id) p2.id, p1.id
        FROM u5_pts p2 JOIN u5_pts p1 ON p1.src = 1 AND p2.src = 2
            AND ST_DWithin(p2.geom, p1.geom, v_ext_deg)
            AND content.union5_dist_m(p2.geom, p1.geom, v_mlon) <= v_ext_prox
            AND (p1.ptype = 'other' OR p2.ptype = 'other' OR p1.ptype = p2.ptype)
        ORDER BY p2.id, content.union5_dist_m(p2.geom, p1.geom, v_mlon), p1.id;
    END IF;
    CREATE INDEX ON u5_pt_match (ds1_id);           -- each DS1 point finds its matches by index
    CREATE INDEX ON u5_pt_match (ds2_id);
    ANALYZE u5_pt_match;
    DROP TABLE IF EXISTS u5_out_points;
    CREATE TEMP TABLE u5_out_points ON COMMIT DROP AS
    SELECT x.id, jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(x.geom, 15)::jsonb,
                                    'properties', jsonb_build_object('_id', x.id::text) || x.props) AS feature
    FROM (
        SELECT p1.id, p1.geom,
               p1.props
               || COALESCE((SELECT jsonb_object_agg(y.key, y.value) FROM (
                      SELECT DISTINCT ON (kv.key) kv.key, kv.value FROM u5_pt_match m JOIN u5_pts p2 ON p2.id = m.ds2_id,
                             jsonb_each(p2.props) kv
                      WHERE m.ds1_id = p1.id AND NOT p1.props ? kv.key ORDER BY kv.key, p2.orig_id DESC) y), '{}'::jsonb)
               || COALESCE((SELECT jsonb_object_agg('ext:union_audit_' || regexp_replace(y.key, '^ext:', ''), to_jsonb(y.s) ORDER BY y.key) FROM (
                      SELECT kv.key, string_agg(COALESCE(p2.orig_id, '?') || '-' || (kv.value #>> '{}'), ',' ORDER BY p2.orig_id) AS s
                      FROM u5_pt_match m JOIN u5_pts p2 ON p2.id = m.ds2_id, jsonb_each(p2.props) kv
                      WHERE m.ds1_id = p1.id GROUP BY kv.key) y), '{}'::jsonb) AS props
        FROM u5_pts p1 WHERE p1.src = 1
        UNION ALL
        SELECT p2.id, p2.geom, p2.props FROM u5_pts p2
        WHERE p2.src = 2 AND NOT EXISTS (SELECT 1 FROM u5_pt_match m WHERE m.ds2_id = p2.id)) x;

    -- extension lines: a DS2 line with at least line_overlap of its length inside a
    -- DS1 line's line_proximity buffer is the same line (DS1 geometry; DS2-only keys added)
    DROP TABLE IF EXISTS u5_lines;
    CREATE TEMP TABLE u5_lines ON COMMIT DROP AS
    SELECT l.id, CASE WHEN l.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END::smallint AS src,
           l.line_loc AS geom, (l.feature::jsonb->'properties') - '_id' AS props
    FROM content.extension_line l WHERE l.tdei_dataset_id IN (v_ds1, v_ds2);
    CREATE INDEX ON u5_lines (id);
    ANALYZE u5_lines;
    DROP TABLE IF EXISTS u5_ln_match;
    CREATE TEMP TABLE u5_ln_match (ds2_id bigint, ds1_id bigint) ON COMMIT DROP;
    IF (v_ext->>'lines')::boolean THEN        -- default mode with lines on
        DROP TABLE IF EXISTS u5_ln_buf;
        CREATE TEMP TABLE u5_ln_buf ON COMMIT DROP AS
        SELECT id, ST_Buffer(geom, v_ext_ldeg) AS buf FROM u5_lines WHERE src = 1;
        CREATE INDEX ON u5_ln_buf USING GIST (buf);
        INSERT INTO u5_ln_match
        SELECT DISTINCT ON (ds2_id) ds2_id, ds1_id FROM (
            SELECT l2.id AS ds2_id, b.id AS ds1_id,
                   ST_Length(ST_CollectionExtract(ST_Intersection(l2.geom, b.buf), 2)) / NULLIF(ST_Length(l2.geom), 0) AS ov
            FROM u5_lines l2 JOIN u5_ln_buf b ON l2.src = 2 AND ST_DWithin(l2.geom, b.buf, 0)) c
        WHERE ov >= (v_ext->>'line_overlap')::float8
        ORDER BY ds2_id, ov DESC, ds1_id;
    END IF;
    CREATE INDEX ON u5_ln_match (ds1_id);
    CREATE INDEX ON u5_ln_match (ds2_id);
    ANALYZE u5_ln_match;
    DROP TABLE IF EXISTS u5_out_lines;
    CREATE TEMP TABLE u5_out_lines ON COMMIT DROP AS
    SELECT x.id, jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(x.geom, 15)::jsonb,
                                    'properties', jsonb_build_object('_id', x.id::text) || x.props) AS feature
    FROM (
        SELECT l1.id, l1.geom,
               l1.props || COALESCE((SELECT jsonb_object_agg(kv.key, kv.value ORDER BY l2.id)   -- the highest id wins
                                     FROM u5_ln_match m JOIN u5_lines l2 ON l2.id = m.ds2_id, jsonb_each(l2.props) kv
                                     WHERE m.ds1_id = l1.id AND NOT l1.props ? kv.key), '{}'::jsonb) AS props
        FROM u5_lines l1 WHERE l1.src = 1
        UNION ALL
        SELECT l2.id, l2.geom, l2.props FROM u5_lines l2
        WHERE l2.src = 2 AND NOT EXISTS (SELECT 1 FROM u5_ln_match m WHERE m.ds2_id = l2.id)) x;

    -- extension polygons: a DS2 polygon covering more than polygon_overlap of a DS1
    -- polygon's area → DS1 geometry + DS2-only keys; intersecting below that → the
    -- union of both; otherwise each is kept
    DROP TABLE IF EXISTS u5_polys;
    CREATE TEMP TABLE u5_polys ON COMMIT DROP AS
    SELECT p.id, CASE WHEN p.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END::smallint AS src,
           p.polygon_loc AS geom, (p.feature::jsonb->'properties') - '_id' AS props
    FROM content.extension_polygon p WHERE p.tdei_dataset_id IN (v_ds1, v_ds2);
    DROP TABLE IF EXISTS u5_poly_pairs;
    CREATE TEMP TABLE u5_poly_pairs (ds1_id bigint, ds2_id bigint, ov float8) ON COMMIT DROP;
    IF (v_ext->>'polygons')::boolean THEN        -- default mode with polygons on
        CREATE INDEX ON u5_polys USING GIST (geom);  -- each DS1 polygon meets only the DS2 polygons it touches
        CREATE INDEX ON u5_polys (id);
        ANALYZE u5_polys;
        INSERT INTO u5_poly_pairs
        SELECT DISTINCT ON (p1.id) p1.id, p2.id, ST_Area(ST_Intersection(p1.geom, p2.geom)) / NULLIF(ST_Area(p1.geom), 0)
        FROM u5_polys p1 JOIN u5_polys p2 ON p1.src = 1 AND p2.src = 2
             AND ST_IsValid(p1.geom) AND ST_IsValid(p2.geom) AND ST_Intersects(p1.geom, p2.geom)
        ORDER BY p1.id, ST_Area(ST_Intersection(p1.geom, p2.geom)) / NULLIF(ST_Area(p1.geom), 0) DESC, p2.id;
    END IF;
    CREATE INDEX ON u5_poly_pairs (ds1_id);
    CREATE INDEX ON u5_poly_pairs (ds2_id);
    ANALYZE u5_poly_pairs;
    DROP TABLE IF EXISTS u5_out_polygons;
    CREATE TEMP TABLE u5_out_polygons ON COMMIT DROP AS
    SELECT x.id, jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(x.geom, 15)::jsonb,
                                    'properties', jsonb_build_object('_id', x.id::text) || x.props) AS feature
    FROM (
        SELECT p1.id,
               CASE WHEN pp.ds1_id IS NULL OR pp.ov > (v_ext->>'polygon_overlap')::float8 THEN p1.geom
                    ELSE ST_Union(p1.geom, p2.geom) END AS geom,
               p1.props || COALESCE((SELECT jsonb_object_agg(kv.key, kv.value) FROM jsonb_each(p2.props) kv
                                     WHERE NOT p1.props ? kv.key), '{}'::jsonb) AS props
        FROM u5_polys p1
        LEFT JOIN u5_poly_pairs pp ON pp.ds1_id = p1.id
        LEFT JOIN u5_polys p2 ON p2.id = pp.ds2_id
        WHERE p1.src = 1
        UNION ALL
        SELECT p2.id, p2.geom, p2.props FROM u5_polys p2
        WHERE p2.src = 2 AND NOT EXISTS (SELECT 1 FROM u5_poly_pairs pp WHERE pp.ds2_id = p2.id)) x;
    v_timings := v_timings || jsonb_build_object('extensions', round(extract(epoch FROM clock_timestamp() - v_ts)::numeric, 2));
    v_ts := clock_timestamp();

    -- =========================================================================
    -- PHASE 8 — output: node, edge, zone, point, line, polygon, report
    -- =========================================================================
    RAISE NOTICE 'Phase 8: output at %', clock_timestamp();
    -- an anchor edge records the original _ids of the duplicates it replaced
    DROP TABLE IF EXISTS u5_absorbed;
    CREATE TEMP TABLE u5_absorbed ON COMMIT DROP AS
    SELECT absorbed_by AS root, string_agg(COALESCE(orig_id, eid::text), ',' ORDER BY orig_id) AS ids
    FROM u5_e WHERE removed_by IS NOT NULL AND NOT alive AND absorbed_by IS NOT NULL GROUP BY absorbed_by;

    DROP TABLE IF EXISTS u5_out_edges;
    CREATE TEMP TABLE u5_out_edges ON COMMIT DROP AS
    SELECT y.seq_id AS id, y.u, y.v,
           jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(y.geom, 15)::jsonb,
               'properties', jsonb_build_object('_id', y.seq_id::text, '_u_id', y.u::text, '_v_id', y.v::text)
                             || y.props
                             || CASE WHEN y.ids IS NOT NULL THEN jsonb_build_object('ext:union_absorbed', y.ids)
                                     ELSE '{}'::jsonb END) AS feature
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY e.src = 0, COALESCE(e.parent, e.eid), e.eid) AS seq_id,
                 e.u, e.v, e.geom, e.props, ab.ids
          FROM u5_e e
          LEFT JOIN u5_absorbed ab ON ab.root = COALESCE(e.parent, e.eid)
          WHERE e.alive AND ST_NPoints(e.geom) >= 2 AND ST_Length(e.geom) > 0) y;

    DROP TABLE IF EXISTS u5_out_zones;
    CREATE TEMP TABLE u5_out_zones ON COMMIT DROP AS
    SELECT zid AS id, w_ids,
           jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(geom, 15)::jsonb,
               'properties', jsonb_build_object('_id', zid::text) || props || jsonb_build_object('_w_id', to_jsonb(w_ids))) AS feature
    FROM u5_zone_out WHERE geom IS NOT NULL;

    -- nodes: every node an output edge or zone uses, plus input orphans (passed through)
    DROP TABLE IF EXISTS u5_ref;
    CREATE TEMP TABLE u5_ref ON COMMIT DROP AS
    SELECT u AS nid FROM u5_out_edges UNION SELECT v FROM u5_out_edges
    UNION SELECT w::bigint FROM u5_out_zones, unnest(w_ids) w;
    CREATE INDEX ON u5_ref (nid);
    DROP TABLE IF EXISTS u5_out_nodes;
    CREATE TEMP TABLE u5_out_nodes ON COMMIT DROP AS
    SELECT n.nid AS id,
           jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(n.geom, 15)::jsonb,
               'properties', jsonb_build_object('_id', n.nid::text) || n.props || n.audit
                   || CASE WHEN n.absorbed IS NOT NULL THEN jsonb_build_object('ext:union_absorbed', n.absorbed) ELSE '{}'::jsonb END
                   || CASE WHEN n.union_rules IS NOT NULL THEN jsonb_build_object('ext:union_rule', n.union_rules) ELSE '{}'::jsonb END
                   || CASE WHEN n.confidence IS NOT NULL THEN jsonb_build_object('ext:union_confidence', round(n.confidence::numeric, 3))
                           ELSE '{}'::jsonb END) AS feature
    FROM u5_n n
    WHERE n.alive AND ((n.src <> 0 AND NOT EXISTS (SELECT 1 FROM u5_inref i WHERE i.nid = n.nid))
                       OR EXISTS (SELECT 1 FROM u5_ref f WHERE f.nid = n.nid));

    -- ext:* values must have one JSON type per key across a file: a key seen
    -- with mixed types is written as text everywhere
    FOREACH v_t IN ARRAY ARRAY['u5_out_nodes', 'u5_out_edges', 'u5_out_zones', 'u5_out_points', 'u5_out_lines', 'u5_out_polygons'] LOOP
        EXECUTE format($q$
            SELECT jsonb_object_agg(key, true) FROM (
                SELECT key FROM (SELECT key, jsonb_typeof(value) AS t FROM %I, jsonb_each(feature->'properties')
                                 WHERE key LIKE 'ext:%%' GROUP BY 1, 2) kt    -- a few groups: a hash, no sort
                GROUP BY key HAVING count(*) > 1) s $q$, v_t) INTO v_mixed;
        IF v_mixed IS NOT NULL THEN
            EXECUTE format($q$
                UPDATE %I SET feature = jsonb_set(feature, '{properties}', (
                    SELECT jsonb_object_agg(key, CASE WHEN $1 ? key AND jsonb_typeof(value) <> 'string'
                                                      THEN to_jsonb(value #>> '{}') ELSE value END)
                    FROM jsonb_each(feature->'properties')))
                WHERE EXISTS (SELECT 1 FROM jsonb_each(feature->'properties') WHERE $1 ? key) $q$, v_t) USING v_mixed;
        END IF;
    END LOOP;

    v_timings := v_timings || jsonb_build_object('output', round(extract(epoch FROM clock_timestamp() - v_ts)::numeric, 2));
    v_report := jsonb_build_object(
        'union_version', '5.0',
        'datasets', jsonb_build_object('ds1', v_ds1, 'ds2', v_ds2),
        'effective_config', v_cfg,
        'rules', (SELECT COALESCE(jsonb_agg(jsonb_build_object('stage', stage, 'rule', rule, 'metrics', m) ORDER BY so, seq), '[]'::jsonb)
                  FROM (SELECT stage, rule, min(seq) AS seq, jsonb_object_agg(k, v) AS m,
                               CASE stage WHEN 'snap' THEN 1 WHEN 'dedup' THEN 2 ELSE 3 END AS so
                        FROM u5_metrics GROUP BY stage, rule) x),
        'warnings', to_jsonb(v_warnings),
        'counts', jsonb_build_object(
            'input', v_in || jsonb_build_object(
                'zones', (SELECT count(*) FROM u5_zones), 'points', (SELECT count(*) FROM u5_pts),
                'lines', (SELECT count(*) FROM u5_lines), 'polygons', (SELECT count(*) FROM u5_polys)),
            'output', jsonb_build_object(
                'nodes', (SELECT count(*) FROM u5_out_nodes), 'edges', (SELECT count(*) FROM u5_out_edges),
                'zones', (SELECT count(*) FROM u5_out_zones), 'points', (SELECT count(*) FROM u5_out_points),
                'lines', (SELECT count(*) FROM u5_out_lines), 'polygons', (SELECT count(*) FROM u5_out_polygons)),
            'nodes_snapped', (SELECT count(*) FROM u5_snap),
            'edges_removed', (SELECT count(*) FROM u5_e WHERE removed_by IS NOT NULL AND NOT alive),
            'nodes_created', (SELECT count(*) FROM u5_n WHERE src = 0 AND alive),
            'connectors', (SELECT count(*) FROM u5_e WHERE alive AND is_connector)),
        -- seconds per stage (the cursors are read after this, by the caller)
        'timings_s', v_timings);
    RAISE NOTICE 'Phase 8: % nodes, % edges, % zones, % points, % lines, % polygons; % warning(s)',
        (SELECT count(*) FROM u5_out_nodes), (SELECT count(*) FROM u5_out_edges), (SELECT count(*) FROM u5_out_zones),
        (SELECT count(*) FROM u5_out_points), (SELECT count(*) FROM u5_out_lines), (SELECT count(*) FROM u5_out_polygons),
        cardinality(v_warnings);

    v_cur := 'node_cursor';
    OPEN v_cur FOR SELECT feature FROM u5_out_nodes ORDER BY id;
    file_name := 'node'; cursor_ref := v_cur; RETURN NEXT;
    v_cur := 'edge_cursor';
    OPEN v_cur FOR SELECT feature FROM u5_out_edges ORDER BY id;
    file_name := 'edge'; cursor_ref := v_cur; RETURN NEXT;
    v_cur := 'zone_cursor';
    OPEN v_cur FOR SELECT feature FROM u5_out_zones ORDER BY id;
    file_name := 'zone'; cursor_ref := v_cur; RETURN NEXT;
    v_cur := 'point_cursor';
    OPEN v_cur FOR SELECT feature FROM u5_out_points ORDER BY id;
    file_name := 'point'; cursor_ref := v_cur; RETURN NEXT;
    v_cur := 'line_cursor';
    OPEN v_cur FOR SELECT feature FROM u5_out_lines ORDER BY id;
    file_name := 'line'; cursor_ref := v_cur; RETURN NEXT;
    v_cur := 'polygon_cursor';
    OPEN v_cur FOR SELECT feature FROM u5_out_polygons ORDER BY id;
    file_name := 'polygon'; cursor_ref := v_cur; RETURN NEXT;
    v_cur := 'report_cursor';
    OPEN v_cur FOR SELECT v_report;
    file_name := 'report'; cursor_ref := v_cur; RETURN NEXT;
    RETURN;
END;
$BODY$;
