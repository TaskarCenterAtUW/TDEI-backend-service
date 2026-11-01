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
--              the 4.3.2 merging (no custom mode). In default mode each type
--              has its own block, every key optional:
--                points   {enabled, proximity}     same-type Points within proximity
--                lines    {enabled, proximity, min_overlap}
--                                                  a DS2 Line with min_overlap of its
--                                                  length within proximity of a DS1 Line
--                polygons {enabled, min_overlap}   covering more than min_overlap of a
--                                                  DS1 Polygon's area
--                zones    {enabled, min_overlap}   likewise for Zones
--              enabled defaults to true, proximity to 0.5, min_overlap to 0.7. A
--              type that is not enabled keeps both datasets' copies.
--
-- Stage order is fixed: snap → dedup → (snap topology) → connect → stranding
-- guard → zones and extensions → output and report.
-- Inside a stage the first rule listed that matches takes a location.
-- `default` modes run published, versioned rule sets (see
-- content.union5_validate_config); the run report returns the rules actually
-- used, per-rule counts and warnings.
-- =============================================================================

DROP FUNCTION IF EXISTS content.tdei_union_dataset(CHARACTER VARYING, CHARACTER VARYING, REAL, JSONB);
DROP FUNCTION IF EXISTS content.tdei_union_dataset(CHARACTER VARYING, CHARACTER VARYING, REAL);

-- The helpers are dropped first: CREATE OR REPLACE cannot rename a parameter.
DROP FUNCTION IF EXISTS content.union5_apply_splits(bigint);
DROP FUNCTION IF EXISTS content.union5_audit_add(jsonb, jsonb);
DROP FUNCTION IF EXISTS content.union5_validate_config(jsonb);
DROP FUNCTION IF EXISTS content.union5_cfg_ext(jsonb, text, boolean, boolean);
DROP FUNCTION IF EXISTS content.union5_cfg_tags(jsonb, text);
DROP FUNCTION IF EXISTS content.union5_cfg_name(jsonb, text, text);
DROP FUNCTION IF EXISTS content.union5_cfg_bool(jsonb, text, boolean);
DROP FUNCTION IF EXISTS content.union5_cfg_enum(jsonb, text, text[], text);
DROP FUNCTION IF EXISTS content.union5_cfg_num(jsonb, text, float8, boolean, float8, float8);
DROP FUNCTION IF EXISTS content.union5_cfg_keys(jsonb, text, text[]);
DROP FUNCTION IF EXISTS content.union5_tags_for(text, jsonb);
DROP FUNCTION IF EXISTS content.union5_osw_key_ok(text, text);
DROP FUNCTION IF EXISTS content.union5_tag_match(jsonb, jsonb, boolean);
DROP FUNCTION IF EXISTS content.union5_normalize_type(jsonb, text);
DROP FUNCTION IF EXISTS content.union5_builtin_type(text);
DROP FUNCTION IF EXISTS content.union5_dist_m(geometry, geometry, float8);

-- ─────────────────────────────────────────────────────────────────────────────
-- Helpers
-- ─────────────────────────────────────────────────────────────────────────────

-- Ground distance in metres between two points (SRID 4326 geometry). Inlined.
CREATE OR REPLACE FUNCTION content.union5_dist_m(point_a geometry, point_b geometry, m_per_deg_lon float8)
RETURNS float8 LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT sqrt(power((ST_X(point_b) - ST_X(point_a)) * m_per_deg_lon, 2) + power((ST_Y(point_b) - ST_Y(point_a)) * 111111.0, 2))
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
    clauses     jsonb := '[]'::jsonb;
    list_entry  jsonb;
    tag         record;
    tag_match   jsonb := '{}'::jsonb;
    builtin     jsonb;
BEGIN
    IF spec IS NULL OR jsonb_typeof(spec) = 'null' THEN
        RAISE EXCEPTION 'config.%: a type is required', path USING ERRCODE = '22023';
    ELSIF jsonb_typeof(spec) = 'string' THEN
        builtin := content.union5_builtin_type(spec #>> '{}');
        IF builtin IS NULL THEN
            RAISE EXCEPTION 'config.%: unknown type name "%" — use a tag match such as {"highway": "footway"} or one of crossing, sidewalk, pedestrian, living_street, road, other', path, spec #>> '{}'
                USING ERRCODE = '22023';
        END IF;
        RETURN builtin;
    ELSIF jsonb_typeof(spec) = 'object' THEN
        IF spec = '{}'::jsonb THEN
            RAISE EXCEPTION 'config.%: an empty tag match {} would match everything — name at least one key', path USING ERRCODE = '22023';
        END IF;
        FOR tag IN SELECT key, value FROM jsonb_each(spec) LOOP
            IF tag.key = '' THEN
                RAISE EXCEPTION 'config.%: empty key in a tag match', path USING ERRCODE = '22023';
            ELSIF jsonb_typeof(tag.value) = 'string' AND tag.value #>> '{}' = '*' THEN
                tag_match := tag_match || jsonb_build_object(tag.key, '*');
            ELSIF jsonb_typeof(tag.value) = 'string' THEN
                tag_match := tag_match || jsonb_build_object(tag.key, jsonb_build_array(tag.value));
            ELSIF jsonb_typeof(tag.value) = 'array' AND jsonb_array_length(tag.value) > 0
                  AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(tag.value) listed_value
                                  WHERE jsonb_typeof(listed_value) <> 'string') THEN
                tag_match := tag_match || jsonb_build_object(tag.key, tag.value);
            ELSE
                RAISE EXCEPTION 'config.%.%: a tag value must be a string, a non-empty list of strings, or "*"', path, tag.key
                    USING ERRCODE = '22023';
            END IF;
        END LOOP;
        RETURN jsonb_build_array(jsonb_build_object('all', tag_match));
    ELSIF jsonb_typeof(spec) = 'array' THEN
        IF jsonb_array_length(spec) = 0 THEN
            RAISE EXCEPTION 'config.%: an empty list of types matches nothing', path USING ERRCODE = '22023';
        END IF;
        FOR list_entry IN SELECT value FROM jsonb_array_elements(spec) LOOP
            IF jsonb_typeof(list_entry) NOT IN ('string', 'object') THEN
                RAISE EXCEPTION 'config.%: each list entry must be a type name or a tag match', path USING ERRCODE = '22023';
            END IF;
            clauses := clauses || content.union5_normalize_type(list_entry, path);
        END LOOP;
        RETURN clauses;
    END IF;
    RAISE EXCEPTION 'config.%: a type must be a type name, a tag match object, or a list of them', path USING ERRCODE = '22023';
END $$;

-- Does a property set match a normalised clause list? With positive_only, a
-- clause needs at least one 'all' condition (a node's own tags can only
-- identify it positively; 'other' is a pure negative and never matches a node
-- by its own tags).
CREATE OR REPLACE FUNCTION content.union5_tag_match(properties jsonb, clauses jsonb, positive_only boolean)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT EXISTS (
        SELECT 1 FROM jsonb_array_elements(clauses) clause
        WHERE (NOT positive_only OR COALESCE(clause->'all', '{}'::jsonb) <> '{}'::jsonb)
          AND NOT EXISTS (
                SELECT 1 FROM jsonb_each(COALESCE(clause->'all', '{}'::jsonb)) required_tag
                WHERE NOT CASE WHEN required_tag.value = '"*"'::jsonb THEN properties ? required_tag.key
                               ELSE COALESCE(properties->>required_tag.key IN (SELECT jsonb_array_elements_text(required_tag.value)), FALSE) END)
          AND NOT EXISTS (
                SELECT 1 FROM jsonb_each(COALESCE(clause->'none', '{}'::jsonb)) excluded_tag
                WHERE COALESCE(properties->>excluded_tag.key IN (SELECT jsonb_array_elements_text(excluded_tag.value)), FALSE)))
$$;

-- OSW keys written as given on created elements; any other key becomes ext:<key>.
-- Taken from the TDEI content schema's generated columns (node / edge).
CREATE OR REPLACE FUNCTION content.union5_osw_key_ok(kind text, tag_key text)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT tag_key LIKE 'ext:%' OR CASE kind
        WHEN 'node' THEN tag_key IN ('barrier', 'kerb', 'tactile_paving')
        WHEN 'edge' THEN tag_key IN ('highway', 'footway', 'name', 'description', 'surface', 'width', 'incline',
                                     'length', 'foot', 'service', 'tactile_paving', 'crossing:markings',
                                     'step_count', 'climb', 'building', 'opening_hours')
        ELSE FALSE END
$$;

-- Rule tags as written on a created/reused element of a kind.
CREATE OR REPLACE FUNCTION content.union5_tags_for(kind text, tags jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT COALESCE(jsonb_object_agg(CASE WHEN content.union5_osw_key_ok(kind, tag.key) THEN tag.key
                                          ELSE 'ext:' || tag.key END,
                                     CASE WHEN jsonb_typeof(tag.value) = 'string' THEN tag.value
                                          ELSE to_jsonb(tag.value #>> '{}') END ORDER BY tag.key), '{}'::jsonb)
    FROM jsonb_each(COALESCE(tags, '{}'::jsonb)) tag
$$;

-- ── config validation building blocks ───────────────────────────────────────
CREATE OR REPLACE FUNCTION content.union5_cfg_keys(setting_object jsonb, path text, allowed text[])
RETURNS void LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    setting_key text;
    config_location text := CASE WHEN path = '' THEN 'config' ELSE 'config.' || path END;
BEGIN
    IF setting_object IS NULL OR jsonb_typeof(setting_object) <> 'object' THEN
        RAISE EXCEPTION '%: must be a JSON object', config_location USING ERRCODE = '22023';
    END IF;
    FOR setting_key IN SELECT jsonb_object_keys(setting_object) LOOP
        IF NOT setting_key = ANY(allowed) THEN
            IF allowed = ARRAY['mode'] THEN
                RAISE EXCEPTION '%: mode off takes no other settings (got "%")', config_location, setting_key USING ERRCODE = '22023';
            END IF;
            RAISE EXCEPTION '%: unknown % "%" — expected one of %', config_location,
                CASE WHEN path = '' THEN 'section' ELSE 'setting' END, setting_key, array_to_string(allowed, ', ')
                USING ERRCODE = '22023';
        END IF;
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION content.union5_cfg_num(setting jsonb, path text, min_value float8, min_inclusive boolean,
                                                  max_value float8, default_value float8)
RETURNS float8 LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE parsed_number float8;
BEGIN
    IF setting IS NULL THEN RETURN default_value; END IF;
    IF jsonb_typeof(setting) <> 'number' THEN
        RAISE EXCEPTION 'config.%: must be a number (got %)', path, setting USING ERRCODE = '22023';
    END IF;
    parsed_number := (setting #>> '{}')::float8;
    IF (min_inclusive AND parsed_number < min_value) OR (NOT min_inclusive AND parsed_number <= min_value) OR parsed_number > max_value THEN
        RAISE EXCEPTION 'config.%: must be % % and at most % (got %)', path,
            CASE WHEN min_inclusive THEN 'at least' ELSE 'greater than' END, min_value, max_value, parsed_number USING ERRCODE = '22023';
    END IF;
    RETURN parsed_number;
END $$;

CREATE OR REPLACE FUNCTION content.union5_cfg_enum(setting jsonb, path text, allowed text[], default_value text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF setting IS NULL THEN
        IF default_value IS NULL THEN
            RAISE EXCEPTION 'config.%: is required — one of %', path, array_to_string(allowed, ', ') USING ERRCODE = '22023';
        END IF;
        RETURN default_value;
    END IF;
    IF jsonb_typeof(setting) <> 'string' OR NOT (setting #>> '{}') = ANY(allowed) THEN
        RAISE EXCEPTION 'config.%: expected one of %, got %', path, array_to_string(allowed, ', '), setting USING ERRCODE = '22023';
    END IF;
    RETURN setting #>> '{}';
END $$;

CREATE OR REPLACE FUNCTION content.union5_cfg_bool(setting jsonb, path text, default_value boolean)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF setting IS NULL THEN RETURN default_value; END IF;
    IF jsonb_typeof(setting) <> 'boolean' THEN
        RAISE EXCEPTION 'config.%: must be true or false (got %)', path, setting USING ERRCODE = '22023';
    END IF;
    RETURN (setting #>> '{}')::boolean;
END $$;

CREATE OR REPLACE FUNCTION content.union5_cfg_name(setting jsonb, path text, default_name text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF setting IS NULL THEN
        IF default_name IS NULL THEN RAISE EXCEPTION 'config.%: a rule name is required', path USING ERRCODE = '22023'; END IF;
        RETURN default_name;
    END IF;
    IF jsonb_typeof(setting) <> 'string' OR length(setting #>> '{}') = 0 OR length(setting #>> '{}') > 64
       OR (setting #>> '{}') ~ '[,]' THEN
        RAISE EXCEPTION 'config.%: a rule name must be 1-64 characters, without commas (got %)', path, setting USING ERRCODE = '22023';
    END IF;
    RETURN setting #>> '{}';
END $$;

CREATE OR REPLACE FUNCTION content.union5_cfg_tags(setting jsonb, path text)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE tag record;
BEGIN
    IF setting IS NULL THEN RETURN '{}'::jsonb; END IF;
    IF jsonb_typeof(setting) <> 'object' THEN
        RAISE EXCEPTION 'config.%: tags must be an object of key: value', path USING ERRCODE = '22023';
    END IF;
    FOR tag IN SELECT key, value FROM jsonb_each(setting) LOOP
        IF tag.key = '' OR tag.key IN ('_id', '_u_id', '_v_id', '_w_id') OR tag.key LIKE 'ext:union\_%' THEN
            RAISE EXCEPTION 'config.%.%: this key is reserved and cannot be set by a rule', path, tag.key USING ERRCODE = '22023';
        END IF;
        IF jsonb_typeof(tag.value) NOT IN ('string', 'number', 'boolean') THEN
            RAISE EXCEPTION 'config.%.%: a tag value must be a string, number or boolean', path, tag.key USING ERRCODE = '22023';
        END IF;
    END LOOP;
    RETURN setting;
END $$;

-- One extensions type block ({enabled, proximity?, min_overlap?}) with its defaults filled in.
CREATE OR REPLACE FUNCTION content.union5_cfg_ext(block jsonb, path text, has_proximity boolean, has_min_overlap boolean)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE effective_block jsonb;
BEGIN
    IF block IS NULL THEN block := '{}'::jsonb; END IF;
    IF jsonb_typeof(block) <> 'object' THEN
        RAISE EXCEPTION 'config.%: must be an object, e.g. {"enabled": true%}', path,
            CASE WHEN has_proximity THEN ', "proximity": 0.5' ELSE '' END || CASE WHEN has_min_overlap THEN ', "min_overlap": 0.7' ELSE '' END
            USING ERRCODE = '22023';
    END IF;
    PERFORM content.union5_cfg_keys(block, path, ARRAY['enabled']
        || CASE WHEN has_proximity THEN ARRAY['proximity'] ELSE ARRAY[]::text[] END
        || CASE WHEN has_min_overlap THEN ARRAY['min_overlap'] ELSE ARRAY[]::text[] END);
    effective_block := jsonb_build_object('enabled', content.union5_cfg_bool(block->'enabled', path || '.enabled', true));
    IF has_proximity THEN
        effective_block := effective_block || jsonb_build_object('proximity',
            content.union5_cfg_num(block->'proximity', path || '.proximity', 0, false, 5, 0.5));
    END IF;
    IF has_min_overlap THEN
        effective_block := effective_block || jsonb_build_object('min_overlap',
            content.union5_cfg_num(block->'min_overlap', path || '.min_overlap', 0, false, 1, 0.7));
    END IF;
    RETURN effective_block;
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
    input_config    jsonb := COALESCE(config, '{}'::jsonb);
    section         jsonb;              -- the section being checked (snap, dedup, …)
    section_mode    text;
    rule_cfg        jsonb;              -- one rule of a custom section
    rule_index      int;
    rule_path       text;               -- e.g. dedup.rules[2]
    rule_name       text;
    rule_names      text[];
    proximity       float8;             -- section-level defaults for its rules
    anchor_ds       text;
    attributes      text;
    min_overlap     float8;
    max_angle       float8;
    coincident      boolean;
    effective_rules jsonb;
    effective       jsonb := '{}'::jsonb;
    type_group      text;
    old_key         text;
    type_groups     CONSTANT text[] := ARRAY['crossing', 'pedestrian', 'living_street', 'road', 'other'];
    max_rules       CONSTANT int := 15;     -- every rule is one more pass over the data it matches
BEGIN
    PERFORM content.union5_cfg_keys(input_config, '', ARRAY['snap', 'dedup', 'connect', 'extensions']);

    -- ── snap ──
    section := input_config->'snap';
    IF section IS NULL THEN
        effective := effective || jsonb_build_object('snap', jsonb_build_object('mode', 'off', 'coincident', false, 'rules', '[]'::jsonb));
    ELSE
        IF jsonb_typeof(section) <> 'object' THEN RAISE EXCEPTION 'config.snap: must be a JSON object' USING ERRCODE = '22023'; END IF;
        section_mode := content.union5_cfg_enum(section->'mode', 'snap.mode', ARRAY['off', 'default', 'custom'], NULL);
        IF section_mode = 'off' THEN
            PERFORM content.union5_cfg_keys(section, 'snap', ARRAY['mode']);
            effective := effective || jsonb_build_object('snap', jsonb_build_object('mode', 'off', 'coincident', false, 'rules', '[]'::jsonb));
        ELSE
            PERFORM content.union5_cfg_keys(section, 'snap', CASE WHEN section_mode = 'default'
                THEN ARRAY['mode', 'proximity', 'anchor', 'attributes', 'coincident']
                ELSE ARRAY['mode', 'proximity', 'anchor', 'attributes', 'coincident', 'others', 'rules'] END);
            proximity  := content.union5_cfg_num(section->'proximity', 'snap.proximity', 0, true, 5, 0.5);
            anchor_ds  := content.union5_cfg_enum(section->'anchor', 'snap.anchor', ARRAY['ds1', 'ds2'], 'ds1');
            attributes := content.union5_cfg_enum(section->'attributes', 'snap.attributes', ARRAY['merge', 'anchor_only'], 'merge');
            coincident := content.union5_cfg_bool(section->'coincident', 'snap.coincident', section_mode = 'default');
            effective_rules := '[]'::jsonb;
            IF section_mode = 'default' THEN
                FOREACH type_group IN ARRAY type_groups LOOP
                    effective_rules := effective_rules || jsonb_build_array(jsonb_build_object(
                        'name', type_group, 'type', type_group, 'proximity', proximity, 'anchor', anchor_ds, 'attributes', attributes));
                END LOOP;
                effective := effective || jsonb_build_object('snap', jsonb_build_object(
                    'mode', section_mode, 'coincident', coincident, 'anchor', anchor_ds, 'attributes', attributes,
                    'rules', effective_rules, 'others', NULL));
            ELSE
                IF section ? 'rules' AND jsonb_typeof(section->'rules') <> 'array' THEN
                    RAISE EXCEPTION 'config.snap.rules: must be a list of rules' USING ERRCODE = '22023';
                END IF;
                IF COALESCE(jsonb_array_length(section->'rules'), 0) = 0 AND NOT coincident AND NOT section ? 'others' THEN
                    RAISE EXCEPTION 'config.snap.rules: custom mode needs at least one rule (or coincident / others)' USING ERRCODE = '22023';
                END IF;
                IF COALESCE(jsonb_array_length(section->'rules'), 0) > max_rules THEN
                    RAISE EXCEPTION 'config.snap.rules: at most % rules per stage (got %)', max_rules, jsonb_array_length(section->'rules') USING ERRCODE = '22023';
                END IF;
                rule_names := ARRAY[]::text[];
                FOR rule_index IN 0 .. COALESCE(jsonb_array_length(section->'rules'), 0) - 1 LOOP
                    rule_cfg := section->'rules'->rule_index; rule_path := format('snap.rules[%s]', rule_index + 1);
                    PERFORM content.union5_cfg_keys(rule_cfg, rule_path, ARRAY['name', 'type', 'proximity', 'anchor', 'attributes']);
                    rule_name := content.union5_cfg_name(rule_cfg->'name', rule_path || '.name', 'snap-' || (rule_index + 1));
                    IF rule_name = ANY(rule_names) THEN RAISE EXCEPTION 'config.%.name: "%" is used by an earlier snap rule', rule_path, rule_name USING ERRCODE = '22023'; END IF;
                    rule_names := rule_names || rule_name;
                    PERFORM content.union5_normalize_type(rule_cfg->'type', rule_path || '.type');
                    effective_rules := effective_rules || jsonb_build_array(jsonb_build_object(
                        'name', rule_name, 'type', rule_cfg->'type',
                        'proximity', content.union5_cfg_num(rule_cfg->'proximity', rule_path || '.proximity', 0, true, 5, proximity),
                        'anchor', content.union5_cfg_enum(rule_cfg->'anchor', rule_path || '.anchor', ARRAY['ds1', 'ds2'], anchor_ds),
                        'attributes', content.union5_cfg_enum(rule_cfg->'attributes', rule_path || '.attributes', ARRAY['merge', 'anchor_only'], attributes)));
                END LOOP;
                effective := effective || jsonb_build_object('snap', jsonb_build_object(
                    'mode', section_mode, 'coincident', coincident, 'anchor', anchor_ds, 'attributes', attributes, 'rules', effective_rules,
                    'others', CASE WHEN section ? 'others' THEN jsonb_build_object(
                        'proximity', content.union5_cfg_num(section->'others', 'snap.others', 0, true, 5, NULL),
                        'anchor', anchor_ds, 'attributes', attributes) END));
            END IF;
        END IF;
    END IF;

    -- ── dedup ──
    section := input_config->'dedup';
    IF section IS NULL THEN
        effective := effective || jsonb_build_object('dedup', jsonb_build_object('mode', 'off', 'rules', '[]'::jsonb));
    ELSE
        IF jsonb_typeof(section) <> 'object' THEN RAISE EXCEPTION 'config.dedup: must be a JSON object' USING ERRCODE = '22023'; END IF;
        section_mode := content.union5_cfg_enum(section->'mode', 'dedup.mode', ARRAY['off', 'default', 'custom'], NULL);
        IF section_mode = 'off' THEN
            PERFORM content.union5_cfg_keys(section, 'dedup', ARRAY['mode']);
            effective := effective || jsonb_build_object('dedup', jsonb_build_object('mode', 'off', 'rules', '[]'::jsonb));
        ELSE
            PERFORM content.union5_cfg_keys(section, 'dedup', CASE WHEN section_mode = 'default'
                THEN ARRAY['mode', 'proximity', 'min_overlap', 'max_angle', 'anchor']
                ELSE ARRAY['mode', 'proximity', 'min_overlap', 'max_angle', 'anchor', 'rules'] END);
            proximity   := content.union5_cfg_num(section->'proximity', 'dedup.proximity', 0, false, 5, 0.5);
            min_overlap := content.union5_cfg_num(section->'min_overlap', 'dedup.min_overlap', 0, false, 1, 0.8);
            -- max_angle: degrees between the edge and the anchor edge (above 0, at most 90)
            max_angle   := content.union5_cfg_num(section->'max_angle', 'dedup.max_angle', 0, false, 90, 30);
            anchor_ds   := content.union5_cfg_enum(section->'anchor', 'dedup.anchor', ARRAY['ds1', 'ds2'], 'ds1');
            effective_rules := '[]'::jsonb;
            IF section_mode = 'default' THEN
                FOREACH type_group IN ARRAY type_groups LOOP
                    effective_rules := effective_rules || jsonb_build_array(jsonb_build_object(
                        'name', type_group, 'from', type_group, 'to', type_group, 'proximity', proximity,
                        'min_overlap', min_overlap, 'max_angle', max_angle, 'anchor', anchor_ds));
                END LOOP;
            ELSE
                IF jsonb_typeof(section->'rules') IS DISTINCT FROM 'array' OR jsonb_array_length(section->'rules') = 0 THEN
                    RAISE EXCEPTION 'config.dedup.rules: custom mode needs a non-empty list of rules' USING ERRCODE = '22023';
                END IF;
                IF jsonb_array_length(section->'rules') > max_rules THEN
                    RAISE EXCEPTION 'config.dedup.rules: at most % rules per stage (got %)', max_rules, jsonb_array_length(section->'rules') USING ERRCODE = '22023';
                END IF;
                rule_names := ARRAY[]::text[];
                FOR rule_index IN 0 .. jsonb_array_length(section->'rules') - 1 LOOP
                    rule_cfg := section->'rules'->rule_index; rule_path := format('dedup.rules[%s]', rule_index + 1);
                    PERFORM content.union5_cfg_keys(rule_cfg, rule_path, ARRAY['name', 'from', 'to', 'proximity', 'min_overlap', 'max_angle', 'anchor']);
                    rule_name := content.union5_cfg_name(rule_cfg->'name', rule_path || '.name', 'dedup-' || (rule_index + 1));
                    IF rule_name = ANY(rule_names) THEN RAISE EXCEPTION 'config.%.name: "%" is used by an earlier dedup rule', rule_path, rule_name USING ERRCODE = '22023'; END IF;
                    rule_names := rule_names || rule_name;
                    PERFORM content.union5_normalize_type(rule_cfg->'from', rule_path || '.from');
                    PERFORM content.union5_normalize_type(rule_cfg->'to', rule_path || '.to');
                    effective_rules := effective_rules || jsonb_build_array(jsonb_build_object(
                        'name', rule_name, 'from', rule_cfg->'from', 'to', rule_cfg->'to',
                        'proximity', content.union5_cfg_num(rule_cfg->'proximity', rule_path || '.proximity', 0, false, 5, proximity),
                        'min_overlap', content.union5_cfg_num(rule_cfg->'min_overlap', rule_path || '.min_overlap', 0, false, 1, min_overlap),
                        'max_angle', content.union5_cfg_num(rule_cfg->'max_angle', rule_path || '.max_angle', 0, false, 90, max_angle),
                        'anchor', content.union5_cfg_enum(rule_cfg->'anchor', rule_path || '.anchor', ARRAY['ds1', 'ds2'], anchor_ds)));
                END LOOP;
            END IF;
            effective := effective || jsonb_build_object('dedup', jsonb_build_object('mode', section_mode, 'rules', effective_rules));
        END IF;
    END IF;

    -- ── connect ──
    section := input_config->'connect';
    IF section IS NULL THEN
        effective := effective || jsonb_build_object('connect', jsonb_build_object('mode', 'off', 'rules', '[]'::jsonb));
    ELSE
        IF jsonb_typeof(section) <> 'object' THEN RAISE EXCEPTION 'config.connect: must be a JSON object' USING ERRCODE = '22023'; END IF;
        section_mode := content.union5_cfg_enum(section->'mode', 'connect.mode', ARRAY['off', 'default', 'custom'], NULL);
        IF section_mode = 'off' THEN
            PERFORM content.union5_cfg_keys(section, 'connect', ARRAY['mode']);
            effective := effective || jsonb_build_object('connect', jsonb_build_object('mode', 'off', 'rules', '[]'::jsonb));
        ELSE
            PERFORM content.union5_cfg_keys(section, 'connect', CASE WHEN section_mode = 'default'
                THEN ARRAY['mode', 'proximity'] ELSE ARRAY['mode', 'proximity', 'rules'] END);
            proximity := content.union5_cfg_num(section->'proximity', 'connect.proximity', 0, false, 5, 0.5);
            IF section_mode = 'default' THEN
                effective_rules := jsonb_build_array(
                    jsonb_build_object('name', 'road-crossing',               'from', 'crossing',      'to', 'road',          'operation', 'point',    'proximity', proximity, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb),
                    jsonb_build_object('name', 'crossing-sidewalk',           'from', 'crossing',      'to', 'sidewalk',      'operation', 'geometry', 'proximity', proximity, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb),
                    jsonb_build_object('name', 'crossing-living-street',      'from', 'crossing',      'to', 'living_street', 'operation', 'geometry', 'proximity', proximity, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb),
                    jsonb_build_object('name', 'pedestrian-pedestrian',       'from', 'pedestrian',    'to', 'pedestrian',    'operation', 'geometry', 'proximity', proximity, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb),
                    jsonb_build_object('name', 'living-street-living-street', 'from', 'living_street', 'to', 'living_street', 'operation', 'geometry', 'proximity', proximity, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb),
                    jsonb_build_object('name', 'crossing-crossing',           'from', 'crossing',      'to', 'crossing',      'operation', 'geometry', 'proximity', proximity, 'node_tags', '{}'::jsonb, 'edge_tags', '{}'::jsonb));
            ELSE
                IF jsonb_typeof(section->'rules') IS DISTINCT FROM 'array' OR jsonb_array_length(section->'rules') = 0 THEN
                    RAISE EXCEPTION 'config.connect.rules: custom mode needs a non-empty list of rules' USING ERRCODE = '22023';
                END IF;
                IF jsonb_array_length(section->'rules') > max_rules THEN
                    RAISE EXCEPTION 'config.connect.rules: at most % rules per stage (got %)', max_rules, jsonb_array_length(section->'rules') USING ERRCODE = '22023';
                END IF;
                effective_rules := '[]'::jsonb; rule_names := ARRAY[]::text[];
                FOR rule_index IN 0 .. jsonb_array_length(section->'rules') - 1 LOOP
                    rule_cfg := section->'rules'->rule_index; rule_path := format('connect.rules[%s]', rule_index + 1);
                    IF rule_cfg ? 'tags' THEN
                        RAISE EXCEPTION 'config.%.tags: a connect rule takes node_tags (for the junction node) and edge_tags (for the connector edge) instead of tags', rule_path USING ERRCODE = '22023';
                    END IF;
                    PERFORM content.union5_cfg_keys(rule_cfg, rule_path, ARRAY['name', 'from', 'to', 'operation', 'proximity', 'node_tags', 'edge_tags']);
                    rule_name := content.union5_cfg_name(rule_cfg->'name', rule_path || '.name', NULL);
                    IF rule_name = ANY(rule_names) THEN RAISE EXCEPTION 'config.%.name: "%" is used by an earlier connect rule — names must be unique', rule_path, rule_name USING ERRCODE = '22023'; END IF;
                    rule_names := rule_names || rule_name;
                    PERFORM content.union5_normalize_type(rule_cfg->'from', rule_path || '.from');
                    PERFORM content.union5_normalize_type(rule_cfg->'to', rule_path || '.to');
                    effective_rules := effective_rules || jsonb_build_array(jsonb_build_object(
                        'name', rule_name, 'from', rule_cfg->'from', 'to', rule_cfg->'to',
                        'operation', content.union5_cfg_enum(rule_cfg->'operation', rule_path || '.operation', ARRAY['point', 'geometry'], NULL),
                        'proximity', content.union5_cfg_num(rule_cfg->'proximity', rule_path || '.proximity', 0, false, 5, proximity),
                        'node_tags', content.union5_cfg_tags(rule_cfg->'node_tags', rule_path || '.node_tags'),
                        'edge_tags', content.union5_cfg_tags(rule_cfg->'edge_tags', rule_path || '.edge_tags')));
                END LOOP;
            END IF;
            effective := effective || jsonb_build_object('connect', jsonb_build_object('mode', section_mode, 'rules', effective_rules));
        END IF;
    END IF;

    -- ── extensions ──
    section := input_config->'extensions';
    IF section IS NULL THEN
        effective := effective || jsonb_build_object('extensions', jsonb_build_object('mode', 'off'));
    ELSE
        IF jsonb_typeof(section) <> 'object' THEN RAISE EXCEPTION 'config.extensions: must be a JSON object' USING ERRCODE = '22023'; END IF;
        section_mode := content.union5_cfg_enum(section->'mode', 'extensions.mode', ARRAY['off', 'default'], NULL);
        IF section_mode = 'off' THEN
            PERFORM content.union5_cfg_keys(section, 'extensions', ARRAY['mode']);
            effective := effective || jsonb_build_object('extensions', jsonb_build_object('mode', 'off'));
        ELSE
            -- settings sit under each type; the earlier flat keys say where they went
            FOREACH old_key IN ARRAY ARRAY['proximity', 'line_proximity', 'line_overlap', 'polygon_overlap', 'zone_overlap'] LOOP
                IF section ? old_key THEN
                    RAISE EXCEPTION 'config.extensions.%: each type has its own block now — use %', old_key,
                        CASE old_key WHEN 'proximity' THEN 'extensions.points.proximity (and extensions.lines.proximity)'
                                     WHEN 'line_proximity' THEN 'extensions.lines.proximity'
                                     WHEN 'line_overlap' THEN 'extensions.lines.min_overlap'
                                     WHEN 'polygon_overlap' THEN 'extensions.polygons.min_overlap'
                                     ELSE 'extensions.zones.min_overlap' END USING ERRCODE = '22023';
                END IF;
            END LOOP;
            PERFORM content.union5_cfg_keys(section, 'extensions', ARRAY['mode', 'points', 'lines', 'polygons', 'zones']);
            effective_rules := jsonb_build_object(
                'points',   content.union5_cfg_ext(section->'points',   'extensions.points',   true,  false),
                'lines',    content.union5_cfg_ext(section->'lines',    'extensions.lines',    true,  true),
                'polygons', content.union5_cfg_ext(section->'polygons', 'extensions.polygons', false, true),
                'zones',    content.union5_cfg_ext(section->'zones',    'extensions.zones',    false, true));
            IF NOT EXISTS (SELECT 1 FROM jsonb_each(effective_rules) type_block WHERE (type_block.value->>'enabled')::boolean) THEN
                RAISE EXCEPTION 'config.extensions: points, lines, polygons and zones are all disabled — use "mode": "off" instead' USING ERRCODE = '22023';
            END IF;
            effective := effective || jsonb_build_object('extensions', jsonb_build_object('mode', 'default') || effective_rules);
        END IF;
    END IF;
    RETURN effective;
END $$;

-- Merge two audit objects: a key present in both keeps both values, comma-separated.
CREATE OR REPLACE FUNCTION content.union5_audit_add(current_audit jsonb, added_audit jsonb)
RETURNS jsonb LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT COALESCE(current_audit, '{}'::jsonb) || COALESCE((
        SELECT jsonb_object_agg(audit_key,
                                CASE WHEN current_audit ? audit_key AND (current_audit->>audit_key) <> (added_audit->>audit_key)
                                     THEN to_jsonb((current_audit->>audit_key) || ',' || (added_audit->>audit_key))
                                     ELSE added_audit->audit_key END)
        FROM jsonb_object_keys(COALESCE(added_audit, '{}'::jsonb)) audit_key), '{}'::jsonb)
$$;

-- ─────────────────────────────────────────────────────────────────────────────
-- content.union5_apply_splits(next_edge_id) — split edges at the requests queued
-- in the running union's temp table u5_split_request(edge_id, fraction, node_id).
-- Each piece runs between consecutive split fractions and its end vertices are
-- set EXACTLY to the node positions, so an edge end always equals the node it
-- references. Pieces keep the edge's tags, type and dataset (they gain nothing);
-- the split edge is retired. Returns the next free edge id.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION content.union5_apply_splits(next_edge_id bigint)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE new_piece_count bigint;
BEGIN
    DROP TABLE IF EXISTS u5_split_point;
    CREATE TEMP TABLE u5_split_point ON COMMIT DROP AS
    SELECT DISTINCT ON (request.edge_id, request.node_id) request.edge_id, request.fraction, request.node_id
    FROM u5_split_request request
    JOIN u5_edge edge ON edge.edge_id = request.edge_id AND edge.alive
    WHERE request.fraction > 1e-12 AND request.fraction < 1 - 1e-12
      AND request.node_id <> edge.start_node AND request.node_id <> edge.end_node
    ORDER BY request.edge_id, request.node_id, request.fraction;
    DELETE FROM u5_split_point later USING u5_split_point earlier          -- one node per fraction
    WHERE later.edge_id = earlier.edge_id AND later.fraction = earlier.fraction AND later.node_id > earlier.node_id;
    IF NOT EXISTS (SELECT 1 FROM u5_split_point) THEN
        TRUNCATE u5_split_request;
        RETURN next_edge_id;
    END IF;

    DROP TABLE IF EXISTS u5_split_piece;
    CREATE TEMP TABLE u5_split_piece ON COMMIT DROP AS
    SELECT cut.edge_id,
           cut.fraction AS start_fraction, cut.node_id AS start_node,
           LEAD(cut.fraction) OVER along_edge AS end_fraction, LEAD(cut.node_id) OVER along_edge AS end_node,
           ROW_NUMBER() OVER along_edge AS piece_no
    FROM (SELECT edge.edge_id, 0.0::float8 AS fraction, edge.start_node AS node_id
          FROM u5_edge edge WHERE edge.edge_id IN (SELECT edge_id FROM u5_split_point)
          UNION ALL
          SELECT edge.edge_id, 1.0::float8, edge.end_node
          FROM u5_edge edge WHERE edge.edge_id IN (SELECT edge_id FROM u5_split_point)
          UNION ALL
          SELECT edge_id, fraction, node_id FROM u5_split_point) cut
    WINDOW along_edge AS (PARTITION BY cut.edge_id ORDER BY cut.fraction);

    INSERT INTO u5_edge (edge_id, dataset, original_id, geom, props, start_node, end_node, alive, type_ids,
                         is_connector, made_by_rule, parent_edge, level_key)
    SELECT next_edge_id + ROW_NUMBER() OVER (ORDER BY piece.edge_id, piece.piece_no), piece.dataset, piece.original_id,
           piece.piece_geom, piece.props, piece.start_node, piece.end_node, true, piece.type_ids, piece.is_connector,
           piece.made_by_rule, piece.parent_edge, piece.level_key
    FROM (
        SELECT split_piece.edge_id, split_piece.piece_no, edge.dataset, edge.original_id, edge.props,
               split_piece.start_node, split_piece.end_node, edge.type_ids, edge.is_connector, edge.made_by_rule,
               COALESCE(edge.parent_edge, edge.edge_id) AS parent_edge, edge.level_key,
               ST_SetPoint(ST_SetPoint(cut_line.geom, 0, piece_start.geom), ST_NPoints(cut_line.geom) - 1, piece_end.geom) AS piece_geom
        FROM u5_split_piece split_piece
        JOIN u5_edge edge        ON edge.edge_id = split_piece.edge_id
        JOIN u5_node piece_start ON piece_start.node_id = split_piece.start_node
        JOIN u5_node piece_end   ON piece_end.node_id = split_piece.end_node
        CROSS JOIN LATERAL (SELECT ST_RemoveRepeatedPoints(ST_LineSubstring(edge.geom, split_piece.start_fraction,
                                                                            split_piece.end_fraction), 1e-10) AS geom) cut_line
        WHERE split_piece.end_fraction IS NOT NULL AND split_piece.end_fraction > split_piece.start_fraction
          AND split_piece.start_node <> split_piece.end_node
          AND GeometryType(cut_line.geom) = 'LINESTRING' AND ST_NPoints(cut_line.geom) >= 2) piece;
    GET DIAGNOSTICS new_piece_count = ROW_COUNT;

    UPDATE u5_edge SET alive = false WHERE edge_id IN (SELECT edge_id FROM u5_split_point);
    TRUNCATE u5_split_request;
    RETURN next_edge_id + new_piece_count + 1;
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
    v_ds1                   CONSTANT text := src_one_tdei_dataset_id;
    v_ds2                   CONSTANT text := src_two_tdei_dataset_id;
    v_config                jsonb;      -- the effective config (defaults filled in)
    v_rule_cfg              jsonb;      -- one rule of the effective config
    v_rule_index            int;
    snap_rule               record;     -- loop records over u5_snap_rules / u5_dedup_rules / u5_connect_rules
    dedup_rule              record;
    connect_rule            record;
    v_m_per_deg_lat         CONSTANT float8 := 111111.0;
    v_m_per_deg_lon         float8;
    v_on_line_tolerance_deg CONSTANT float8 := 1e-8;    -- "lies on": vertex identity for 7 dp data
    v_grid_deg              CONSTANT float8 := 1e-7;    -- created nodes are placed on the 7 dp grid
    v_next_edge_id          bigint;
    v_next_node_id          bigint;
    v_first_new_node_id     bigint;
    v_row_count             bigint;
    v_group_type_ids        int[];      -- the five type groups (snap 'others')
    v_type_tag_keys         text[];     -- every tag key a rule type mentions
    v_type_clauses          jsonb;
    v_type_id               int;
    v_from_type_id          int;
    v_to_type_id            int;
    v_junction_node_tags    jsonb;
    v_warnings              text[] := ARRAY[]::text[];
    v_report                jsonb;
    v_extensions            jsonb;      -- the extensions section of the effective config
    v_point_proximity_m     float8;
    v_point_proximity_deg   float8;
    v_line_proximity_deg    float8;
    v_out_cursor            refcursor;
    v_mixed_type_keys       jsonb;
    v_out_table             text;
    v_input_counts          jsonb;
    v_stage_start           timestamptz := clock_timestamp();   -- start of the current stage
    v_timings               jsonb := '{}'::jsonb;               -- seconds per stage, for the report
BEGIN
    IF v_ds1 = v_ds2 THEN
        RAISE EXCEPTION 'the two datasets must be different (both are %)', v_ds1 USING ERRCODE = '22023';
    END IF;
    v_config := content.union5_validate_config(config);       -- rejects a bad config before any data is read
    v_extensions := v_config->'extensions';
    RAISE NOTICE 'union 5.0: % + %', v_ds1, v_ds2;
    RAISE NOTICE 'effective config: %', v_config::text;

    -- JIT is off for this transaction: compiling each of the union's statements
    -- costs more than it saves (the time is spent inside PostGIS functions).
    -- Memory settings are left to the server's configuration.
    PERFORM set_config('jit', 'off', true);

    -- =========================================================================
    -- PHASE 1 — compile the rules (no data is read yet)
    --   Every type a rule names is normalised once into u5_type. Distances
    --   become degrees once the data's latitude is known (phase 2).
    -- =========================================================================
    RAISE NOTICE 'Phase 1: rules at %', clock_timestamp();
    DROP TABLE IF EXISTS u5_split_request;
    CREATE TEMP TABLE u5_split_request (edge_id bigint, fraction float8, node_id bigint) ON COMMIT DROP;
    DROP TABLE IF EXISTS u5_rule_metric;
    CREATE TEMP TABLE u5_rule_metric (stage text, rule_seq int, rule_name text, metric text, value bigint,
                                      PRIMARY KEY (stage, rule_name, metric)) ON COMMIT DROP;
    DROP TABLE IF EXISTS u5_type;
    CREATE TEMP TABLE u5_type (type_id serial PRIMARY KEY, clauses jsonb UNIQUE) ON COMMIT DROP;
    INSERT INTO u5_type (clauses)
    SELECT content.union5_builtin_type(type_group)
    FROM unnest(ARRAY['crossing', 'pedestrian', 'living_street', 'road', 'other']) type_group;
    v_group_type_ids := ARRAY(SELECT type_id FROM u5_type ORDER BY type_id);

    DROP TABLE IF EXISTS u5_snap_rules;
    CREATE TEMP TABLE u5_snap_rules (rule_seq int, rule_name text, rule_kind text, type_id int, proximity_m float8,
                                     proximity_deg float8, anchor_ds smallint, attributes text) ON COMMIT DROP;
    IF (v_config->'snap'->>'coincident')::boolean THEN
        INSERT INTO u5_snap_rules VALUES (0, 'coincident', 'coincident', NULL, 0, 0,
            CASE WHEN v_config->'snap'->>'anchor' = 'ds2' THEN 2 ELSE 1 END, COALESCE(v_config->'snap'->>'attributes', 'merge'));
    END IF;
    FOR v_rule_index IN 0 .. jsonb_array_length(v_config->'snap'->'rules') - 1 LOOP
        v_rule_cfg := v_config->'snap'->'rules'->v_rule_index;
        v_type_clauses := content.union5_normalize_type(v_rule_cfg->'type', 'snap');
        INSERT INTO u5_type (clauses) VALUES (v_type_clauses) ON CONFLICT (clauses) DO NOTHING;
        SELECT type_id INTO v_type_id FROM u5_type WHERE clauses = v_type_clauses;
        INSERT INTO u5_snap_rules VALUES (v_rule_index + 1, v_rule_cfg->>'name', 'rule', v_type_id,
            (v_rule_cfg->>'proximity')::float8, NULL,
            CASE WHEN v_rule_cfg->>'anchor' = 'ds2' THEN 2 ELSE 1 END, v_rule_cfg->>'attributes');
    END LOOP;
    IF jsonb_typeof(v_config->'snap'->'others') = 'object' THEN
        INSERT INTO u5_snap_rules VALUES (jsonb_array_length(v_config->'snap'->'rules') + 1, 'others', 'others', NULL,
            (v_config->'snap'->'others'->>'proximity')::float8, NULL,
            CASE WHEN v_config->'snap'->'others'->>'anchor' = 'ds2' THEN 2 ELSE 1 END, v_config->'snap'->'others'->>'attributes');
    END IF;

    DROP TABLE IF EXISTS u5_dedup_rules;
    CREATE TEMP TABLE u5_dedup_rules (rule_seq int, rule_name text, from_type_id int, to_type_id int, proximity_m float8,
                                      buffer_deg float8, min_overlap float8, anchor_ds smallint, cos_max_angle float8) ON COMMIT DROP;
    FOR v_rule_index IN 0 .. jsonb_array_length(v_config->'dedup'->'rules') - 1 LOOP
        v_rule_cfg := v_config->'dedup'->'rules'->v_rule_index;
        v_type_clauses := content.union5_normalize_type(v_rule_cfg->'from', 'dedup');
        INSERT INTO u5_type (clauses) VALUES (v_type_clauses) ON CONFLICT (clauses) DO NOTHING;
        SELECT type_id INTO v_from_type_id FROM u5_type WHERE clauses = v_type_clauses;
        v_type_clauses := content.union5_normalize_type(v_rule_cfg->'to', 'dedup');
        INSERT INTO u5_type (clauses) VALUES (v_type_clauses) ON CONFLICT (clauses) DO NOTHING;
        SELECT type_id INTO v_to_type_id FROM u5_type WHERE clauses = v_type_clauses;
        INSERT INTO u5_dedup_rules VALUES (v_rule_index + 1, v_rule_cfg->>'name', v_from_type_id, v_to_type_id,
            (v_rule_cfg->>'proximity')::float8, NULL, (v_rule_cfg->>'min_overlap')::float8,
            CASE WHEN v_rule_cfg->>'anchor' = 'ds2' THEN 2 ELSE 1 END,
            cos(radians(COALESCE((v_rule_cfg->>'max_angle')::float8, 30))));
    END LOOP;

    DROP TABLE IF EXISTS u5_connect_rules;
    CREATE TEMP TABLE u5_connect_rules (rule_seq int, rule_name text, from_type_id int, to_type_id int, operation text,
                                        proximity_m float8, proximity_deg float8, node_tags jsonb, edge_tags jsonb) ON COMMIT DROP;
    FOR v_rule_index IN 0 .. jsonb_array_length(v_config->'connect'->'rules') - 1 LOOP
        v_rule_cfg := v_config->'connect'->'rules'->v_rule_index;
        v_type_clauses := content.union5_normalize_type(v_rule_cfg->'from', 'connect');
        INSERT INTO u5_type (clauses) VALUES (v_type_clauses) ON CONFLICT (clauses) DO NOTHING;
        SELECT type_id INTO v_from_type_id FROM u5_type WHERE clauses = v_type_clauses;
        v_type_clauses := content.union5_normalize_type(v_rule_cfg->'to', 'connect');
        INSERT INTO u5_type (clauses) VALUES (v_type_clauses) ON CONFLICT (clauses) DO NOTHING;
        SELECT type_id INTO v_to_type_id FROM u5_type WHERE clauses = v_type_clauses;
        INSERT INTO u5_connect_rules VALUES (v_rule_index + 1, v_rule_cfg->>'name', v_from_type_id, v_to_type_id,
            v_rule_cfg->>'operation', (v_rule_cfg->>'proximity')::float8, NULL,
            COALESCE(v_rule_cfg->'node_tags', '{}'::jsonb), COALESCE(v_rule_cfg->'edge_tags', '{}'::jsonb));
    END LOOP;

    -- every rule appears in the report, even one that does nothing
    INSERT INTO u5_rule_metric
    SELECT 'snap', rule_row.rule_seq, rule_row.rule_name, metric_name, 0 FROM u5_snap_rules rule_row,
        unnest(ARRAY['snapped', 'edges_repointed', 'edges_split']) metric_name;
    INSERT INTO u5_rule_metric
    SELECT 'dedup', rule_row.rule_seq, rule_row.rule_name, metric_name, 0 FROM u5_dedup_rules rule_row,
        unnest(ARRAY['candidates', 'removed', 'restored']) metric_name;
    INSERT INTO u5_rule_metric
    SELECT 'connect', rule_row.rule_seq, rule_row.rule_name, metric_name, 0 FROM u5_connect_rules rule_row,
        unnest(ARRAY['candidates', 'junctions', 'nodes_created', 'nodes_reused', 'joined_earlier_node', 'connectors',
                     'edges_split', 'skipped_taken', 'skipped_nearer_end', 'skipped_angle', 'skipped_level',
                     'skipped_mutual']) metric_name;

    -- the tag keys any type mentions: a feature's signature is its values for these
    v_type_tag_keys := ARRAY(
        SELECT DISTINCT clause_key.tag_key
        FROM u5_type rule_type, jsonb_array_elements(rule_type.clauses) clause,
             LATERAL (SELECT jsonb_object_keys(COALESCE(clause->'all', '{}'::jsonb))
                      UNION SELECT jsonb_object_keys(COALESCE(clause->'none', '{}'::jsonb))) clause_key(tag_key));

    -- =========================================================================
    -- PHASE 2 — load nodes and edges of both datasets; match types
    --   Each edge and node gets the ids of the types it matches (type_ids),
    --   computed once per DISTINCT tag signature while loading — spatial steps
    --   never re-read tags. Indexes are built once the tables are complete.
    -- =========================================================================
    RAISE NOTICE 'Phase 2: load at %', clock_timestamp();
    -- Every working table is filled in one INSERT and indexed before anything
    -- updates it: an index built after a row was updated in the same transaction
    -- cannot be used until that transaction ends (PostgreSQL's broken-HOT-chain
    -- rule), and the union runs in one transaction.
    DROP TABLE IF EXISTS u5_node_load;
    CREATE TEMP TABLE u5_node_load ON COMMIT DROP AS
    SELECT raw.id AS node_id, (CASE WHEN raw.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END)::smallint AS dataset,
           raw.properties->>'_id' AS original_id, raw.node_loc AS geom, raw.properties - '_id' AS props,
           COALESCE(raw.properties->>'barrier' = 'kerb', false) AS is_kerb
    FROM (SELECT id, tdei_dataset_id, node_loc, (feature->'properties')::jsonb AS properties   -- parse the properties once
          FROM content.node WHERE tdei_dataset_id IN (v_ds1, v_ds2) OFFSET 0) raw;
    -- edge ends → nodes by EXACT coordinate in the same dataset (no tolerance in node
    -- identity): a hash join on (dataset, x, y)
    DROP TABLE IF EXISTS u5_node_by_coord;
    CREATE TEMP TABLE u5_node_by_coord ON COMMIT DROP AS
    SELECT dataset, ST_X(geom) AS x, ST_Y(geom) AS y, min(node_id) AS node_id FROM u5_node_load GROUP BY 1, 2, 3;
    DROP TABLE IF EXISTS u5_edge_load;
    CREATE TEMP TABLE u5_edge_load ON COMMIT DROP AS
    SELECT edge.edge_id, edge.dataset, edge.original_id, edge.geom, edge.props, edge.level_key,
           edge.start_x, edge.start_y, edge.end_x, edge.end_y,
           (SELECT COALESCE(jsonb_object_agg(tag_key, edge.props->tag_key), '{}'::jsonb)
            FROM unnest(v_type_tag_keys) tag_key WHERE edge.props ? tag_key) AS signature
    FROM (SELECT raw.id AS edge_id,
                 (CASE WHEN raw.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END)::smallint AS dataset,
                 raw.properties->>'_id' AS original_id,
                 raw.edge_loc AS geom,
                 raw.properties - '_id' - '_u_id' - '_v_id' AS props,
                 -- vertical position: lines on different layers / levels / bridge / tunnel never join (G1)
                 concat_ws('|', COALESCE(raw.properties->>'layer', '0'), COALESCE(raw.properties->>'level', ''),
                     CASE WHEN COALESCE(raw.properties->>'bridge', 'no') IN ('no', 'false', '0') THEN '' ELSE 'bridge' END,
                     CASE WHEN COALESCE(raw.properties->>'tunnel', 'no') IN ('no', 'false', '0') THEN '' ELSE 'tunnel' END) AS level_key,
                 ST_X(ST_StartPoint(raw.edge_loc)) AS start_x, ST_Y(ST_StartPoint(raw.edge_loc)) AS start_y,
                 ST_X(ST_EndPoint(raw.edge_loc)) AS end_x, ST_Y(ST_EndPoint(raw.edge_loc)) AS end_y
          FROM (SELECT id, tdei_dataset_id, edge_loc, (feature->'properties')::jsonb AS properties    -- parse the properties once
                FROM content.edge WHERE tdei_dataset_id IN (v_ds1, v_ds2) AND ST_NPoints(edge_loc) >= 2 OFFSET 0) raw
          OFFSET 0) edge;
    ANALYZE u5_node_by_coord;
    ANALYZE u5_edge_load;

    -- an edge end with no node at its coordinate gets one, so the output stays valid
    SELECT COALESCE(max(node_id), 0) + 1 INTO v_next_node_id FROM u5_node_load;
    WITH edge_end AS (
        SELECT edge.dataset, edge.start_x AS x, edge.start_y AS y FROM u5_edge_load edge
        UNION SELECT edge.dataset, edge.end_x, edge.end_y FROM u5_edge_load edge),
    missing_node AS (
        SELECT edge_end.dataset, edge_end.x, edge_end.y,
               v_next_node_id + ROW_NUMBER() OVER (ORDER BY edge_end.dataset, edge_end.x, edge_end.y) AS node_id
        FROM edge_end
        LEFT JOIN u5_node_by_coord known ON known.dataset = edge_end.dataset AND known.x = edge_end.x AND known.y = edge_end.y
        WHERE known.node_id IS NULL),
    added_node AS (
        INSERT INTO u5_node_load (node_id, dataset, original_id, geom, props, is_kerb)
        SELECT node_id, dataset, NULL, ST_SetSRID(ST_MakePoint(x, y), 4326), '{}'::jsonb, false FROM missing_node
        RETURNING node_id, dataset, geom)
    INSERT INTO u5_node_by_coord SELECT dataset, ST_X(geom), ST_Y(geom), node_id FROM added_node;
    GET DIAGNOSTICS v_row_count = ROW_COUNT;
    IF v_row_count > 0 THEN
        v_warnings := v_warnings || format('%s edge end(s) had no node at their coordinate; a node was added for each', v_row_count);
        ANALYZE u5_node_by_coord;
    END IF;

    -- edges, with their end nodes and the types of their signature
    DROP TABLE IF EXISTS u5_signature_type;
    CREATE TEMP TABLE u5_signature_type ON COMMIT DROP AS
    SELECT 'e'::text AS kind, sig.signature,
           ARRAY(SELECT rule_type.type_id FROM u5_type rule_type
                 WHERE content.union5_tag_match(sig.signature, rule_type.clauses, false) ORDER BY rule_type.type_id) AS type_ids
    FROM (SELECT DISTINCT signature FROM u5_edge_load) sig;
    DROP TABLE IF EXISTS u5_edge;
    CREATE TEMP TABLE u5_edge (
        edge_id           bigint PRIMARY KEY,
        dataset           smallint NOT NULL,       -- 1 = DS1, 2 = DS2, 0 = connector
        original_id       text,
        geom              geometry(LineString, 4326) NOT NULL,
        props             jsonb NOT NULL DEFAULT '{}'::jsonb,
        start_node        bigint,
        end_node          bigint,
        alive             boolean NOT NULL DEFAULT true,
        type_ids          int[] NOT NULL DEFAULT '{}',
        is_connector      boolean NOT NULL DEFAULT false,
        made_by_rule      text,                    -- the connect rule that made a connector
        removed_by        text,                    -- the dedup rule that removed this edge
        removed_anchor_ds smallint,
        absorbed_by       bigint,                  -- the anchor edge it duplicated (root id)
        parent_edge       bigint,                  -- the original edge of a split piece
        level_key         text NOT NULL DEFAULT ''
    ) WITH (fillfactor = 70) ON COMMIT DROP;
    INSERT INTO u5_edge (edge_id, dataset, original_id, geom, props, level_key, start_node, end_node, type_ids)
    SELECT edge.edge_id, edge.dataset, edge.original_id, edge.geom, edge.props, edge.level_key,
           start_key.node_id, end_key.node_id, COALESCE(sig.type_ids, '{}')
    FROM u5_edge_load edge
    LEFT JOIN u5_node_by_coord start_key ON start_key.dataset = edge.dataset AND start_key.x = edge.start_x AND start_key.y = edge.start_y
    LEFT JOIN u5_node_by_coord end_key   ON end_key.dataset = edge.dataset AND end_key.x = edge.end_x AND end_key.y = edge.end_y
    LEFT JOIN u5_signature_type sig ON sig.kind = 'e' AND sig.signature = edge.signature;
    DROP TABLE u5_edge_load;
    CREATE INDEX ON u5_edge (start_node);
    CREATE INDEX ON u5_edge (end_node);
    CREATE INDEX ON u5_edge (parent_edge);

    -- nodes: a node matches on its own identifying tags, and also takes the types of
    -- every edge it lies on: edges ending at it (by id) and edges passing through it
    -- (an interior vertex at its exact coordinate, in its own dataset)
    DROP TABLE IF EXISTS u5_node_signature;
    CREATE TEMP TABLE u5_node_signature ON COMMIT DROP AS
    SELECT node_id,
           (SELECT COALESCE(jsonb_object_agg(tag_key, props->tag_key), '{}'::jsonb)
            FROM unnest(v_type_tag_keys) tag_key WHERE props ? tag_key) AS signature
    FROM u5_node_load;
    INSERT INTO u5_signature_type
    SELECT 'n', sig.signature,
           ARRAY(SELECT rule_type.type_id FROM u5_type rule_type
                 WHERE content.union5_tag_match(sig.signature, rule_type.clauses, true) ORDER BY rule_type.type_id)
    FROM (SELECT DISTINCT signature FROM u5_node_signature) sig;
    DROP TABLE IF EXISTS u5_interior_vertex;
    CREATE TEMP TABLE u5_interior_vertex ON COMMIT DROP AS
    SELECT edge.edge_id, edge.dataset, ST_X(vertex.geom) AS x, ST_Y(vertex.geom) AS y
    FROM u5_edge edge, LATERAL ST_DumpPoints(edge.geom) vertex
    WHERE ST_NPoints(edge.geom) > 2 AND vertex.path[1] > 1 AND vertex.path[1] < ST_NPoints(edge.geom);
    ANALYZE u5_interior_vertex;
    DROP TABLE IF EXISTS u5_node_on_edge;               -- a node lying on an edge's interior vertex
    CREATE TEMP TABLE u5_node_on_edge ON COMMIT DROP AS
    SELECT DISTINCT vertex.edge_id, known.node_id
    FROM u5_interior_vertex vertex
    JOIN u5_node_by_coord known ON known.dataset = vertex.dataset AND known.x = vertex.x AND known.y = vertex.y;
    DROP TABLE u5_interior_vertex;
    CREATE INDEX ON u5_node_on_edge (node_id);
    CREATE INDEX ON u5_node_on_edge (edge_id);
    ANALYZE u5_node_on_edge;
    DROP TABLE IF EXISTS u5_node_type;
    CREATE TEMP TABLE u5_node_type ON COMMIT DROP AS
    SELECT node_id, array_agg(DISTINCT type_id ORDER BY type_id) AS type_ids FROM (
        SELECT node_sig.node_id, unnest(sig.type_ids) AS type_id
        FROM u5_node_signature node_sig JOIN u5_signature_type sig ON sig.kind = 'n' AND sig.signature = node_sig.signature
        WHERE sig.type_ids <> '{}'
        UNION ALL
        SELECT edge.start_node, unnest(edge.type_ids) FROM u5_edge edge WHERE edge.type_ids <> '{}'
        UNION ALL
        SELECT edge.end_node, unnest(edge.type_ids) FROM u5_edge edge WHERE edge.type_ids <> '{}'
        UNION ALL
        SELECT on_edge.node_id, unnest(edge.type_ids)
        FROM u5_node_on_edge on_edge JOIN u5_edge edge ON edge.edge_id = on_edge.edge_id WHERE edge.type_ids <> '{}') node_type_ids
    GROUP BY node_id;
    DROP TABLE u5_node_signature;
    ANALYZE u5_node_type;
    DROP TABLE IF EXISTS u5_node;
    CREATE TEMP TABLE u5_node (
        node_id      bigint PRIMARY KEY,      -- output _id (the database id for input nodes)
        dataset      smallint NOT NULL,       -- 1 = DS1, 2 = DS2, 0 = created by a connect rule
        original_id  text,                    -- original GeoJSON _id
        geom         geometry(Point, 4326) NOT NULL,
        props        jsonb NOT NULL DEFAULT '{}'::jsonb,
        is_kerb      boolean NOT NULL DEFAULT false,
        alive        boolean NOT NULL DEFAULT true,
        merged_into  bigint,                  -- set when this node joined another
        type_ids     int[] NOT NULL DEFAULT '{}',   -- ids of the rule types the node matches
        audit        jsonb NOT NULL DEFAULT '{}'::jsonb,
        absorbed     text,
        union_rules  text,
        confidence   float8
    ) WITH (fillfactor = 70) ON COMMIT DROP;
    INSERT INTO u5_node (node_id, dataset, original_id, geom, props, is_kerb, type_ids)
    SELECT loaded.node_id, loaded.dataset, loaded.original_id, loaded.geom, loaded.props, loaded.is_kerb,
           COALESCE(node_type.type_ids, '{}')
    FROM u5_node_load loaded LEFT JOIN u5_node_type node_type ON node_type.node_id = loaded.node_id;
    DROP TABLE u5_node_load;
    DROP TABLE u5_node_type;
    CREATE INDEX ON u5_node USING GIST (geom);
    CREATE INDEX ON u5_node (dataset, original_id);

    -- nodes an input edge uses (zone rings are added in phase 7); the rest are input orphans
    DROP TABLE IF EXISTS u5_input_node_ref;
    CREATE TEMP TABLE u5_input_node_ref ON COMMIT DROP AS
    SELECT start_node AS node_id FROM u5_edge UNION SELECT end_node FROM u5_edge;
    CREATE UNIQUE INDEX ON u5_input_node_ref (node_id);

    ANALYZE u5_node;                    -- temp tables are never auto-analysed; plans need the stats
    ANALYZE u5_edge;
    SELECT COALESCE(max(edge_id), 0) + 1 INTO v_next_edge_id FROM u5_edge;
    SELECT COALESCE(max(node_id), 0) + 1 INTO v_next_node_id FROM u5_node;
    v_m_per_deg_lon := v_m_per_deg_lat * COS(RADIANS(COALESCE((SELECT AVG(ST_Y(geom)) FROM u5_node), 0)));
    v_input_counts := jsonb_build_object('nodes', (SELECT count(*) FROM u5_node WHERE original_id IS NOT NULL),
                                         'edges', (SELECT count(*) FROM u5_edge));
    -- rule distances in degrees of longitude at the data's latitude (the larger
    -- span: an index search box that is never too small)
    UPDATE u5_snap_rules SET proximity_deg = proximity_m / v_m_per_deg_lon WHERE rule_kind <> 'coincident';
    UPDATE u5_dedup_rules SET buffer_deg = proximity_m / v_m_per_deg_lon;
    UPDATE u5_connect_rules SET proximity_deg = proximity_m / v_m_per_deg_lon;

    RAISE NOTICE 'Phase 2 complete: ds1 nodes=%, ds2 nodes=%, ds1 edges=%, ds2 edges=%; % types, % tag signatures',
        (SELECT count(*) FROM u5_node WHERE dataset = 1), (SELECT count(*) FROM u5_node WHERE dataset = 2),
        (SELECT count(*) FROM u5_edge WHERE dataset = 1), (SELECT count(*) FROM u5_edge WHERE dataset = 2),
        (SELECT count(*) FROM u5_type), (SELECT count(*) FROM u5_signature_type);
    v_timings := v_timings || jsonb_build_object('load', round(extract(epoch FROM clock_timestamp() - v_stage_start)::numeric, 2));
    v_stage_start := clock_timestamp();

    -- =========================================================================
    -- PHASE 3 — SNAP: a DS1 node and a DS2 node of the same type within the
    -- governing rule's proximity join; the non-anchor node moves onto the anchor.
    --   coincident first (any types), then the rules in order, then 'others'.
    --   A pair is governed by the FIRST rule whose type both nodes match.
    --   Nearest wins (tie: lowest original _id). Anchors never move, a node that
    --   moved is never a target, a kerb never moves (except coincident).
    -- =========================================================================
    DROP TABLE IF EXISTS u5_snap_pair;
    CREATE TEMP TABLE u5_snap_pair (mover_node bigint PRIMARY KEY, target_node bigint, rule_name text, rule_seq int,
                                    distance_m float8, attributes text, proximity_m float8) ON COMMIT DROP;
    CREATE INDEX ON u5_snap_pair (target_node);
    IF EXISTS (SELECT 1 FROM u5_snap_rules) THEN
        RAISE NOTICE 'Phase 3: snap at %', clock_timestamp();
        FOR snap_rule IN SELECT * FROM u5_snap_rules ORDER BY rule_seq LOOP
            INSERT INTO u5_snap_pair (mover_node, target_node, rule_name, rule_seq, distance_m, attributes, proximity_m)
            SELECT DISTINCT ON (mover.node_id) mover.node_id, anchor.node_id, snap_rule.rule_name, snap_rule.rule_seq,
                   content.union5_dist_m(mover.geom, anchor.geom, v_m_per_deg_lon), snap_rule.attributes, snap_rule.proximity_m
            FROM u5_node mover
            JOIN u5_node anchor ON anchor.dataset = snap_rule.anchor_ds AND anchor.alive
                               AND ST_DWithin(anchor.geom, mover.geom, GREATEST(snap_rule.proximity_deg, 1e-12))
            WHERE mover.dataset = 3 - snap_rule.anchor_ds AND mover.alive
              AND content.union5_dist_m(mover.geom, anchor.geom, v_m_per_deg_lon) <= snap_rule.proximity_m
              AND (snap_rule.rule_kind <> 'coincident' OR ST_Equals(mover.geom, anchor.geom))
              AND (snap_rule.rule_kind = 'coincident' OR NOT mover.is_kerb)
              AND (snap_rule.rule_kind <> 'rule' OR (
                        snap_rule.type_id = ANY(mover.type_ids) AND snap_rule.type_id = ANY(anchor.type_ids)
                    AND NOT EXISTS (SELECT 1 FROM u5_snap_rules earlier_rule
                                    WHERE earlier_rule.rule_kind = 'rule' AND earlier_rule.rule_seq < snap_rule.rule_seq
                                      AND earlier_rule.type_id = ANY(mover.type_ids) AND earlier_rule.type_id = ANY(anchor.type_ids))))
              AND (snap_rule.rule_kind <> 'others' OR (
                        NOT EXISTS (SELECT 1 FROM u5_snap_rules any_rule
                                    WHERE any_rule.rule_kind = 'rule'
                                      AND any_rule.type_id = ANY(mover.type_ids) AND any_rule.type_id = ANY(anchor.type_ids))
                    AND EXISTS (SELECT 1 FROM unnest(mover.type_ids) shared_type
                                WHERE shared_type = ANY(anchor.type_ids) AND shared_type = ANY(v_group_type_ids))))
              AND NOT EXISTS (SELECT 1 FROM u5_snap_pair pair WHERE pair.mover_node = mover.node_id)
              AND NOT EXISTS (SELECT 1 FROM u5_snap_pair pair WHERE pair.target_node = mover.node_id)
              AND NOT EXISTS (SELECT 1 FROM u5_snap_pair pair WHERE pair.mover_node = anchor.node_id)
            ORDER BY mover.node_id, content.union5_dist_m(mover.geom, anchor.geom, v_m_per_deg_lon), anchor.original_id, anchor.node_id;
        END LOOP;

        -- never collapse an edge: when both ends of an edge would land on one
        -- node, the farther end stays where it is
        LOOP
            DELETE FROM u5_snap_pair pair
            USING (SELECT CASE WHEN start_pair.distance_m >= end_pair.distance_m THEN start_pair.mover_node
                               ELSE end_pair.mover_node END AS dropped_mover
                   FROM u5_edge edge
                   JOIN u5_snap_pair start_pair ON start_pair.mover_node = edge.start_node
                   JOIN u5_snap_pair end_pair   ON end_pair.mover_node = edge.end_node
                   WHERE edge.alive AND edge.start_node <> edge.end_node
                     AND start_pair.target_node = end_pair.target_node) collapsing
            WHERE pair.mover_node = collapsing.dropped_mover;
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            EXIT WHEN v_row_count = 0;
        END LOOP;

        -- Attributes onto the anchor node. Every mover value that differs from the
        -- anchor's own (including keys the anchor lacks) is audited as
        -- ext:union_audit_<key> = "<original _id>-<value>". merge: anchor values
        -- win and keys only the mover has are added; anchor_only: nothing is added.
        DROP TABLE IF EXISTS u5_snap_mover_tag;
        CREATE TEMP TABLE u5_snap_mover_tag ON COMMIT DROP AS
        SELECT pair.target_node, pair.attributes, mover.original_id AS mover_original_id,
               tag.key AS tag_key, tag.value AS tag_value
        FROM u5_snap_pair pair JOIN u5_node mover ON mover.node_id = pair.mover_node, jsonb_each(mover.props) tag;
        UPDATE u5_node anchor_node SET audit = content.union5_audit_add(anchor_node.audit, changes.audit)
        FROM (SELECT target_node,
                     jsonb_object_agg('ext:union_audit_' || regexp_replace(tag_key, '^ext:', ''), to_jsonb(mover_values)
                                      ORDER BY tag_key) AS audit
              FROM (SELECT mover_tag.target_node, mover_tag.tag_key,
                           string_agg(COALESCE(mover_tag.mover_original_id, '?') || '-' || (mover_tag.tag_value #>> '{}'), ','
                                      ORDER BY mover_tag.mover_original_id) AS mover_values
                    FROM u5_snap_mover_tag mover_tag
                    JOIN u5_node current_anchor ON current_anchor.node_id = mover_tag.target_node
                    WHERE (current_anchor.props->mover_tag.tag_key) IS DISTINCT FROM mover_tag.tag_value
                    GROUP BY mover_tag.target_node, mover_tag.tag_key) per_key
              GROUP BY target_node) changes
        WHERE anchor_node.node_id = changes.target_node;
        UPDATE u5_node anchor_node SET props = anchor_node.props || added.tags
        FROM (SELECT target_node, jsonb_object_agg(tag_key, tag_value) AS tags FROM (
                  SELECT DISTINCT ON (mover_tag.target_node, mover_tag.tag_key) mover_tag.target_node, mover_tag.tag_key, mover_tag.tag_value
                  FROM u5_snap_mover_tag mover_tag
                  JOIN u5_node current_anchor ON current_anchor.node_id = mover_tag.target_node
                  WHERE mover_tag.attributes = 'merge' AND NOT (current_anchor.props ? mover_tag.tag_key)
                  ORDER BY mover_tag.target_node, mover_tag.tag_key, mover_tag.mover_original_id) new_key
              GROUP BY target_node) added
        WHERE anchor_node.node_id = added.target_node;
        UPDATE u5_node anchor_node SET
            absorbed = joined.absorbed_ids,
            confidence = joined.confidence
        FROM (SELECT pair.target_node,
                     string_agg(COALESCE(mover.original_id, mover.node_id::text), ',' ORDER BY mover.original_id) AS absorbed_ids,
                     max(CASE WHEN pair.distance_m = 0 OR pair.proximity_m = 0 THEN 1.0
                              ELSE GREATEST(0.0, 1.0 - pair.distance_m / pair.proximity_m) END) AS confidence
              FROM u5_snap_pair pair JOIN u5_node mover ON mover.node_id = pair.mover_node
              GROUP BY pair.target_node) joined
        WHERE anchor_node.node_id = joined.target_node;

        -- Movers retire. Their edges follow: an edge ending at a mover is
        -- re-pointed (its end set onto the anchor node) …
        UPDATE u5_node mover SET alive = false, merged_into = pair.target_node
        FROM u5_snap_pair pair WHERE pair.mover_node = mover.node_id;
        UPDATE u5_rule_metric rule_metric SET value = counted.edge_count
        FROM (SELECT pair.rule_name, count(*) AS edge_count
              FROM u5_snap_pair pair
              JOIN (SELECT start_node AS node_id FROM u5_edge WHERE alive
                    UNION ALL SELECT end_node FROM u5_edge WHERE alive) edge_end
                ON edge_end.node_id = pair.mover_node
              GROUP BY pair.rule_name) counted
        WHERE rule_metric.stage = 'snap' AND rule_metric.rule_name = counted.rule_name AND rule_metric.metric = 'edges_repointed';
        UPDATE u5_edge edge SET start_node = pair.target_node, geom = ST_SetPoint(edge.geom, 0, anchor_node.geom)
        FROM u5_snap_pair pair JOIN u5_node anchor_node ON anchor_node.node_id = pair.target_node
        WHERE edge.start_node = pair.mover_node;
        UPDATE u5_edge edge SET end_node = pair.target_node, geom = ST_SetPoint(edge.geom, ST_NPoints(edge.geom) - 1, anchor_node.geom)
        FROM u5_snap_pair pair JOIN u5_node anchor_node ON anchor_node.node_id = pair.target_node
        WHERE edge.end_node = pair.mover_node;
        -- … and an edge passing through a mover as an interior vertex is split there
        INSERT INTO u5_split_request (edge_id, fraction, node_id)
        SELECT edge.edge_id, ST_LineLocatePoint(edge.geom, mover.geom), pair.target_node
        FROM u5_snap_pair pair
        JOIN u5_node_on_edge on_edge ON on_edge.node_id = pair.mover_node
        JOIN u5_edge edge ON edge.edge_id = on_edge.edge_id AND edge.alive
        JOIN u5_node mover ON mover.node_id = pair.mover_node
        WHERE edge.start_node <> pair.target_node AND edge.end_node <> pair.target_node;
        UPDATE u5_rule_metric rule_metric SET value = counted.edge_count
        FROM (SELECT pair.rule_name, count(DISTINCT request.edge_id) AS edge_count
              FROM u5_split_request request JOIN u5_snap_pair pair ON pair.target_node = request.node_id
              GROUP BY pair.rule_name) counted
        WHERE rule_metric.stage = 'snap' AND rule_metric.rule_name = counted.rule_name AND rule_metric.metric = 'edges_split';
        v_next_edge_id := content.union5_apply_splits(v_next_edge_id);

        UPDATE u5_rule_metric rule_metric SET value = counted.pair_count
        FROM (SELECT rule_name, count(*) AS pair_count FROM u5_snap_pair GROUP BY rule_name) counted
        WHERE rule_metric.stage = 'snap' AND rule_metric.rule_name = counted.rule_name AND rule_metric.metric = 'snapped';
        RAISE NOTICE 'Phase 3 complete: % nodes joined onto % anchor nodes',
            (SELECT count(*) FROM u5_snap_pair), (SELECT count(DISTINCT target_node) FROM u5_snap_pair);
    END IF;
    v_timings := v_timings || jsonb_build_object('snap', round(extract(epoch FROM clock_timestamp() - v_stage_start)::numeric, 2));
    v_stage_start := clock_timestamp();

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
    CREATE TEMP TABLE u5_dedup_seen (edge_id bigint PRIMARY KEY) ON COMMIT DROP;
    FOR dedup_rule IN SELECT * FROM u5_dedup_rules ORDER BY rule_seq LOOP
        IF dedup_rule.rule_seq = 1 THEN RAISE NOTICE 'Phase 4: dedup at %', clock_timestamp(); END IF;
        DROP TABLE IF EXISTS u5_dedup_candidate;
        CREATE TEMP TABLE u5_dedup_candidate ON COMMIT DROP AS
        SELECT edge.edge_id, edge.geom, edge.start_node, edge.end_node FROM u5_edge edge
        WHERE edge.alive AND NOT edge.is_connector AND edge.dataset = 3 - dedup_rule.anchor_ds
          AND dedup_rule.from_type_id = ANY(edge.type_ids)
          AND NOT EXISTS (SELECT 1 FROM u5_dedup_seen seen WHERE seen.edge_id = edge.edge_id);
        INSERT INTO u5_dedup_seen SELECT edge_id FROM u5_dedup_candidate;
        UPDATE u5_rule_metric SET value = (SELECT count(*) FROM u5_dedup_candidate)
        WHERE stage = 'dedup' AND rule_name = dedup_rule.rule_name AND metric = 'candidates';
        CONTINUE WHEN NOT EXISTS (SELECT 1 FROM u5_dedup_candidate);
        CREATE INDEX ON u5_dedup_candidate USING GIST (geom);
        ANALYZE u5_dedup_candidate;

        -- the anchor edges of the 'to' type within reach of a candidate (only those
        -- can cover one, join the same nodes, or be walked by the connectivity guard),
        -- with their buffers
        DROP TABLE IF EXISTS u5_dedup_anchor;
        CREATE TEMP TABLE u5_dedup_anchor ON COMMIT DROP AS
        SELECT edge.edge_id, COALESCE(edge.parent_edge, edge.edge_id) AS root_edge, edge.geom, edge.start_node, edge.end_node,
               LEAST(edge.start_node, edge.end_node) AS low_node, GREATEST(edge.start_node, edge.end_node) AS high_node,
               ST_Buffer(edge.geom, dedup_rule.buffer_deg) AS buffer_geom
        FROM u5_edge edge
        WHERE edge.alive AND NOT edge.is_connector AND edge.dataset = dedup_rule.anchor_ds
          AND dedup_rule.to_type_id = ANY(edge.type_ids)
          AND EXISTS (SELECT 1 FROM u5_dedup_candidate candidate
                      WHERE ST_DWithin(candidate.geom, edge.geom, dedup_rule.buffer_deg));
        CREATE INDEX ON u5_dedup_anchor USING GIST (buffer_geom);
        CREATE INDEX ON u5_dedup_anchor USING GIST (geom);
        CREATE INDEX ON u5_dedup_anchor (low_node, high_node);
        ANALYZE u5_dedup_anchor;

        DROP TABLE IF EXISTS u5_dedup_decision;
        CREATE TEMP TABLE u5_dedup_decision ON COMMIT DROP AS
        SELECT facts.edge_id, facts.geom, facts.start_node, facts.end_node, facts.same_pair, facts.near_root_edge,
               facts.along, facts.start_on_anchor, facts.end_on_anchor,
               -- the share of the edge inside the anchor buffers. It decides only an edge that
               -- could be removed (same two nodes, or along an anchor edge), and only against
               -- min_overlap: the nearest anchor edge's buffer alone usually settles it (a copy
               -- of one edge). Only when another anchor buffer also touches the edge is the
               -- union of every buffer it touches built; with no other buffer it equals the first.
               CASE WHEN NOT (facts.same_pair OR facts.along) THEN NULL
                    ELSE (SELECT CASE WHEN nearest.nearest_share >= dedup_rule.min_overlap THEN nearest.nearest_share
                                      WHEN NOT EXISTS (SELECT 1 FROM u5_dedup_anchor other_anchor
                                                       WHERE ST_DWithin(facts.geom, other_anchor.buffer_geom, 0)
                                                         AND other_anchor.edge_id IS DISTINCT FROM facts.near_edge) THEN nearest.nearest_share
                                      ELSE COALESCE(ST_Length(ST_CollectionExtract(ST_Intersection(facts.geom,
                                               (SELECT ST_Union(touching.buffer_geom) FROM u5_dedup_anchor touching
                                                WHERE ST_DWithin(facts.geom, touching.buffer_geom, 0))), 2))
                                             / NULLIF(ST_Length(facts.geom), 0), 0.0) END
                          FROM (SELECT COALESCE(ST_Length(ST_CollectionExtract(ST_Intersection(facts.geom, facts.near_buffer), 2))
                                                / NULLIF(ST_Length(facts.geom), 0), 0.0) AS nearest_share) nearest)
               END AS covered
        FROM (
            SELECT candidate.edge_id, candidate.geom, candidate.start_node, candidate.end_node,
                   EXISTS (SELECT 1 FROM u5_dedup_anchor anchor
                           WHERE anchor.low_node = LEAST(candidate.start_node, candidate.end_node)
                             AND anchor.high_node = GREATEST(candidate.start_node, candidate.end_node)) AS same_pair,
                   nearest_anchor.root_edge AS near_root_edge, nearest_anchor.buffer_geom AS near_buffer,
                   nearest_anchor.edge_id AS near_edge,
                   COALESCE(nearest_anchor.along, true) AS along,
                   (EXISTS (SELECT 1 FROM u5_edge anchor_edge
                            WHERE anchor_edge.start_node = candidate.start_node AND anchor_edge.alive
                              AND NOT anchor_edge.is_connector AND anchor_edge.dataset = dedup_rule.anchor_ds)
                    OR EXISTS (SELECT 1 FROM u5_edge anchor_edge
                               WHERE anchor_edge.end_node = candidate.start_node AND anchor_edge.alive
                                 AND NOT anchor_edge.is_connector AND anchor_edge.dataset = dedup_rule.anchor_ds)) AS start_on_anchor,
                   (EXISTS (SELECT 1 FROM u5_edge anchor_edge
                            WHERE anchor_edge.start_node = candidate.end_node AND anchor_edge.alive
                              AND NOT anchor_edge.is_connector AND anchor_edge.dataset = dedup_rule.anchor_ds)
                    OR EXISTS (SELECT 1 FROM u5_edge anchor_edge
                               WHERE anchor_edge.end_node = candidate.end_node AND anchor_edge.alive
                                 AND NOT anchor_edge.is_connector AND anchor_edge.dataset = dedup_rule.anchor_ds)) AS end_on_anchor
            FROM u5_dedup_candidate candidate
            LEFT JOIN LATERAL (
                SELECT closest.edge_id, closest.root_edge, closest.buffer_geom,
                       -- angle guard: the edge runs within max_angle of the anchor edge at its midpoint
                       CASE WHEN content.union5_dist_m(ST_StartPoint(candidate.geom), ST_EndPoint(candidate.geom), v_m_per_deg_lon) < 0.1 THEN true
                            ELSE abs(  ((ST_X(tangent.point_after) - ST_X(tangent.point_before)) * v_m_per_deg_lon)
                                         * ((ST_X(ST_EndPoint(candidate.geom)) - ST_X(ST_StartPoint(candidate.geom))) * v_m_per_deg_lon)
                                     + ((ST_Y(tangent.point_after) - ST_Y(tangent.point_before)) * v_m_per_deg_lat)
                                         * ((ST_Y(ST_EndPoint(candidate.geom)) - ST_Y(ST_StartPoint(candidate.geom))) * v_m_per_deg_lat))
                                 >= dedup_rule.cos_max_angle * content.union5_dist_m(tangent.point_before, tangent.point_after, v_m_per_deg_lon)
                                    * content.union5_dist_m(ST_StartPoint(candidate.geom), ST_EndPoint(candidate.geom), v_m_per_deg_lon)
                       END AS along
                FROM (SELECT anchor.edge_id, anchor.root_edge, anchor.geom, anchor.buffer_geom,
                             ST_LineLocatePoint(anchor.geom, ST_LineInterpolatePoint(candidate.geom, 0.5)) AS fraction
                      FROM u5_dedup_anchor anchor
                      WHERE ST_DWithin(anchor.geom, candidate.geom, dedup_rule.buffer_deg)
                      ORDER BY anchor.geom <-> ST_LineInterpolatePoint(candidate.geom, 0.5), anchor.edge_id
                      LIMIT 1) closest,
                     LATERAL (SELECT ST_LineInterpolatePoint(closest.geom, GREATEST(closest.fraction - 1e-4, 0.0)) AS point_before,
                                     ST_LineInterpolatePoint(closest.geom, LEAST(closest.fraction + 1e-4, 1.0)) AS point_after) tangent
            ) nearest_anchor ON true
            OFFSET 0) facts;

        -- connectivity guard: an edge whose two ends are both on the anchor network is a
        -- duplicate only when the anchor network already joins those two ends through
        -- anchor edges lying inside its buffer (the same line, drawn as more pieces).
        -- The walk runs only for the edges it can decide (cheap tests first).
        ALTER TABLE u5_dedup_decision ADD COLUMN joined boolean NOT NULL DEFAULT false;
        UPDATE u5_dedup_decision decision SET joined = EXISTS (
              WITH RECURSIVE corridor AS (
                  -- the candidate's buffer is built once, not once per anchor edge near it
                  SELECT anchor.start_node, anchor.end_node
                  FROM (SELECT ST_Buffer(decision.geom, dedup_rule.buffer_deg) AS candidate_buffer OFFSET 0) own,
                       u5_dedup_anchor anchor
                  WHERE ST_DWithin(anchor.geom, decision.geom, dedup_rule.buffer_deg)
                    AND ST_Length(ST_CollectionExtract(ST_Intersection(anchor.geom, own.candidate_buffer), 2))
                        >= dedup_rule.min_overlap * ST_Length(anchor.geom)),
              walk(node_id) AS (
                  SELECT decision.start_node
                  UNION
                  SELECT CASE WHEN step.start_node = walk.node_id THEN step.end_node ELSE step.start_node END
                  FROM walk JOIN corridor step ON step.start_node = walk.node_id OR step.end_node = walk.node_id)
              SELECT 1 FROM walk WHERE walk.node_id = decision.end_node)
        WHERE decision.start_on_anchor AND decision.end_on_anchor AND NOT decision.same_pair AND decision.along
          AND decision.covered >= dedup_rule.min_overlap;
        UPDATE u5_edge edge SET alive = false, removed_by = dedup_rule.rule_name, removed_anchor_ds = dedup_rule.anchor_ds,
                                absorbed_by = decision.near_root_edge
        FROM u5_dedup_decision decision
        WHERE decision.edge_id = edge.edge_id AND decision.covered >= dedup_rule.min_overlap
          AND (decision.same_pair OR (decision.along AND (NOT (decision.start_on_anchor AND decision.end_on_anchor) OR decision.joined)));
        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        UPDATE u5_rule_metric SET value = v_row_count
        WHERE stage = 'dedup' AND rule_name = dedup_rule.rule_name AND metric = 'removed';
    END LOOP;
    v_timings := v_timings || jsonb_build_object('dedup', round(extract(epoch FROM clock_timestamp() - v_stage_start)::numeric, 2));
    v_stage_start := clock_timestamp();

    -- Snap topology: an anchor edge passing through a node that a moved edge now
    -- ENDS at is split there, so the join is a real connection. Done after
    -- dedup, so a duplicate that was removed never splits the anchor.
    DROP TABLE IF EXISTS u5_snap_target_vertex;
    CREATE TEMP TABLE u5_snap_target_vertex ON COMMIT DROP AS
    SELECT DISTINCT target.node_id, target.dataset, target.geom
    FROM (SELECT DISTINCT target_node FROM u5_snap_pair) pair
    JOIN u5_node target ON target.node_id = pair.target_node AND target.alive
    JOIN u5_node_on_edge on_edge ON on_edge.node_id = target.node_id
    WHERE EXISTS (SELECT 1 FROM u5_edge moved WHERE moved.start_node = target.node_id AND moved.alive AND moved.dataset <> target.dataset)
       OR EXISTS (SELECT 1 FROM u5_edge moved WHERE moved.end_node = target.node_id AND moved.alive AND moved.dataset <> target.dataset);
    -- the edge itself, or a piece of it after a split (two index joins, not one OR join)
    INSERT INTO u5_split_request (edge_id, fraction, node_id)
    SELECT edge.edge_id, ST_LineLocatePoint(edge.geom, target_vertex.geom), target_vertex.node_id
    FROM u5_snap_target_vertex target_vertex
    JOIN u5_node_on_edge on_edge ON on_edge.node_id = target_vertex.node_id
    JOIN u5_edge edge ON edge.edge_id = on_edge.edge_id AND edge.alive AND NOT edge.is_connector
    WHERE edge.start_node <> target_vertex.node_id AND edge.end_node <> target_vertex.node_id
      AND ST_DWithin(edge.geom, target_vertex.geom, v_on_line_tolerance_deg)
    UNION ALL
    SELECT edge.edge_id, ST_LineLocatePoint(edge.geom, target_vertex.geom), target_vertex.node_id
    FROM u5_snap_target_vertex target_vertex
    JOIN u5_node_on_edge on_edge ON on_edge.node_id = target_vertex.node_id
    JOIN u5_edge edge ON edge.parent_edge = on_edge.edge_id AND edge.alive AND NOT edge.is_connector
    WHERE edge.start_node <> target_vertex.node_id AND edge.end_node <> target_vertex.node_id
      AND ST_DWithin(edge.geom, target_vertex.geom, v_on_line_tolerance_deg);
    UPDATE u5_rule_metric rule_metric SET value = rule_metric.value + counted.edge_count
    FROM (SELECT split_by_rule.rule_name, count(DISTINCT split_by_rule.edge_id) AS edge_count FROM (
              SELECT DISTINCT ON (request.edge_id, request.node_id) request.edge_id, pair.rule_name
              FROM u5_split_request request JOIN u5_snap_pair pair ON pair.target_node = request.node_id
              ORDER BY request.edge_id, request.node_id, pair.rule_seq) split_by_rule
          GROUP BY split_by_rule.rule_name) counted
    WHERE rule_metric.stage = 'snap' AND rule_metric.rule_name = counted.rule_name AND rule_metric.metric = 'edges_split';
    v_next_edge_id := content.union5_apply_splits(v_next_edge_id);
    ANALYZE u5_edge;
    v_timings := v_timings || jsonb_build_object('snap_topology', round(extract(epoch FROM clock_timestamp() - v_stage_start)::numeric, 2));
    v_stage_start := clock_timestamp();

    -- =========================================================================
    -- PHASE 5 — CONNECT: junctions between two kinds of way, rule by rule.
    --   A = edges matching 'from' (u5_connect_from_line), B = edges matching
    --   'to' (u5_connect_to_line); always one from DS1 and one from DS2. Nothing
    --   moves: a node is made on B (B is split there) and, across a gap, a
    --   connector edge joins A's end to it.
    --   O1 an A end an earlier rule used is taken; O2 a junction landing within
    --   reach of an earlier rule's node on B uses that node; O3 created nodes
    --   and connectors are never A or B.
    -- =========================================================================
    DROP TABLE IF EXISTS u5_connect_taken;
    CREATE TEMP TABLE u5_connect_taken (node_id bigint PRIMARY KEY) ON COMMIT DROP;           -- A ends already used
    DROP TABLE IF EXISTS u5_connect_junction_node;
    CREATE TEMP TABLE u5_connect_junction_node (node_id bigint PRIMARY KEY) ON COMMIT DROP;   -- nodes rules made or used
    FOR connect_rule IN SELECT * FROM u5_connect_rules ORDER BY rule_seq LOOP
        IF connect_rule.rule_seq = 1 THEN RAISE NOTICE 'Phase 5: connect at %', clock_timestamp(); END IF;

        DROP TABLE IF EXISTS u5_connect_to_line;
        CREATE TEMP TABLE u5_connect_to_line ON COMMIT DROP AS
        SELECT edge_id, dataset, original_id, geom, start_node, end_node, level_key FROM u5_edge
        WHERE alive AND NOT is_connector AND dataset IN (1, 2) AND connect_rule.to_type_id = ANY(type_ids);
        CREATE INDEX ON u5_connect_to_line USING GIST (geom) WHERE dataset = 1;   -- one index per dataset: a probe
        CREATE INDEX ON u5_connect_to_line USING GIST (geom) WHERE dataset = 2;   -- only sees the other dataset's lines
        CREATE INDEX ON u5_connect_to_line (start_node);
        CREATE INDEX ON u5_connect_to_line (end_node);
        DROP TABLE IF EXISTS u5_connect_from_line;
        CREATE TEMP TABLE u5_connect_from_line ON COMMIT DROP AS
        SELECT edge_id, dataset, original_id, geom, start_node, end_node, level_key, props FROM u5_edge
        WHERE alive AND NOT is_connector AND dataset IN (1, 2) AND connect_rule.from_type_id = ANY(type_ids);
        CREATE INDEX ON u5_connect_from_line USING GIST (geom);
        CREATE INDEX ON u5_connect_from_line (start_node);
        CREATE INDEX ON u5_connect_from_line (end_node);
        ANALYZE u5_connect_to_line;
        ANALYZE u5_connect_from_line;

        -- ── an END of A within reach of B ──
        DROP TABLE IF EXISTS u5_connect_candidate;
        CREATE TEMP TABLE u5_connect_candidate ON COMMIT DROP AS
        SELECT from_line.edge_id AS from_edge, from_line.dataset AS from_dataset, from_line.original_id AS from_original_id,
               from_line.level_key AS from_level_key,
               line_end.end_node AS from_end_node, line_end.end_point AS from_end_point,
               line_end.next_vertex AS from_next_vertex, line_end.at_start,
               to_line.edge_id AS to_edge, to_line.dataset AS to_dataset, to_line.original_id AS to_original_id,
               to_line.start_node AS to_start_node, to_line.end_node AS to_end_node, to_line.level_key AS to_level_key,
               to_line.geom AS to_geom, ST_ClosestPoint(to_line.geom, line_end.end_point) AS junction_point,
               NULL::float8 AS gap_m, NULL::geometry AS crossing_point, false AS split_from_line
        FROM u5_connect_from_line from_line
        CROSS JOIN LATERAL (VALUES (from_line.start_node, ST_StartPoint(from_line.geom), ST_PointN(from_line.geom, 2), true),
                                   (from_line.end_node, ST_EndPoint(from_line.geom),
                                    ST_PointN(from_line.geom, ST_NPoints(from_line.geom) - 1), false))
                   line_end(end_node, end_point, next_vertex, at_start)
        JOIN u5_connect_to_line to_line ON to_line.dataset = 2 AND ST_DWithin(to_line.geom, line_end.end_point, connect_rule.proximity_deg)
        WHERE from_line.dataset = 1 AND to_line.start_node <> line_end.end_node AND to_line.end_node <> line_end.end_node
          AND EXISTS (SELECT 1 FROM u5_connect_to_line WHERE dataset = 2)
        UNION ALL
        SELECT from_line.edge_id, from_line.dataset, from_line.original_id, from_line.level_key,
               line_end.end_node, line_end.end_point, line_end.next_vertex, line_end.at_start,
               to_line.edge_id, to_line.dataset, to_line.original_id, to_line.start_node, to_line.end_node, to_line.level_key,
               to_line.geom, ST_ClosestPoint(to_line.geom, line_end.end_point), NULL::float8, NULL::geometry, false
        FROM u5_connect_from_line from_line
        CROSS JOIN LATERAL (VALUES (from_line.start_node, ST_StartPoint(from_line.geom), ST_PointN(from_line.geom, 2), true),
                                   (from_line.end_node, ST_EndPoint(from_line.geom),
                                    ST_PointN(from_line.geom, ST_NPoints(from_line.geom) - 1), false))
                   line_end(end_node, end_point, next_vertex, at_start)
        JOIN u5_connect_to_line to_line ON to_line.dataset = 1 AND ST_DWithin(to_line.geom, line_end.end_point, connect_rule.proximity_deg)
        WHERE from_line.dataset = 2 AND to_line.start_node <> line_end.end_node AND to_line.end_node <> line_end.end_node
          AND EXISTS (SELECT 1 FROM u5_connect_to_line WHERE dataset = 1);
        DELETE FROM u5_connect_candidate candidate USING u5_node from_end            -- O3: a created node is never an A end
        WHERE from_end.node_id = candidate.from_end_node AND from_end.dataset = 0;
        -- an end already joined to a B line of the other dataset is a junction already
        DELETE FROM u5_connect_candidate candidate
        WHERE EXISTS (SELECT 1 FROM u5_connect_to_line joined_line
                      WHERE joined_line.start_node = candidate.from_end_node AND joined_line.dataset <> candidate.from_dataset)
           OR EXISTS (SELECT 1 FROM u5_connect_to_line joined_line
                      WHERE joined_line.end_node = candidate.from_end_node AND joined_line.dataset <> candidate.from_dataset);
        UPDATE u5_connect_candidate SET gap_m = content.union5_dist_m(from_end_point, junction_point, v_m_per_deg_lon);
        DELETE FROM u5_connect_candidate WHERE gap_m > connect_rule.proximity_m;
        -- A crossing B near this end (an overshoot): the crossing nearest the end
        UPDATE u5_connect_candidate candidate SET crossing_point = (
            SELECT crossing.geom FROM ST_Dump(ST_Intersection(from_line.geom, candidate.to_geom)) crossing
            WHERE GeometryType(crossing.geom) = 'POINT' AND NOT ST_Equals(crossing.geom, candidate.from_end_point)
            ORDER BY abs(ST_LineLocatePoint(from_line.geom, crossing.geom) - CASE WHEN candidate.at_start THEN 0.0 ELSE 1.0 END),
                     ST_X(crossing.geom), ST_Y(crossing.geom) LIMIT 1)
        FROM u5_connect_from_line from_line
        WHERE from_line.edge_id = candidate.from_edge AND ST_Intersects(from_line.geom, candidate.to_geom);
        IF connect_rule.operation = 'point' THEN
            DELETE FROM u5_connect_candidate WHERE crossing_point IS NOT NULL;   -- the crossing itself makes the junction (below)
        ELSE
            -- geometry: an end that overshoots B meets it where it crosses (A and B split there)
            UPDATE u5_connect_candidate
            SET junction_point = crossing_point, gap_m = content.union5_dist_m(from_end_point, crossing_point, v_m_per_deg_lon),
                split_from_line = true
            WHERE crossing_point IS NOT NULL;
        END IF;
        UPDATE u5_rule_metric SET value = (SELECT count(DISTINCT from_end_node) FROM u5_connect_candidate)
        WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'candidates';

        WITH skipped AS (DELETE FROM u5_connect_candidate WHERE from_level_key <> to_level_key RETURNING from_end_node)
        SELECT count(DISTINCT from_end_node) INTO v_row_count FROM skipped;
        UPDATE u5_rule_metric SET value = v_row_count
        WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'skipped_level';
        WITH skipped AS (DELETE FROM u5_connect_candidate candidate
                         WHERE EXISTS (SELECT 1 FROM u5_connect_taken taken WHERE taken.node_id = candidate.from_end_node)
                         RETURNING from_end_node)
        SELECT count(DISTINCT from_end_node) INTO v_row_count FROM skipped;
        UPDATE u5_rule_metric SET value = v_row_count
        WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'skipped_taken';
        -- W4 NEARER END ONLY: an end does not join B when the far end of an A edge
        -- at that node is nearer to B (that end makes the junction)
        WITH skipped AS (
            DELETE FROM u5_connect_candidate candidate
            WHERE NOT candidate.split_from_line AND EXISTS (
                SELECT 1 FROM (SELECT ST_EndPoint(other_from.geom) AS far_point FROM u5_connect_from_line other_from
                               WHERE other_from.start_node = candidate.from_end_node AND other_from.dataset = candidate.from_dataset
                               UNION ALL
                               SELECT ST_StartPoint(other_from.geom) FROM u5_connect_from_line other_from
                               WHERE other_from.end_node = candidate.from_end_node AND other_from.dataset = candidate.from_dataset) far_end
                WHERE content.union5_dist_m(far_end.far_point, ST_ClosestPoint(candidate.to_geom, far_end.far_point), v_m_per_deg_lon)
                      < candidate.gap_m - 1e-9)
            RETURNING from_end_node)
        SELECT count(DISTINCT from_end_node) INTO v_row_count FROM skipped;
        UPDATE u5_rule_metric SET value = v_row_count
        WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'skipped_nearer_end';
        -- W5 ANGLE GUARD: a T onto B's span needs 30° or more; a gap to B's end node does not
        WITH skipped AS (
            DELETE FROM u5_connect_candidate candidate
            WHERE NOT candidate.split_from_line AND candidate.gap_m > 0
              AND NOT ST_Equals(candidate.junction_point, ST_StartPoint(candidate.to_geom))
              AND NOT ST_Equals(candidate.junction_point, ST_EndPoint(candidate.to_geom))
              AND (SELECT abs(  ((ST_X(candidate.from_end_point) - ST_X(candidate.from_next_vertex)) * v_m_per_deg_lon)
                                  * ((ST_X(tangent.point_after) - ST_X(tangent.point_before)) * v_m_per_deg_lon)
                              + ((ST_Y(candidate.from_end_point) - ST_Y(candidate.from_next_vertex)) * v_m_per_deg_lat)
                                  * ((ST_Y(tangent.point_after) - ST_Y(tangent.point_before)) * v_m_per_deg_lat))
                          / NULLIF(content.union5_dist_m(candidate.from_next_vertex, candidate.from_end_point, v_m_per_deg_lon)
                                   * content.union5_dist_m(tangent.point_before, tangent.point_after, v_m_per_deg_lon), 0)
                   FROM (SELECT ST_LineInterpolatePoint(candidate.to_geom,
                                    GREATEST(ST_LineLocatePoint(candidate.to_geom, candidate.junction_point) - 1e-4, 0.0)) AS point_before,
                                ST_LineInterpolatePoint(candidate.to_geom,
                                    LEAST(ST_LineLocatePoint(candidate.to_geom, candidate.junction_point) + 1e-4, 1.0)) AS point_after) tangent)
                  > 0.866025
            RETURNING from_end_node)
        SELECT count(DISTINCT from_end_node) INTO v_row_count FROM skipped;
        UPDATE u5_rule_metric SET value = v_row_count
        WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'skipped_angle';

        -- W2: one junction per A end, to the nearest B (tie: lowest original _id); when
        -- several A edges end at that node, the connector takes the tags of the one
        -- with the lowest original _id
        DROP TABLE IF EXISTS u5_connect_junction;
        CREATE TEMP TABLE u5_connect_junction ON COMMIT DROP AS
        SELECT DISTINCT ON (from_end_node) from_edge, from_dataset, from_end_node, from_end_point, to_edge, to_dataset,
               to_start_node, to_end_node, to_geom, junction_point, gap_m, split_from_line,
               NULL::text AS junction_kind, NULL::bigint AS target_node
        FROM u5_connect_candidate
        ORDER BY from_end_node, gap_m, to_original_id, to_edge, from_original_id, from_edge;
        ANALYZE u5_connect_junction;
        -- W3: two ends facing across one gap make ONE junction (the shorter gap; tie: DS1's end)
        WITH facing AS (
            SELECT one.from_end_node AS end_one, other.from_end_node AS end_other
            FROM u5_connect_junction one JOIN u5_connect_junction other
              ON one.to_edge = other.from_edge AND other.to_edge = one.from_edge
            UNION SELECT one.from_end_node, other.from_end_node
            FROM u5_connect_junction one JOIN u5_connect_junction other
              ON other.from_end_node = one.to_start_node AND (one.from_end_node = other.to_start_node OR one.from_end_node = other.to_end_node)
            UNION SELECT one.from_end_node, other.from_end_node
            FROM u5_connect_junction one JOIN u5_connect_junction other
              ON other.from_end_node = one.to_end_node AND (one.from_end_node = other.to_start_node OR one.from_end_node = other.to_end_node)),
        dropped AS (
            DELETE FROM u5_connect_junction one USING facing, u5_connect_junction other
            WHERE facing.end_one = one.from_end_node AND facing.end_other = other.from_end_node
              AND one.from_end_node <> other.from_end_node AND NOT one.split_from_line AND NOT other.split_from_line
              AND (one.gap_m, one.from_dataset, one.from_end_node) > (other.gap_m, other.from_dataset, other.from_end_node)
            RETURNING one.from_end_node)
        INSERT INTO u5_connect_taken SELECT DISTINCT from_end_node FROM dropped ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        UPDATE u5_rule_metric SET value = v_row_count
        WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'skipped_mutual';

        -- ── point: lines that cross with no shared node ──
        IF connect_rule.operation = 'point' THEN
            DROP TABLE IF EXISTS u5_connect_crossing;
            CREATE TEMP TABLE u5_connect_crossing ON COMMIT DROP AS
            SELECT from_line.edge_id AS from_edge, from_line.dataset AS from_dataset, from_line.geom AS from_geom,
                   to_line.edge_id AS to_edge, to_line.dataset AS to_dataset, to_line.start_node AS to_start_node,
                   to_line.end_node AS to_end_node, to_line.geom AS to_geom,
                   (ST_Dump(ST_Intersection(from_line.geom, to_line.geom))).geom AS crossing_point,
                   (from_line.level_key = to_line.level_key) AS same_level
            FROM u5_connect_from_line from_line
            JOIN u5_connect_to_line to_line ON to_line.dataset = 2 AND from_line.geom && to_line.geom
                                           AND ST_Intersects(from_line.geom, to_line.geom)
            WHERE from_line.dataset = 1
            UNION ALL
            SELECT from_line.edge_id, from_line.dataset, from_line.geom, to_line.edge_id, to_line.dataset,
                   to_line.start_node, to_line.end_node, to_line.geom,
                   (ST_Dump(ST_Intersection(from_line.geom, to_line.geom))).geom, (from_line.level_key = to_line.level_key)
            FROM u5_connect_from_line from_line
            JOIN u5_connect_to_line to_line ON to_line.dataset = 1 AND from_line.geom && to_line.geom
                                           AND ST_Intersects(from_line.geom, to_line.geom)
            WHERE from_line.dataset = 2;
            DELETE FROM u5_connect_crossing WHERE GeometryType(crossing_point) <> 'POINT'
                OR ST_Equals(crossing_point, ST_StartPoint(from_geom))
                OR ST_Equals(crossing_point, ST_EndPoint(from_geom));   -- an A end on B: handled above
            UPDATE u5_rule_metric SET value = value + (SELECT count(*) FROM u5_connect_crossing)
            WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'candidates';
            WITH skipped AS (DELETE FROM u5_connect_crossing WHERE NOT same_level RETURNING 1)
            SELECT count(*) INTO v_row_count FROM skipped;
            UPDATE u5_rule_metric SET value = value + v_row_count
            WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'skipped_level';
            INSERT INTO u5_connect_junction (from_edge, from_dataset, from_end_node, from_end_point, to_edge, to_dataset,
                                             to_start_node, to_end_node, to_geom, junction_point, gap_m, split_from_line,
                                             junction_kind, target_node)
            SELECT from_edge, from_dataset, NULL, NULL, to_edge, to_dataset, to_start_node, to_end_node, to_geom,
                   crossing_point, 0, true, 'cross', NULL
            FROM u5_connect_crossing;
        END IF;

        IF EXISTS (SELECT 1 FROM u5_connect_junction) THEN
            -- O2: within reach of a node an earlier rule made or used on B → that node
            UPDATE u5_connect_junction junction SET junction_kind = 'reuse', target_node = (
                SELECT node.node_id
                FROM u5_connect_junction_node earlier JOIN u5_node node ON node.node_id = earlier.node_id AND node.alive
                WHERE node.node_id <> junction.from_end_node
                  AND node.geom && ST_Expand(junction.junction_point, connect_rule.proximity_deg)
                  AND content.union5_dist_m(node.geom, junction.junction_point, v_m_per_deg_lon) <= connect_rule.proximity_m
                  AND (EXISTS (SELECT 1 FROM u5_connect_to_line to_line
                               WHERE to_line.start_node = node.node_id AND to_line.dataset = junction.to_dataset)
                       OR EXISTS (SELECT 1 FROM u5_connect_to_line to_line
                                  WHERE to_line.end_node = node.node_id AND to_line.dataset = junction.to_dataset))
                ORDER BY content.union5_dist_m(node.geom, junction.junction_point, v_m_per_deg_lon), node.node_id LIMIT 1)
            WHERE junction.junction_kind IS NULL AND junction.gap_m > 0 AND NOT junction.split_from_line;
            UPDATE u5_connect_junction SET junction_kind = NULL WHERE junction_kind = 'reuse' AND target_node IS NULL;
            -- gap of zero: A's end lies on B, so A's end node is the junction
            UPDATE u5_connect_junction SET junction_kind = 'onspan', target_node = from_end_node
            WHERE junction_kind IS NULL AND gap_m = 0 AND NOT split_from_line;
            -- a gap to B's end → B's end node
            UPDATE u5_connect_junction
            SET junction_kind = 'end',
                target_node = CASE WHEN ST_Equals(junction_point, ST_StartPoint(to_geom)) THEN to_start_node ELSE to_end_node END
            WHERE junction_kind IS NULL
              AND (ST_Equals(junction_point, ST_StartPoint(to_geom)) OR ST_Equals(junction_point, ST_EndPoint(to_geom)));
            -- a node already at the point (one of B's vertex nodes) → that node
            UPDATE u5_connect_junction junction SET target_node = (
                SELECT node.node_id FROM u5_node node
                WHERE node.alive AND node.geom && junction.junction_point AND ST_Equals(node.geom, junction.junction_point)
                ORDER BY node.dataset, node.node_id LIMIT 1)
            WHERE (junction.junction_kind IS NULL OR junction.junction_kind = 'cross') AND junction.target_node IS NULL;
            UPDATE u5_connect_junction SET junction_kind = 'vertex' WHERE junction_kind IS NULL AND target_node IS NOT NULL;
            -- otherwise a new node on B, on the 7 dp grid (one per distinct point)
            v_first_new_node_id := v_next_node_id;
            UPDATE u5_connect_junction SET junction_point = ST_SnapToGrid(junction_point, v_grid_deg) WHERE target_node IS NULL;
            INSERT INTO u5_node (node_id, dataset, original_id, geom, props, alive)
            SELECT v_next_node_id + ROW_NUMBER() OVER (ORDER BY ST_X(new_point.geom), ST_Y(new_point.geom)) - 1, 0, NULL,
                   new_point.geom, '{}'::jsonb, true
            FROM (SELECT DISTINCT junction_point AS geom FROM u5_connect_junction WHERE target_node IS NULL) new_point;
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_next_node_id := v_next_node_id + v_row_count;
            UPDATE u5_connect_junction junction SET junction_kind = COALESCE(junction.junction_kind, 'new'),
                target_node = (SELECT node.node_id FROM u5_node node
                               WHERE node.node_id >= v_first_new_node_id AND node.geom && junction.junction_point
                                 AND ST_Equals(node.geom, junction.junction_point) LIMIT 1)
            WHERE junction.target_node IS NULL;

            -- an A end lying on B where another node already sits (left apart when
            -- coincident snapping is off): they become one node, kept by priority
            -- created > DS1 > DS2, then lowest id
            DROP TABLE IF EXISTS u5_node_merge;
            CREATE TEMP TABLE u5_node_merge ON COMMIT DROP AS
            SELECT DISTINCT ON (gone_node) gone_node, kept_node FROM (
                SELECT CASE WHEN (other_node.dataset, other_node.node_id) < (from_end.dataset, from_end.node_id)
                            THEN from_end.node_id ELSE other_node.node_id END AS gone_node,
                       CASE WHEN (other_node.dataset, other_node.node_id) < (from_end.dataset, from_end.node_id)
                            THEN other_node.node_id ELSE from_end.node_id END AS kept_node
                FROM u5_connect_junction junction
                JOIN u5_node from_end ON from_end.node_id = junction.from_end_node
                JOIN u5_node other_node ON other_node.alive AND other_node.node_id <> junction.from_end_node
                                       AND other_node.geom && junction.from_end_point
                                       AND ST_Equals(other_node.geom, junction.from_end_point)
                WHERE junction.junction_kind = 'onspan') coinciding
            ORDER BY gone_node, kept_node;
            IF EXISTS (SELECT 1 FROM u5_node_merge) THEN
                UPDATE u5_node node SET alive = false, merged_into = merged.kept_node
                FROM u5_node_merge merged WHERE node.node_id = merged.gone_node;
                UPDATE u5_edge edge SET start_node = merged.kept_node FROM u5_node_merge merged WHERE edge.start_node = merged.gone_node;
                UPDATE u5_edge edge SET end_node = merged.kept_node FROM u5_node_merge merged WHERE edge.end_node = merged.gone_node;
                UPDATE u5_connect_junction junction SET target_node = merged.kept_node
                FROM u5_node_merge merged WHERE junction.target_node = merged.gone_node;
                UPDATE u5_connect_junction junction SET from_end_node = merged.kept_node
                FROM u5_node_merge merged WHERE junction.from_end_node = merged.gone_node;
                UPDATE u5_connect_to_line to_line SET start_node = merged.kept_node
                FROM u5_node_merge merged WHERE to_line.start_node = merged.gone_node;
                UPDATE u5_connect_to_line to_line SET end_node = merged.kept_node
                FROM u5_node_merge merged WHERE to_line.end_node = merged.gone_node;
            END IF;

            -- splits: B where the junction lies on its span; A where it crosses B
            INSERT INTO u5_split_request (edge_id, fraction, node_id)
            SELECT junction.to_edge, ST_LineLocatePoint(junction.to_geom, node.geom), junction.target_node
            FROM u5_connect_junction junction JOIN u5_node node ON node.node_id = junction.target_node
            WHERE junction.junction_kind IN ('onspan', 'vertex', 'new', 'cross');
            INSERT INTO u5_split_request (edge_id, fraction, node_id)
            SELECT junction.from_edge, ST_LineLocatePoint(from_line_edge.geom, node.geom), junction.target_node
            FROM u5_connect_junction junction
            JOIN u5_edge from_line_edge ON from_line_edge.edge_id = junction.from_edge
            JOIN u5_node node ON node.node_id = junction.target_node
            WHERE junction.split_from_line;
            SELECT count(DISTINCT request.edge_id) INTO v_row_count
            FROM u5_split_request request JOIN u5_edge edge ON edge.edge_id = request.edge_id
            WHERE request.fraction > 1e-12 AND request.fraction < 1 - 1e-12
              AND request.node_id <> edge.start_node AND request.node_id <> edge.end_node;
            UPDATE u5_rule_metric SET value = v_row_count
            WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'edges_split';
            v_next_edge_id := content.union5_apply_splits(v_next_edge_id);

            -- connectors across a gap: A's highway/footway + the rule's edge_tags
            INSERT INTO u5_edge (edge_id, dataset, original_id, geom, props, start_node, end_node, alive, type_ids,
                                 is_connector, made_by_rule, level_key)
            SELECT v_next_edge_id + ROW_NUMBER() OVER (ORDER BY junction.from_end_node), 0, NULL,
                   ST_MakeLine(from_end.geom, target.geom),
                   jsonb_strip_nulls(jsonb_build_object('highway', from_line_edge.props->'highway', 'footway', from_line_edge.props->'footway'))
                     || content.union5_tags_for('edge', connect_rule.edge_tags)
                     || jsonb_build_object('ext:union_rule', connect_rule.rule_name,
                                           'ext:union_confidence',
                                           round(GREATEST(0.0, 1.0 - junction.gap_m / connect_rule.proximity_m)::numeric, 3)),
                   junction.from_end_node, junction.target_node, true, '{}', true, connect_rule.rule_name, from_line_edge.level_key
            FROM u5_connect_junction junction
            JOIN u5_edge from_line_edge ON from_line_edge.edge_id = junction.from_edge
            JOIN u5_node from_end       ON from_end.node_id = junction.from_end_node
            JOIN u5_node target         ON target.node_id = junction.target_node
            WHERE junction.junction_kind IN ('reuse', 'end', 'vertex', 'new') AND NOT junction.split_from_line
              AND junction.from_end_node IS NOT NULL
              AND junction.target_node <> junction.from_end_node AND NOT ST_Equals(from_end.geom, target.geom);
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            v_next_edge_id := v_next_edge_id + v_row_count + 1;
            UPDATE u5_rule_metric SET value = v_row_count
            WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'connectors';

            -- the junction node: the rule's node_tags (existing values win; differing
            -- ones audited), the rules that used it (in rule order), a confidence
            v_junction_node_tags := content.union5_tags_for('node', connect_rule.node_tags);
            UPDATE u5_node node SET
                props = node.props || COALESCE((SELECT jsonb_object_agg(tag.key, tag.value) FROM jsonb_each(v_junction_node_tags) tag
                                                WHERE NOT node.props ? tag.key), '{}'::jsonb),
                audit = content.union5_audit_add(node.audit, (
                            SELECT jsonb_object_agg('ext:union_audit_' || regexp_replace(tag.key, '^ext:', ''),
                                                    to_jsonb(connect_rule.rule_name || '-' || (tag.value #>> '{}')) ORDER BY tag.key)
                            FROM jsonb_each(v_junction_node_tags) tag
                            WHERE node.props ? tag.key AND node.props->tag.key IS DISTINCT FROM tag.value)),
                union_rules = CASE WHEN node.union_rules IS NULL THEN connect_rule.rule_name
                                   WHEN connect_rule.rule_name = ANY(string_to_array(node.union_rules, ',')) THEN node.union_rules
                                   ELSE node.union_rules || ',' || connect_rule.rule_name END,
                confidence = GREATEST(COALESCE(node.confidence, 0.0), per_node.confidence)
            FROM (SELECT target_node,
                         max(CASE WHEN junction_kind = 'cross' OR gap_m = 0 THEN 1.0
                                  ELSE GREATEST(0.0, 1.0 - gap_m / connect_rule.proximity_m) END) AS confidence
                  FROM u5_connect_junction GROUP BY target_node) per_node
            WHERE node.node_id = per_node.target_node;

            INSERT INTO u5_connect_taken SELECT DISTINCT from_end_node FROM u5_connect_junction
            WHERE from_end_node IS NOT NULL ON CONFLICT DO NOTHING;
            INSERT INTO u5_connect_junction_node SELECT DISTINCT target_node FROM u5_connect_junction ON CONFLICT DO NOTHING;
            UPDATE u5_rule_metric SET value = (SELECT count(*) FROM u5_connect_junction)
            WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'junctions';
            UPDATE u5_rule_metric SET value = (SELECT count(DISTINCT target_node) FROM u5_connect_junction
                                               WHERE target_node >= v_first_new_node_id)
            WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'nodes_created';
            UPDATE u5_rule_metric SET value = (SELECT count(DISTINCT target_node) FROM u5_connect_junction
                                               WHERE target_node < v_first_new_node_id)
            WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'nodes_reused';
            UPDATE u5_rule_metric SET value = (SELECT count(*) FROM u5_connect_junction WHERE junction_kind = 'reuse')
            WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'joined_earlier_node';
        END IF;
        RAISE NOTICE '  connect rule % (%): % junction(s) at %', connect_rule.rule_seq, connect_rule.rule_name,
            (SELECT value FROM u5_rule_metric
             WHERE stage = 'connect' AND rule_name = connect_rule.rule_name AND metric = 'junctions'), clock_timestamp();
    END LOOP;
    IF EXISTS (SELECT 1 FROM u5_connect_rules) THEN
        RAISE NOTICE 'Phase 5 complete: % junctions, % connectors',
            (SELECT COALESCE(sum(value), 0) FROM u5_rule_metric WHERE stage = 'connect' AND metric = 'junctions'),
            (SELECT count(*) FROM u5_edge WHERE alive AND is_connector);
    END IF;
    v_timings := v_timings || jsonb_build_object('connect', round(extract(epoch FROM clock_timestamp() - v_stage_start)::numeric, 2));
    v_stage_start := clock_timestamp();
    -- O1: a rule that found locations but was beaten to all of them by earlier rules
    v_warnings := v_warnings || ARRAY(
        SELECT format('connect rule "%s" matched %s location(s), all taken by earlier rules — it made no junction',
                      junctions.rule_name, taken.value)
        FROM u5_rule_metric junctions
        JOIN u5_rule_metric taken ON taken.stage = junctions.stage AND taken.rule_name = junctions.rule_name
                                 AND taken.metric = 'skipped_taken'
        WHERE junctions.stage = 'connect' AND junctions.metric = 'junctions' AND junctions.value = 0 AND taken.value > 0
        ORDER BY junctions.rule_seq);

    -- =========================================================================
    -- PHASE 6 — STRANDING GUARD: restore a removed duplicate when its removal
    -- left a surviving edge hanging at a node that is not on the anchor network
    -- (no anchor edge and no connector uses it). Repeated until stable.
    -- =========================================================================
    -- Round by round: the first round looks at every removed duplicate; a later
    -- round only at those sharing a node with an edge the round before restored
    -- (nothing else changed for the rest), so the work follows the restorations.
    DROP TABLE IF EXISTS u5_removed_edge;
    CREATE TEMP TABLE u5_removed_edge ON COMMIT DROP AS
    SELECT edge_id, start_node, end_node FROM u5_edge WHERE removed_by IS NOT NULL AND NOT alive;
    CREATE INDEX ON u5_removed_edge (start_node);
    CREATE INDEX ON u5_removed_edge (end_node);
    ANALYZE u5_removed_edge;
    DROP TABLE IF EXISTS u5_restore_worklist;
    CREATE TEMP TABLE u5_restore_worklist (edge_id bigint PRIMARY KEY) ON COMMIT DROP;
    INSERT INTO u5_restore_worklist SELECT edge_id FROM u5_removed_edge;
    DROP TABLE IF EXISTS u5_restored_edge;
    CREATE TEMP TABLE u5_restored_edge (edge_id bigint, start_node bigint, end_node bigint) ON COMMIT DROP;
    LOOP
        TRUNCATE u5_restored_edge;
        WITH restored AS (
            UPDATE u5_edge removed SET alive = true
            FROM u5_restore_worklist worklist
            WHERE removed.edge_id = worklist.edge_id AND removed.removed_by IS NOT NULL AND NOT removed.alive
              AND EXISTS (
                  SELECT 1 FROM (VALUES (removed.start_node), (removed.end_node)) removed_end(node_id)
                  WHERE (EXISTS (SELECT 1 FROM u5_edge survivor WHERE survivor.start_node = removed_end.node_id AND survivor.alive)
                         OR EXISTS (SELECT 1 FROM u5_edge survivor WHERE survivor.end_node = removed_end.node_id AND survivor.alive))
                    AND NOT EXISTS (SELECT 1 FROM u5_edge anchor_edge
                                    WHERE anchor_edge.start_node = removed_end.node_id AND anchor_edge.alive
                                      AND (anchor_edge.is_connector OR anchor_edge.dataset = removed.removed_anchor_ds))
                    AND NOT EXISTS (SELECT 1 FROM u5_edge anchor_edge
                                    WHERE anchor_edge.end_node = removed_end.node_id AND anchor_edge.alive
                                      AND (anchor_edge.is_connector OR anchor_edge.dataset = removed.removed_anchor_ds)))
            RETURNING removed.edge_id, removed.start_node, removed.end_node)
        INSERT INTO u5_restored_edge SELECT edge_id, start_node, end_node FROM restored;
        GET DIAGNOSTICS v_row_count = ROW_COUNT;
        EXIT WHEN v_row_count = 0;
        TRUNCATE u5_restore_worklist;
        INSERT INTO u5_restore_worklist
        SELECT removed.edge_id FROM u5_restored_edge restored JOIN u5_removed_edge removed ON removed.start_node = restored.start_node
        UNION SELECT removed.edge_id FROM u5_restored_edge restored JOIN u5_removed_edge removed ON removed.start_node = restored.end_node
        UNION SELECT removed.edge_id FROM u5_restored_edge restored JOIN u5_removed_edge removed ON removed.end_node = restored.start_node
        UNION SELECT removed.edge_id FROM u5_restored_edge restored JOIN u5_removed_edge removed ON removed.end_node = restored.end_node;
    END LOOP;
    UPDATE u5_rule_metric rule_metric SET value = counted.edge_count
    FROM (SELECT removed_by, count(*) AS edge_count FROM u5_edge WHERE removed_by IS NOT NULL AND alive GROUP BY removed_by) counted
    WHERE rule_metric.stage = 'dedup' AND rule_metric.rule_name = counted.removed_by AND rule_metric.metric = 'restored';
    UPDATE u5_rule_metric rule_metric SET value = rule_metric.value - counted.edge_count
    FROM (SELECT removed_by, count(*) AS edge_count FROM u5_edge WHERE removed_by IS NOT NULL AND alive GROUP BY removed_by) counted
    WHERE rule_metric.stage = 'dedup' AND rule_metric.rule_name = counted.removed_by AND rule_metric.metric = 'removed';
    UPDATE u5_edge SET removed_by = NULL, absorbed_by = NULL WHERE removed_by IS NOT NULL AND alive;
    v_timings := v_timings || jsonb_build_object('stranding_guard', round(extract(epoch FROM clock_timestamp() - v_stage_start)::numeric, 2));
    v_stage_start := clock_timestamp();

    -- =========================================================================
    -- PHASE 7 — zones and extension features ('extensions': off = pass through;
    -- default = the 4.3.2 merging). Zone rings reference the final nodes.
    -- =========================================================================
    RAISE NOTICE 'Phase 7: zones and extensions (%) at %', v_extensions->>'mode', clock_timestamp();
    v_point_proximity_m   := COALESCE((v_extensions->'points'->>'proximity')::float8, 0.5);
    v_point_proximity_deg := v_point_proximity_m / v_m_per_deg_lon;
    v_line_proximity_deg  := COALESCE((v_extensions->'lines'->>'proximity')::float8, 0.5) / v_m_per_deg_lon;

    -- zones: each ring vertex → its node (by _w_id position, else by coordinate) → final node
    DROP TABLE IF EXISTS u5_zone_in;
    CREATE TEMP TABLE u5_zone_in ON COMMIT DROP AS
    SELECT raw.id AS zone_id, CASE WHEN raw.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END::smallint AS dataset,
           raw.zone_loc AS geom, raw.node_ids, (raw.feature->'properties')::jsonb - '_id' - '_w_id' AS props
    FROM content.zone raw WHERE raw.tdei_dataset_id IN (v_ds1, v_ds2);
    DROP TABLE IF EXISTS u5_zone_vertex;
    CREATE TEMP TABLE u5_zone_vertex ON COMMIT DROP AS
    SELECT input_zone.zone_id, input_zone.dataset, vertex.path[1] AS ring_no, vertex.path[2] AS vertex_no,
           vertex.geom AS vertex_geom,
           CASE WHEN cardinality(input_zone.node_ids) = ST_NPoints(input_zone.geom)
                THEN input_zone.node_ids[vertex.vertex_index] END AS w_original_id,
           NULL::bigint AS node_id
    FROM u5_zone_in input_zone, LATERAL ST_DumpPoints(input_zone.geom) WITH ORDINALITY AS vertex(path, geom, vertex_index);
    UPDATE u5_zone_vertex vertex SET node_id = (
        SELECT node.node_id FROM u5_node node
        WHERE node.dataset = vertex.dataset AND node.original_id = vertex.w_original_id::text
        ORDER BY node.node_id LIMIT 1)
    WHERE vertex.w_original_id IS NOT NULL;
    UPDATE u5_zone_vertex vertex SET node_id = (
        SELECT node.node_id FROM u5_node node
        WHERE node.dataset = vertex.dataset AND node.geom && vertex.vertex_geom AND ST_Equals(node.geom, vertex.vertex_geom)
        ORDER BY node.node_id LIMIT 1)
    WHERE vertex.node_id IS NULL;
    INSERT INTO u5_input_node_ref SELECT DISTINCT node_id FROM u5_zone_vertex WHERE node_id IS NOT NULL ON CONFLICT DO NOTHING;

    -- the final node of each zone node (follows joins)
    DROP TABLE IF EXISTS u5_zone_final_node;
    CREATE TEMP TABLE u5_zone_final_node ON COMMIT DROP AS
    WITH RECURSIVE chain(node_id, current_node, depth) AS (
        SELECT node_id, node_id, 0 FROM (SELECT DISTINCT node_id FROM u5_zone_vertex WHERE node_id IS NOT NULL) zone_node
        UNION ALL
        SELECT chain.node_id, node.merged_into, chain.depth + 1
        FROM chain JOIN u5_node node ON node.node_id = chain.current_node
        WHERE node.merged_into IS NOT NULL AND chain.depth < 16)
    SELECT DISTINCT ON (node_id) node_id, current_node AS final_node FROM chain ORDER BY node_id, depth DESC;
    CREATE INDEX ON u5_zone_final_node (node_id);

    DROP TABLE IF EXISTS u5_zone_rings;
    CREATE TEMP TABLE u5_zone_rings ON COMMIT DROP AS
    SELECT vertex.zone_id, vertex.ring_no,
           ST_MakeLine(COALESCE(final_point.geom, vertex.vertex_geom) ORDER BY vertex.vertex_no) AS ring_line,
           array_agg(final_ref.final_node ORDER BY vertex.vertex_no) FILTER (WHERE final_ref.final_node IS NOT NULL) AS node_ids
    FROM u5_zone_vertex vertex
    LEFT JOIN u5_zone_final_node final_ref ON final_ref.node_id = vertex.node_id
    LEFT JOIN u5_node final_point ON final_point.node_id = final_ref.final_node
    GROUP BY vertex.zone_id, vertex.ring_no;
    DROP TABLE IF EXISTS u5_zone_out;
    CREATE TEMP TABLE u5_zone_out ON COMMIT DROP AS
    SELECT built.*,
           -- validity is checked once per zone, not once per pair it meets
           CASE WHEN (v_extensions->'zones'->>'enabled')::boolean THEN ST_IsValid(built.geom) END AS valid
    FROM (
    SELECT input_zone.zone_id, input_zone.dataset, input_zone.props,
           CASE WHEN array_length(rings.inner_rings, 1) > 0 THEN ST_MakePolygon(rings.outer_ring, rings.inner_rings)
                ELSE ST_MakePolygon(rings.outer_ring) END AS geom,
           -- _w_id: every ring's node ids, outer ring first; adjacent repeats dropped
           ARRAY(SELECT ring_node.node_id::text FROM (
                     SELECT node_id, node_index, LAG(node_id) OVER (ORDER BY node_index) AS previous_node
                     FROM unnest(rings.node_ids) WITH ORDINALITY listed(node_id, node_index)) ring_node
                 WHERE ring_node.previous_node IS DISTINCT FROM ring_node.node_id ORDER BY ring_node.node_index) AS w_node_ids
    FROM u5_zone_in input_zone
    JOIN (SELECT ring.zone_id,
                 (array_agg(ring.ring_line ORDER BY ring.ring_no) FILTER (WHERE ring.ring_no = 1))[1] AS outer_ring,
                 array_agg(ring.ring_line ORDER BY ring.ring_no) FILTER (WHERE ring.ring_no > 1) AS inner_rings,
                 ARRAY(SELECT listed.node_id
                       FROM u5_zone_rings each_ring, unnest(each_ring.node_ids) WITH ORDINALITY listed(node_id, node_index)
                       WHERE each_ring.zone_id = ring.zone_id ORDER BY each_ring.ring_no, listed.node_index) AS node_ids
          FROM u5_zone_rings ring GROUP BY ring.zone_id) rings ON rings.zone_id = input_zone.zone_id
    WHERE ST_NPoints(rings.outer_ring) >= 4 AND ST_IsClosed(rings.outer_ring)
    OFFSET 0) built;
    IF (v_extensions->'zones'->>'enabled')::boolean THEN
        -- a DS2 zone covering more than zones.min_overlap of a DS1 zone's area is the same zone (DS1's kept)
        CREATE INDEX ON u5_zone_out USING GIST (geom);
        ANALYZE u5_zone_out;
        -- the overlap is computed once per pair (filter and ranking share it)
        DELETE FROM u5_zone_out zone_to_drop USING (
            SELECT DISTINCT ON (zone_pair.ds1_zone_id) zone_pair.ds1_zone_id, zone_pair.ds2_zone_id
            FROM (SELECT ds1_zone.zone_id AS ds1_zone_id, ds2_zone.zone_id AS ds2_zone_id,
                         ST_Area(ST_Intersection(ds1_zone.geom, ds2_zone.geom)) / NULLIF(ST_Area(ds1_zone.geom), 0) AS overlap
                  FROM u5_zone_out ds1_zone
                  JOIN u5_zone_out ds2_zone ON ds1_zone.dataset = 1 AND ds2_zone.dataset = 2
                                           AND ds1_zone.valid AND ds2_zone.valid AND ST_Intersects(ds1_zone.geom, ds2_zone.geom)
                  OFFSET 0) zone_pair
            WHERE zone_pair.overlap > (v_extensions->'zones'->>'min_overlap')::float8
            ORDER BY zone_pair.ds1_zone_id, zone_pair.overlap DESC, zone_pair.ds2_zone_id) same_zone
        WHERE zone_to_drop.zone_id = same_zone.ds2_zone_id;
    END IF;

    -- extension points: a DS2 point within proximity of a SAME-TYPE DS1 point is the
    -- same feature (DS1 position; DS2-only keys added; every DS2 value audited)
    DROP TABLE IF EXISTS u5_ext_point;
    CREATE TEMP TABLE u5_ext_point ON COMMIT DROP AS
    SELECT raw.id AS point_id, CASE WHEN raw.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END::smallint AS dataset,
           raw.properties->>'_id' AS original_id, raw.point_loc AS geom,
           raw.properties - '_id' AS props,
           COALESCE((SELECT string_agg(tag.key || '=' || tag.value, ',' ORDER BY tag.key)
                     FROM jsonb_each_text(raw.properties) tag
                     WHERE tag.key <> '_id' AND tag.key NOT LIKE 'ext:%'), 'other') AS point_type
    FROM (SELECT id, tdei_dataset_id, point_loc, (feature->'properties')::jsonb AS properties   -- parse the properties once
          FROM content.extension_point WHERE tdei_dataset_id IN (v_ds1, v_ds2) OFFSET 0) raw;
    CREATE INDEX ON u5_ext_point USING GIST (geom);
    CREATE INDEX ON u5_ext_point (point_id);
    ANALYZE u5_ext_point;
    DROP TABLE IF EXISTS u5_ext_point_match;
    CREATE TEMP TABLE u5_ext_point_match (ds2_point_id bigint, ds1_point_id bigint) ON COMMIT DROP;
    IF (v_extensions->'points'->>'enabled')::boolean THEN
        INSERT INTO u5_ext_point_match
        SELECT DISTINCT ON (ds2_point.point_id) ds2_point.point_id, ds1_point.point_id
        FROM u5_ext_point ds2_point
        JOIN u5_ext_point ds1_point ON ds1_point.dataset = 1 AND ds2_point.dataset = 2
            AND ST_DWithin(ds2_point.geom, ds1_point.geom, v_point_proximity_deg)
            AND content.union5_dist_m(ds2_point.geom, ds1_point.geom, v_m_per_deg_lon) <= v_point_proximity_m
            AND (ds1_point.point_type = 'other' OR ds2_point.point_type = 'other' OR ds1_point.point_type = ds2_point.point_type)
        ORDER BY ds2_point.point_id, content.union5_dist_m(ds2_point.geom, ds1_point.geom, v_m_per_deg_lon), ds1_point.point_id;
    END IF;
    CREATE INDEX ON u5_ext_point_match (ds1_point_id);           -- each DS1 point finds its matches by index
    CREATE INDEX ON u5_ext_point_match (ds2_point_id);
    ANALYZE u5_ext_point_match;
    DROP TABLE IF EXISTS u5_out_points;
    CREATE TEMP TABLE u5_out_points ON COMMIT DROP AS
    SELECT merged.id, jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(merged.geom, 15)::jsonb,
                                         'properties', jsonb_build_object('_id', merged.id::text) || merged.props) AS feature
    FROM (
        SELECT ds1_point.point_id AS id, ds1_point.geom,
               ds1_point.props
               || COALESCE((SELECT jsonb_object_agg(new_tag.key, new_tag.value) FROM (
                      SELECT DISTINCT ON (tag.key) tag.key, tag.value
                      FROM u5_ext_point_match point_match
                      JOIN u5_ext_point ds2_point ON ds2_point.point_id = point_match.ds2_point_id,
                           jsonb_each(ds2_point.props) tag
                      WHERE point_match.ds1_point_id = ds1_point.point_id AND NOT ds1_point.props ? tag.key
                      ORDER BY tag.key, ds2_point.original_id DESC) new_tag), '{}'::jsonb)
               || COALESCE((SELECT jsonb_object_agg('ext:union_audit_' || regexp_replace(audited.key, '^ext:', ''),
                                                    to_jsonb(audited.ds2_values) ORDER BY audited.key) FROM (
                      SELECT tag.key,
                             string_agg(COALESCE(ds2_point.original_id, '?') || '-' || (tag.value #>> '{}'), ','
                                        ORDER BY ds2_point.original_id) AS ds2_values
                      FROM u5_ext_point_match point_match
                      JOIN u5_ext_point ds2_point ON ds2_point.point_id = point_match.ds2_point_id,
                           jsonb_each(ds2_point.props) tag
                      WHERE point_match.ds1_point_id = ds1_point.point_id GROUP BY tag.key) audited), '{}'::jsonb) AS props
        FROM u5_ext_point ds1_point WHERE ds1_point.dataset = 1
        UNION ALL
        SELECT ds2_point.point_id, ds2_point.geom, ds2_point.props FROM u5_ext_point ds2_point
        WHERE ds2_point.dataset = 2
          AND NOT EXISTS (SELECT 1 FROM u5_ext_point_match point_match WHERE point_match.ds2_point_id = ds2_point.point_id)) merged;
    CREATE INDEX ON u5_out_points (id);

    -- extension lines: a DS2 line with at least lines.min_overlap of its length inside a
    -- DS1 line's lines.proximity buffer is the same line (DS1 geometry; DS2-only keys added)
    DROP TABLE IF EXISTS u5_ext_line;
    CREATE TEMP TABLE u5_ext_line ON COMMIT DROP AS
    SELECT raw.id AS line_id, CASE WHEN raw.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END::smallint AS dataset,
           raw.line_loc AS geom, (raw.feature->'properties')::jsonb - '_id' AS props
    FROM content.extension_line raw WHERE raw.tdei_dataset_id IN (v_ds1, v_ds2);
    CREATE INDEX ON u5_ext_line (line_id);
    ANALYZE u5_ext_line;
    DROP TABLE IF EXISTS u5_ext_line_match;
    CREATE TEMP TABLE u5_ext_line_match (ds2_line_id bigint, ds1_line_id bigint) ON COMMIT DROP;
    IF (v_extensions->'lines'->>'enabled')::boolean THEN
        DROP TABLE IF EXISTS u5_ext_line_buffer;
        CREATE TEMP TABLE u5_ext_line_buffer ON COMMIT DROP AS
        SELECT line_id, ST_Buffer(geom, v_line_proximity_deg) AS buffer_geom FROM u5_ext_line WHERE dataset = 1;
        CREATE INDEX ON u5_ext_line_buffer USING GIST (buffer_geom);
        INSERT INTO u5_ext_line_match
        SELECT DISTINCT ON (ds2_line_id) ds2_line_id, ds1_line_id FROM (
            SELECT ds2_line.line_id AS ds2_line_id, ds1_buffer.line_id AS ds1_line_id,
                   ST_Length(ST_CollectionExtract(ST_Intersection(ds2_line.geom, ds1_buffer.buffer_geom), 2))
                     / NULLIF(ST_Length(ds2_line.geom), 0) AS overlap
            FROM u5_ext_line ds2_line
            JOIN u5_ext_line_buffer ds1_buffer ON ds2_line.dataset = 2 AND ST_DWithin(ds2_line.geom, ds1_buffer.buffer_geom, 0)) line_pair
        WHERE overlap >= (v_extensions->'lines'->>'min_overlap')::float8
        ORDER BY ds2_line_id, overlap DESC, ds1_line_id;
    END IF;
    CREATE INDEX ON u5_ext_line_match (ds1_line_id);
    CREATE INDEX ON u5_ext_line_match (ds2_line_id);
    ANALYZE u5_ext_line_match;
    DROP TABLE IF EXISTS u5_out_lines;
    CREATE TEMP TABLE u5_out_lines ON COMMIT DROP AS
    SELECT merged.id, jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(merged.geom, 15)::jsonb,
                                         'properties', jsonb_build_object('_id', merged.id::text) || merged.props) AS feature
    FROM (
        SELECT ds1_line.line_id AS id, ds1_line.geom,
               ds1_line.props || COALESCE((SELECT jsonb_object_agg(tag.key, tag.value ORDER BY ds2_line.line_id)   -- the highest id wins
                                           FROM u5_ext_line_match line_match
                                           JOIN u5_ext_line ds2_line ON ds2_line.line_id = line_match.ds2_line_id,
                                                jsonb_each(ds2_line.props) tag
                                           WHERE line_match.ds1_line_id = ds1_line.line_id AND NOT ds1_line.props ? tag.key),
                                          '{}'::jsonb) AS props
        FROM u5_ext_line ds1_line WHERE ds1_line.dataset = 1
        UNION ALL
        SELECT ds2_line.line_id, ds2_line.geom, ds2_line.props FROM u5_ext_line ds2_line
        WHERE ds2_line.dataset = 2
          AND NOT EXISTS (SELECT 1 FROM u5_ext_line_match line_match WHERE line_match.ds2_line_id = ds2_line.line_id)) merged;
    CREATE INDEX ON u5_out_lines (id);

    -- extension polygons: a DS2 polygon covering more than polygons.min_overlap of a DS1
    -- polygon's area → DS1 geometry + DS2-only keys; intersecting below that → the
    -- union of both; otherwise each is kept
    DROP TABLE IF EXISTS u5_ext_polygon;
    CREATE TEMP TABLE u5_ext_polygon ON COMMIT DROP AS
    SELECT raw.id AS polygon_id, CASE WHEN raw.tdei_dataset_id = v_ds1 THEN 1 ELSE 2 END::smallint AS dataset,
           raw.polygon_loc AS geom, (raw.feature->'properties')::jsonb - '_id' AS props,
           -- validity is checked once per polygon, not once per pair it meets
           CASE WHEN (v_extensions->'polygons'->>'enabled')::boolean THEN ST_IsValid(raw.polygon_loc) END AS valid
    FROM content.extension_polygon raw WHERE raw.tdei_dataset_id IN (v_ds1, v_ds2);
    DROP TABLE IF EXISTS u5_ext_polygon_pair;
    CREATE TEMP TABLE u5_ext_polygon_pair (ds1_polygon_id bigint, ds2_polygon_id bigint, overlap float8) ON COMMIT DROP;
    IF (v_extensions->'polygons'->>'enabled')::boolean THEN
        CREATE INDEX ON u5_ext_polygon USING GIST (geom);  -- each DS1 polygon meets only the DS2 polygons it touches
        CREATE INDEX ON u5_ext_polygon (polygon_id);
        ANALYZE u5_ext_polygon;
        INSERT INTO u5_ext_polygon_pair
        SELECT DISTINCT ON (ds1_polygon.polygon_id) ds1_polygon.polygon_id, ds2_polygon.polygon_id,
               ST_Area(ST_Intersection(ds1_polygon.geom, ds2_polygon.geom)) / NULLIF(ST_Area(ds1_polygon.geom), 0)
        FROM u5_ext_polygon ds1_polygon
        JOIN u5_ext_polygon ds2_polygon ON ds1_polygon.dataset = 1 AND ds2_polygon.dataset = 2
             AND ds1_polygon.valid AND ds2_polygon.valid AND ST_Intersects(ds1_polygon.geom, ds2_polygon.geom)
        ORDER BY ds1_polygon.polygon_id,
                 ST_Area(ST_Intersection(ds1_polygon.geom, ds2_polygon.geom)) / NULLIF(ST_Area(ds1_polygon.geom), 0) DESC,
                 ds2_polygon.polygon_id;
    END IF;
    CREATE INDEX ON u5_ext_polygon_pair (ds1_polygon_id);
    CREATE INDEX ON u5_ext_polygon_pair (ds2_polygon_id);
    ANALYZE u5_ext_polygon_pair;
    DROP TABLE IF EXISTS u5_out_polygons;
    CREATE TEMP TABLE u5_out_polygons ON COMMIT DROP AS
    SELECT merged.id, jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(merged.geom, 15)::jsonb,
                                         'properties', jsonb_build_object('_id', merged.id::text) || merged.props) AS feature
    FROM (
        SELECT ds1_polygon.polygon_id AS id,
               CASE WHEN polygon_pair.ds1_polygon_id IS NULL
                         OR polygon_pair.overlap > (v_extensions->'polygons'->>'min_overlap')::float8 THEN ds1_polygon.geom
                    ELSE ST_Union(ds1_polygon.geom, ds2_polygon.geom) END AS geom,
               ds1_polygon.props || COALESCE((SELECT jsonb_object_agg(tag.key, tag.value) FROM jsonb_each(ds2_polygon.props) tag
                                              WHERE NOT ds1_polygon.props ? tag.key), '{}'::jsonb) AS props
        FROM u5_ext_polygon ds1_polygon
        LEFT JOIN u5_ext_polygon_pair polygon_pair ON polygon_pair.ds1_polygon_id = ds1_polygon.polygon_id
        LEFT JOIN u5_ext_polygon ds2_polygon ON ds2_polygon.polygon_id = polygon_pair.ds2_polygon_id
        WHERE ds1_polygon.dataset = 1
        UNION ALL
        SELECT ds2_polygon.polygon_id, ds2_polygon.geom, ds2_polygon.props FROM u5_ext_polygon ds2_polygon
        WHERE ds2_polygon.dataset = 2
          AND NOT EXISTS (SELECT 1 FROM u5_ext_polygon_pair polygon_pair
                          WHERE polygon_pair.ds2_polygon_id = ds2_polygon.polygon_id)) merged;
    CREATE INDEX ON u5_out_polygons (id);
    v_timings := v_timings || jsonb_build_object('extensions', round(extract(epoch FROM clock_timestamp() - v_stage_start)::numeric, 2));
    v_stage_start := clock_timestamp();

    -- =========================================================================
    -- PHASE 8 — output: node, edge, zone, point, line, polygon, report
    -- =========================================================================
    RAISE NOTICE 'Phase 8: output at %', clock_timestamp();
    -- an anchor edge records the original _ids of the duplicates it replaced
    DROP TABLE IF EXISTS u5_absorbed;
    CREATE TEMP TABLE u5_absorbed ON COMMIT DROP AS
    SELECT absorbed_by AS root_edge, string_agg(COALESCE(original_id, edge_id::text), ',' ORDER BY original_id) AS absorbed_ids
    FROM u5_edge WHERE removed_by IS NOT NULL AND NOT alive AND absorbed_by IS NOT NULL GROUP BY absorbed_by;

    DROP TABLE IF EXISTS u5_out_edges;
    CREATE TEMP TABLE u5_out_edges ON COMMIT DROP AS
    SELECT numbered.seq_id AS id, numbered.start_node, numbered.end_node,
           jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(numbered.geom, 15)::jsonb,
               'properties', jsonb_build_object('_id', numbered.seq_id::text, '_u_id', numbered.start_node::text,
                                                '_v_id', numbered.end_node::text)
                             || numbered.props
                             || CASE WHEN numbered.absorbed_ids IS NOT NULL
                                     THEN jsonb_build_object('ext:union_absorbed', numbered.absorbed_ids)
                                     ELSE '{}'::jsonb END) AS feature
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY edge.dataset = 0, COALESCE(edge.parent_edge, edge.edge_id), edge.edge_id) AS seq_id,
                 edge.start_node, edge.end_node, edge.geom, edge.props, absorbed.absorbed_ids
          FROM u5_edge edge
          LEFT JOIN u5_absorbed absorbed ON absorbed.root_edge = COALESCE(edge.parent_edge, edge.edge_id)
          WHERE edge.alive AND ST_NPoints(edge.geom) >= 2 AND ST_Length(edge.geom) > 0) numbered;
    CREATE INDEX ON u5_out_edges (id);

    DROP TABLE IF EXISTS u5_out_zones;
    CREATE TEMP TABLE u5_out_zones ON COMMIT DROP AS
    SELECT zone_id AS id, w_node_ids,
           jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(geom, 15)::jsonb,
               'properties', jsonb_build_object('_id', zone_id::text) || props
                             || jsonb_build_object('_w_id', to_jsonb(w_node_ids))) AS feature
    FROM u5_zone_out WHERE geom IS NOT NULL;
    CREATE INDEX ON u5_out_zones (id);

    -- nodes: every node an output edge or zone uses, plus input orphans (passed through)
    DROP TABLE IF EXISTS u5_output_node_ref;
    CREATE TEMP TABLE u5_output_node_ref ON COMMIT DROP AS
    SELECT start_node AS node_id FROM u5_out_edges UNION SELECT end_node FROM u5_out_edges
    UNION SELECT w_node_id::bigint FROM u5_out_zones, unnest(w_node_ids) w_node_id;
    CREATE INDEX ON u5_output_node_ref (node_id);
    DROP TABLE IF EXISTS u5_out_nodes;
    CREATE TEMP TABLE u5_out_nodes ON COMMIT DROP AS
    SELECT node.node_id AS id,
           jsonb_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(node.geom, 15)::jsonb,
               'properties', jsonb_build_object('_id', node.node_id::text) || node.props || node.audit
                   || CASE WHEN node.absorbed IS NOT NULL THEN jsonb_build_object('ext:union_absorbed', node.absorbed)
                           ELSE '{}'::jsonb END
                   || CASE WHEN node.union_rules IS NOT NULL THEN jsonb_build_object('ext:union_rule', node.union_rules)
                           ELSE '{}'::jsonb END
                   || CASE WHEN node.confidence IS NOT NULL
                           THEN jsonb_build_object('ext:union_confidence', round(node.confidence::numeric, 3))
                           ELSE '{}'::jsonb END) AS feature
    FROM u5_node node
    WHERE node.alive
      AND ((node.dataset <> 0 AND NOT EXISTS (SELECT 1 FROM u5_input_node_ref input_ref WHERE input_ref.node_id = node.node_id))
           OR EXISTS (SELECT 1 FROM u5_output_node_ref output_ref WHERE output_ref.node_id = node.node_id));
    CREATE INDEX ON u5_out_nodes (id);

    -- ext:* values must have one JSON type per key across a file: a key seen
    -- with mixed types is written as text everywhere
    FOREACH v_out_table IN ARRAY ARRAY['u5_out_nodes', 'u5_out_edges', 'u5_out_zones', 'u5_out_points', 'u5_out_lines', 'u5_out_polygons'] LOOP
        EXECUTE format($q$
            SELECT jsonb_object_agg(key, true) FROM (
                SELECT key FROM (SELECT key, jsonb_typeof(value) AS value_type FROM %I, jsonb_each(feature->'properties')
                                 WHERE key LIKE 'ext:%%' GROUP BY 1, 2) key_types    -- a few groups: a hash, no sort
                GROUP BY key HAVING count(*) > 1) mixed $q$, v_out_table) INTO v_mixed_type_keys;
        IF v_mixed_type_keys IS NOT NULL THEN
            EXECUTE format($q$
                UPDATE %I SET feature = jsonb_set(feature, '{properties}', (
                    SELECT jsonb_object_agg(key, CASE WHEN $1 ? key AND jsonb_typeof(value) <> 'string'
                                                      THEN to_jsonb(value #>> '{}') ELSE value END)
                    FROM jsonb_each(feature->'properties')))
                WHERE EXISTS (SELECT 1 FROM jsonb_each(feature->'properties') WHERE $1 ? key) $q$, v_out_table)
                USING v_mixed_type_keys;
        END IF;
    END LOOP;

    v_timings := v_timings || jsonb_build_object('output', round(extract(epoch FROM clock_timestamp() - v_stage_start)::numeric, 2));
    v_report := jsonb_build_object(
        'union_version', '5.0',
        'datasets', jsonb_build_object('ds1', v_ds1, 'ds2', v_ds2),
        'effective_config', v_config,
        'rules', (SELECT COALESCE(jsonb_agg(jsonb_build_object('stage', per_rule.stage, 'rule', per_rule.rule_name,
                                                               'metrics', per_rule.metrics)
                                            ORDER BY per_rule.stage_order, per_rule.rule_seq), '[]'::jsonb)
                  FROM (SELECT stage, rule_name, min(rule_seq) AS rule_seq, jsonb_object_agg(metric, value) AS metrics,
                               CASE stage WHEN 'snap' THEN 1 WHEN 'dedup' THEN 2 ELSE 3 END AS stage_order
                        FROM u5_rule_metric GROUP BY stage, rule_name) per_rule),
        'warnings', to_jsonb(v_warnings),
        'counts', jsonb_build_object(
            'input', v_input_counts || jsonb_build_object(
                'zones', (SELECT count(*) FROM u5_zone_in), 'points', (SELECT count(*) FROM u5_ext_point),
                'lines', (SELECT count(*) FROM u5_ext_line), 'polygons', (SELECT count(*) FROM u5_ext_polygon)),
            'output', jsonb_build_object(
                'nodes', (SELECT count(*) FROM u5_out_nodes), 'edges', (SELECT count(*) FROM u5_out_edges),
                'zones', (SELECT count(*) FROM u5_out_zones), 'points', (SELECT count(*) FROM u5_out_points),
                'lines', (SELECT count(*) FROM u5_out_lines), 'polygons', (SELECT count(*) FROM u5_out_polygons)),
            'nodes_snapped', (SELECT count(*) FROM u5_snap_pair),
            'edges_removed', (SELECT count(*) FROM u5_edge WHERE removed_by IS NOT NULL AND NOT alive),
            'nodes_created', (SELECT count(*) FROM u5_node WHERE dataset = 0 AND alive),
            'connectors', (SELECT count(*) FROM u5_edge WHERE alive AND is_connector)),
        -- seconds per stage (the cursors are read after this, by the caller)
        'timings_s', v_timings);
    RAISE NOTICE 'Phase 8: % nodes, % edges, % zones, % points, % lines, % polygons; % warning(s)',
        (SELECT count(*) FROM u5_out_nodes), (SELECT count(*) FROM u5_out_edges), (SELECT count(*) FROM u5_out_zones),
        (SELECT count(*) FROM u5_out_points), (SELECT count(*) FROM u5_out_lines), (SELECT count(*) FROM u5_out_polygons),
        cardinality(v_warnings);

    v_out_cursor := 'node_cursor';
    OPEN v_out_cursor FOR SELECT feature FROM u5_out_nodes ORDER BY id;
    file_name := 'node'; cursor_ref := v_out_cursor; RETURN NEXT;
    v_out_cursor := 'edge_cursor';
    OPEN v_out_cursor FOR SELECT feature FROM u5_out_edges ORDER BY id;
    file_name := 'edge'; cursor_ref := v_out_cursor; RETURN NEXT;
    v_out_cursor := 'zone_cursor';
    OPEN v_out_cursor FOR SELECT feature FROM u5_out_zones ORDER BY id;
    file_name := 'zone'; cursor_ref := v_out_cursor; RETURN NEXT;
    v_out_cursor := 'point_cursor';
    OPEN v_out_cursor FOR SELECT feature FROM u5_out_points ORDER BY id;
    file_name := 'point'; cursor_ref := v_out_cursor; RETURN NEXT;
    v_out_cursor := 'line_cursor';
    OPEN v_out_cursor FOR SELECT feature FROM u5_out_lines ORDER BY id;
    file_name := 'line'; cursor_ref := v_out_cursor; RETURN NEXT;
    v_out_cursor := 'polygon_cursor';
    OPEN v_out_cursor FOR SELECT feature FROM u5_out_polygons ORDER BY id;
    file_name := 'polygon'; cursor_ref := v_out_cursor; RETURN NEXT;
    v_out_cursor := 'report_cursor';
    OPEN v_out_cursor FOR SELECT v_report;
    file_name := 'report'; cursor_ref := v_out_cursor; RETURN NEXT;
    RETURN;
END;
$BODY$;
