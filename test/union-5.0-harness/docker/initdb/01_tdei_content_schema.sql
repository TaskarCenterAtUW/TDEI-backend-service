-- =============================================================================
-- TDEI content schema — node, edge, zone and extension tables exactly as in the
-- TDEI reference DDL (feature json + GENERATED columns).
--
-- Used two ways:
--   • automatically, by the local Docker database on first start;
--   • by hand, to build a scratch database:  psql "<scratch-db>" -f docker/initdb/01_tdei_content_schema.sql
--
-- Loading data: insert ONLY (tdei_dataset_id, feature). Every other column is
-- generated from the feature (ids must be numeric: _id/_u_id/_v_id -> bigint).
--
-- Everything here — every table and content.zone_extract_w_id — is from the
-- TDEI reference DDL.
-- =============================================================================
\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS content;
-- the union .sql ends with ALTER FUNCTION ... OWNER TO tdeiadmin
DO $$ BEGIN CREATE ROLE tdeiadmin; EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS content.edge
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    tdei_dataset_id character varying(40) COLLATE pg_catalog."default" NOT NULL,
    feature json NOT NULL,
    edge_id bigint GENERATED ALWAYS AS ((feature->'properties'->>'_id')::bigint) STORED,
    edge_loc geometry(LineString, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_GeomFromGeoJSON(feature->>'geometry'), 4326)) STORED,
    orig_node_id bigint GENERATED ALWAYS AS ((feature->'properties'->>'_u_id')::bigint) STORED,
    dest_node_id bigint GENERATED ALWAYS AS ((feature->'properties'->>'_v_id')::bigint) STORED,
    name character varying GENERATED ALWAYS AS ((feature->'properties'->>'name')::text) STORED,
    highway character varying GENERATED ALWAYS AS ((feature->'properties'->>'highway')::text) STORED,
    service character varying GENERATED ALWAYS AS ((feature->'properties'->>'service')::text) STORED,
    footway character varying GENERATED ALWAYS AS ((feature->'properties'->>'footway')::text) STORED,
    foot character varying GENERATED ALWAYS AS ((feature->'properties'->>'foot')::text) STORED,
    description character varying GENERATED ALWAYS AS ((feature->'properties'->>'description')::text) STORED,
    incline real GENERATED ALWAYS AS ((feature->'properties'->>'incline')::real) STORED,
    surface character varying GENERATED ALWAYS AS ((feature->'properties'->>'surface')::text) STORED,
    length real GENERATED ALWAYS AS ((feature->'properties'->>'length')::real) STORED,
    width real GENERATED ALWAYS AS ((feature->'properties'->>'width')::real) STORED,
    tactile_paving character varying GENERATED ALWAYS AS ((feature->'properties'->>'tactile_paving')::text) STORED,
    crossing_markings character varying GENERATED ALWAYS AS ((feature->'properties'->>'crossing:markings')::text) STORED,
    step_count integer GENERATED ALWAYS AS ((feature->'properties'->>'step_count')::integer) STORED,
    climb character varying GENERATED ALWAYS AS ((feature->'properties'->>'climb')::text) STORED,
    building character varying GENERATED ALWAYS AS ((feature->'properties'->>'building')::text) STORED,
    opening_hours character varying GENERATED ALWAYS AS ((feature->'properties'->>'opening_hours')::text) STORED,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    requested_by character varying(40) COLLATE pg_catalog."default",
    CONSTRAINT "PK_edge_id" PRIMARY KEY (id),
    CONSTRAINT unq_dataset_edge_id UNIQUE (tdei_dataset_id, edge_id)
);
CREATE INDEX IF NOT EXISTS idx_edge_location ON content.edge USING gist (edge_loc);

CREATE TABLE IF NOT EXISTS content.node
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    tdei_dataset_id character varying(40) COLLATE pg_catalog."default" NOT NULL,
    feature json NOT NULL,
    node_loc geometry(Point, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_GeomFromGeoJSON(feature->>'geometry'), 4326)) STORED,
    node_id bigint GENERATED ALWAYS AS ((feature->'properties'->>'_id')::bigint) STORED,
    barrier character varying GENERATED ALWAYS AS ((feature->'properties'->>'barrier')::text) STORED,
    kerb character varying GENERATED ALWAYS AS ((feature->'properties'->>'kerb')::text) STORED,
    tactile_paving character varying GENERATED ALWAYS AS ((feature->'properties'->>'tactile_paving')::text) STORED,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    requested_by character varying(40) COLLATE pg_catalog."default",
    CONSTRAINT "PK_node_id" PRIMARY KEY (id),
    CONSTRAINT unq_dataset_node_id UNIQUE (tdei_dataset_id, node_id)
);
CREATE INDEX IF NOT EXISTS idx_nodes_location ON content.node USING gist (node_loc);

CREATE TABLE IF NOT EXISTS content.extension_point
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    tdei_dataset_id character varying(40) COLLATE pg_catalog."default" NOT NULL,
    feature json NOT NULL,
    point_loc geometry(Point, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_GeomFromGeoJSON(feature->>'geometry'), 4326)) STORED,
    point_id bigint GENERATED ALWAYS AS ((feature->'properties'->>'_id')::bigint) STORED,
    emergency character varying GENERATED ALWAYS AS ((feature->'properties'->>'emergency')::text) STORED,
    power character varying GENERATED ALWAYS AS ((feature->'properties'->>'power')::text) STORED,
    highway character varying GENERATED ALWAYS AS ((feature->'properties'->>'highway')::text) STORED,
    amenity character varying GENERATED ALWAYS AS ((feature->'properties'->>'amenity')::text) STORED,
    barrier character varying GENERATED ALWAYS AS ((feature->'properties'->>'barrier')::text) STORED,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    requested_by character varying(40) COLLATE pg_catalog."default",
    CONSTRAINT "PK_point_id" PRIMARY KEY (id),
    CONSTRAINT unq_dataset_point_id UNIQUE (tdei_dataset_id, point_id)
);
CREATE INDEX IF NOT EXISTS idx_point_location ON content.extension_point USING gist (point_loc);

CREATE TABLE IF NOT EXISTS content.extension_polygon
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    tdei_dataset_id character varying(40) COLLATE pg_catalog."default" NOT NULL,
    feature json NOT NULL,
    polygon_loc geometry(Polygon, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_GeomFromGeoJSON(feature->>'geometry'), 4326)) STORED,
    polygon_id bigint GENERATED ALWAYS AS ((feature->'properties'->>'_id')::bigint) STORED,
    building character varying GENERATED ALWAYS AS ((feature->'properties'->>'building')::text) STORED,
    name character varying GENERATED ALWAYS AS ((feature->'properties'->>'name')::text) STORED,
    opening_hours character varying GENERATED ALWAYS AS ((feature->'properties'->>'opening_hours')::text) STORED,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    requested_by character varying(40) COLLATE pg_catalog."default",
    CONSTRAINT "PK_polygon_id" PRIMARY KEY (id),
    CONSTRAINT unq_dataset_polygon_id UNIQUE (tdei_dataset_id, polygon_id)
);
CREATE INDEX IF NOT EXISTS idx_polygon_location ON content.extension_polygon USING gist (polygon_loc);

CREATE TABLE IF NOT EXISTS content.extension_line
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    tdei_dataset_id character varying(40) COLLATE pg_catalog."default" NOT NULL,
    feature json NOT NULL,
    line_loc geometry(LineString, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_GeomFromGeoJSON(feature->>'geometry'), 4326)) STORED,
    line_id bigint GENERATED ALWAYS AS ((feature->'properties'->>'_id')::bigint) STORED,
    barrier character varying GENERATED ALWAYS AS ((feature->'properties'->>'barrier')::text) STORED,
    length character varying GENERATED ALWAYS AS ((feature->'properties'->>'length')::text) STORED,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    requested_by character varying(40) COLLATE pg_catalog."default",
    CONSTRAINT "PK_line_id" PRIMARY KEY (id),
    CONSTRAINT unq_dataset_line_id UNIQUE (tdei_dataset_id, line_id)
);
CREATE INDEX IF NOT EXISTS idx_line_location ON content.extension_line USING gist (line_loc);

-- zone_extract_w_id: TDEI's function behind content.zone.node_ids (the zone's
-- _w_id node ids as bigint[]). As in the TDEI DDL, except CREATE OR REPLACE so
-- this file can be re-run.
CREATE OR REPLACE FUNCTION content.zone_extract_w_id(json_data JSON) RETURNS bigint[]
AS $$
    SELECT ARRAY(
        SELECT CAST(value AS bigint)
        FROM JSON_ARRAY_ELEMENTS_TEXT(json_data->'properties'->'_w_id') AS elements(value)
    );
$$ LANGUAGE SQL IMMUTABLE;

CREATE TABLE IF NOT EXISTS content.zone
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    tdei_dataset_id character varying(40) COLLATE pg_catalog."default" NOT NULL,
    feature json NOT NULL,
    zone_loc geometry(Polygon, 4326) GENERATED ALWAYS AS (ST_SetSRID(ST_GeomFromGeoJSON(feature->>'geometry'), 4326)) STORED,
    zone_id bigint GENERATED ALWAYS AS ((feature->'properties'->>'_id')::bigint) STORED,
    node_ids bigint[] GENERATED ALWAYS AS (content.zone_extract_w_id(feature)) STORED,
    name character varying GENERATED ALWAYS AS ((feature->'properties'->>'name')::text) STORED,
    description character varying GENERATED ALWAYS AS ((feature->'properties'->>'description')::text) STORED,
    highway character varying GENERATED ALWAYS AS ((feature->'properties'->>'highway')::text) STORED,
    surface character varying GENERATED ALWAYS AS ((feature->'properties'->>'surface')::text) STORED,
    foot character varying GENERATED ALWAYS AS ((feature->'properties'->>'foot')::text) STORED,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    requested_by character varying(40) COLLATE pg_catalog."default",
    CONSTRAINT "PK_zone_id" PRIMARY KEY (id),
    CONSTRAINT unq_dataset_zone_id UNIQUE (tdei_dataset_id, zone_id)
);
CREATE INDEX IF NOT EXISTS idx_zone_location ON content.zone USING gist (zone_loc);
