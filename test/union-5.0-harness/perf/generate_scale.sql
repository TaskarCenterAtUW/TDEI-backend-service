-- =============================================================================
-- perf/generate_scale.sql — a synthetic city for union 5.0 scale tests.
--
--   psql "<conn>" -v ds1=scale-ds1 -v ds2=scale-ds2 -v side=100 -f perf/generate_scale.sql
--
-- A grid of side × side city blocks, 100 m apart, written into the six TDEI
-- content tables as two datasets (any rows already under those ids are
-- deleted first). Per block, in each dataset:
--
--   roads       residential streets between intersections (2 per block)
--   sidewalks   4 sides inset 6 m from the street, each split at its middle
--               (8 edges; DS2 leaves a third of the sides unsplit)
--   crossings   ~4 per block, kerb-to-kerb across each street, crossing the road
--   footways    a diagonal path through 1 block in 7 (DS2) / 1 in 11 (DS1)
--   extensions  a building (polygon), a bench and a hydrant (points); a fence
--               (line) in 1 block in 4; a pedestrian zone in 1 block in 10
--
-- Edges per block ≈ 14, nodes ≈ 9: side 100 → ~140k edges and ~90k nodes per
-- dataset; side 270 → ~1M edges; side 380 → ~2M edges.
--
-- DS2 is the same city shifted 30 % east (so 70 % of it overlaps DS1, the
-- rest is new), every location moved by up to ±0.25 m (so snap and dedup have
-- work), with different gaps: DS1 lacks 1 crossing in 7 and 1 sidewalk side in
-- 20; DS2 lacks 1 crossing and 1 sidewalk side in 20 and 1 extension feature
-- in 10. Half of DS2's block corners (a quarter of DS1's) are barrier=kerb,
-- which never move, so connect has gaps to close. Everything is deterministic.
-- =============================================================================
\set ON_ERROR_STOP on
SET client_min_messages = warning;
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';

\echo 'generating side' :side 'into' :ds1 '+' :ds2

BEGIN;
DELETE FROM content.node              WHERE tdei_dataset_id IN (:'ds1', :'ds2');
DELETE FROM content.edge              WHERE tdei_dataset_id IN (:'ds1', :'ds2');
DELETE FROM content.zone              WHERE tdei_dataset_id IN (:'ds1', :'ds2');
DELETE FROM content.extension_point   WHERE tdei_dataset_id IN (:'ds1', :'ds2');
DELETE FROM content.extension_line    WHERE tdei_dataset_id IN (:'ds1', :'ds2');
DELETE FROM content.extension_polygon WHERE tdei_dataset_id IN (:'ds1', :'ds2');

-- deterministic pseudo-random integer ≥ 0
CREATE FUNCTION pg_temp.h(VARIADIC a int[]) RETURNS int LANGUAGE sql IMMUTABLE AS
$$ SELECT abs(hashtext(array_to_string(a, ','))) $$;
-- metres from the origin → [lon, lat] on the 7 dp grid; DS2 moves every
-- location by up to ±0.25 m (the same location always moves the same way)
CREATE FUNCTION pg_temp.ll(d int, x float8, y float8) RETURNS float8[] LANGUAGE sql IMMUTABLE AS $$
    SELECT ARRAY[round((-123.10 + (x + jx) / (111111.0 * cos(radians(44.63))))::numeric, 7)::float8,
                 round((44.63 + (y + jy) / 111111.0)::numeric, 7)::float8]
    FROM (SELECT CASE WHEN d = 2 THEN (pg_temp.h(11, round(x * 100)::int, round(y * 100)::int) % 1000) / 2000.0 - 0.25 ELSE 0 END AS jx,
                 CASE WHEN d = 2 THEN (pg_temp.h(12, round(x * 100)::int, round(y * 100)::int) % 1000) / 2000.0 - 0.25 ELSE 0 END AS jy) j
$$;

CREATE TEMP TABLE g_par AS SELECT :side::int AS s, round(:side * 0.3)::int AS off, 100.0::float8 AS b, 6.0::float8 AS w;

-- blocks of each dataset: DS1 columns 0 .. s-1, DS2 columns off .. off+s-1
CREATE TEMP TABLE g_blk AS
SELECT dd.d, i, j,
       ARRAY[i * p.b + p.w, (i + 1) * p.b - p.w, (i + 1) * p.b - p.w, i * p.b + p.w] AS cx,   -- SW SE NE NW
       ARRAY[j * p.b + p.w, j * p.b + p.w, (j + 1) * p.b - p.w, (j + 1) * p.b - p.w] AS cy,
       CASE WHEN dd.d = 1 THEN 0 ELSE p.off END AS i0
FROM g_par p, (VALUES (1), (2)) dd(d),
     generate_series(CASE WHEN dd.d = 1 THEN 0 ELSE p.off END, CASE WHEN dd.d = 1 THEN 0 ELSE p.off END + p.s - 1) i,
     generate_series(0, p.s - 1) j;
CREATE INDEX ON g_blk (d, i, j);
ANALYZE g_blk;

CREATE TEMP TABLE g_e (d int, kind text, x0 float8, y0 float8, xm float8, ym float8, x1 float8, y1 float8, tags jsonb);

-- roads: horizontal (street j) and vertical (avenue i)
INSERT INTO g_e
SELECT dd.d, 'road', i * p.b, j * p.b, NULL::float8, NULL::float8, (i + 1) * p.b, j * p.b,
       jsonb_build_object('highway', 'residential', 'name', 'Street ' || j, 'surface', 'asphalt')
FROM g_par p, (VALUES (1), (2)) dd(d),
     generate_series(CASE WHEN dd.d = 1 THEN 0 ELSE p.off END, CASE WHEN dd.d = 1 THEN 0 ELSE p.off END + p.s - 1) i,
     generate_series(0, p.s) j
UNION ALL
SELECT dd.d, 'road', i * p.b, j * p.b, NULL::float8, NULL::float8, i * p.b, (j + 1) * p.b,
       jsonb_build_object('highway', 'residential', 'name', 'Avenue ' || i, 'surface', 'asphalt')
FROM g_par p, (VALUES (1), (2)) dd(d),
     generate_series(CASE WHEN dd.d = 1 THEN 0 ELSE p.off END, CASE WHEN dd.d = 1 THEN 0 ELSE p.off END + p.s) i,
     generate_series(0, p.s - 1) j;

-- sidewalks: side k runs corner a → corner c via its middle m; split at m,
-- except a third of DS2's sides (m is then a plain vertex)
INSERT INTO g_e
SELECT b.d, 'sidewalk', s.ax, s.ay, NULL::float8, NULL::float8, s.mx, s.my, s.tags FROM g_blk b, generate_series(0, 3) k,
     LATERAL (SELECT b.cx[k + 1] AS ax, b.cy[k + 1] AS ay, b.cx[(k + 1) % 4 + 1] AS cx_, b.cy[(k + 1) % 4 + 1] AS cy_,
                     (b.cx[k + 1] + b.cx[(k + 1) % 4 + 1]) / 2 AS mx, (b.cy[k + 1] + b.cy[(k + 1) % 4 + 1]) / 2 AS my,
                     '{"highway":"footway","footway":"sidewalk","surface":"concrete","width":1.8}'::jsonb AS tags) s
WHERE pg_temp.h(b.d, b.i, b.j, k) % 20 <> 0 AND NOT (b.d = 2 AND pg_temp.h(3, b.i, b.j, k) % 3 = 0)
UNION ALL
SELECT b.d, 'sidewalk', s.mx, s.my, NULL::float8, NULL::float8, s.cx_, s.cy_, s.tags FROM g_blk b, generate_series(0, 3) k,
     LATERAL (SELECT b.cx[(k + 1) % 4 + 1] AS cx_, b.cy[(k + 1) % 4 + 1] AS cy_,
                     (b.cx[k + 1] + b.cx[(k + 1) % 4 + 1]) / 2 AS mx, (b.cy[k + 1] + b.cy[(k + 1) % 4 + 1]) / 2 AS my,
                     '{"highway":"footway","footway":"sidewalk","surface":"concrete","width":1.8}'::jsonb AS tags) s
WHERE pg_temp.h(b.d, b.i, b.j, k) % 20 <> 0 AND NOT (b.d = 2 AND pg_temp.h(3, b.i, b.j, k) % 3 = 0)
UNION ALL
SELECT b.d, 'sidewalk', s.ax, s.ay, s.mx, s.my, s.cx_, s.cy_, s.tags FROM g_blk b, generate_series(0, 3) k,
     LATERAL (SELECT b.cx[k + 1] AS ax, b.cy[k + 1] AS ay, b.cx[(k + 1) % 4 + 1] AS cx_, b.cy[(k + 1) % 4 + 1] AS cy_,
                     (b.cx[k + 1] + b.cx[(k + 1) % 4 + 1]) / 2 AS mx, (b.cy[k + 1] + b.cy[(k + 1) % 4 + 1]) / 2 AS my,
                     '{"highway":"footway","footway":"sidewalk","surface":"concrete","width":1.8}'::jsonb AS tags) s
WHERE pg_temp.h(b.d, b.i, b.j, k) % 20 <> 0 AND b.d = 2 AND pg_temp.h(3, b.i, b.j, k) % 3 = 0;

-- crossings: from a block to its north and east neighbours, kerb to kerb across
-- the street, through a vertex on the road line (no node there)
INSERT INTO g_e
SELECT b.d, 'crossing', x.x0, x.y0, x.xm, x.ym, x.x1, x.y1,
       '{"highway":"footway","footway":"crossing","crossing:markings":"zebra"}'::jsonb
FROM g_blk b
JOIN g_blk n ON n.d = b.d AND n.i = b.i AND n.j = b.j + 1
CROSS JOIN LATERAL (VALUES
    (0, b.cx[4], b.cy[4], b.cx[4], (b.j + 1) * 100.0, n.cx[1], n.cy[1]),      -- NW of b → SW of n
    (1, b.cx[3], b.cy[3], b.cx[3], (b.j + 1) * 100.0, n.cx[2], n.cy[2]))     -- NE of b → SE of n
    x(c, x0, y0, xm, ym, x1, y1)
WHERE pg_temp.h(CASE WHEN b.d = 1 THEN 4 ELSE 5 END, b.i, b.j, x.c) % CASE WHEN b.d = 1 THEN 7 ELSE 20 END <> 0
UNION ALL
SELECT b.d, 'crossing', x.x0, x.y0, x.xm, x.ym, x.x1, x.y1,
       '{"highway":"footway","footway":"crossing","crossing:markings":"zebra"}'::jsonb
FROM g_blk b
JOIN g_blk n ON n.d = b.d AND n.i = b.i + 1 AND n.j = b.j
CROSS JOIN LATERAL (VALUES
    (2, b.cx[2], b.cy[2], (b.i + 1) * 100.0, b.cy[2], n.cx[1], n.cy[1]),      -- SE of b → SW of n
    (3, b.cx[3], b.cy[3], (b.i + 1) * 100.0, b.cy[3], n.cx[4], n.cy[4]))     -- NE of b → NW of n
    x(c, x0, y0, xm, ym, x1, y1)
WHERE pg_temp.h(CASE WHEN b.d = 1 THEN 4 ELSE 5 END, b.i, b.j, x.c) % CASE WHEN b.d = 1 THEN 7 ELSE 20 END <> 0;

-- footways: a diagonal path across some blocks (different blocks in each dataset)
INSERT INTO g_e
SELECT b.d, 'footway', b.cx[1], b.cy[1], NULL::float8, NULL::float8, b.cx[3], b.cy[3], '{"highway":"footway","surface":"asphalt"}'::jsonb
FROM g_blk b WHERE b.d = 2 AND pg_temp.h(6, b.i, b.j) % 7 = 0
UNION ALL
SELECT b.d, 'footway', b.cx[2], b.cy[2], NULL::float8, NULL::float8, b.cx[4], b.cy[4], '{"highway":"footway","surface":"asphalt"}'::jsonb
FROM g_blk b WHERE b.d = 1 AND pg_temp.h(7, b.i, b.j) % 11 = 0;

-- nodes: every edge end; block corners may be kerbs
CREATE TEMP TABLE g_n AS
SELECT d, x, y, (CASE WHEN d = 1 THEN 10000000 ELSE 20000000 END + row_number() OVER (PARTITION BY d ORDER BY x, y))::bigint AS nid,
       CASE WHEN x::numeric % 100 IN (6, 94) AND y::numeric % 100 IN (6, 94)
                 AND pg_temp.h(CASE WHEN d = 1 THEN 8 ELSE 9 END, x::int, y::int) % CASE WHEN d = 1 THEN 4 ELSE 2 END = 0
            THEN '{"barrier":"kerb","kerb":"lowered"}'::jsonb ELSE '{}'::jsonb END AS tags
FROM (SELECT d, x0 AS x, y0 AS y FROM g_e UNION SELECT d, x1, y1 FROM g_e) p;
CREATE INDEX ON g_n (d, x, y);
ANALYZE g_n;

INSERT INTO content.node (tdei_dataset_id, feature)
SELECT CASE WHEN d = 1 THEN :'ds1' ELSE :'ds2' END,
       json_build_object('type', 'Feature',
                         'geometry', json_build_object('type', 'Point', 'coordinates', pg_temp.ll(d, x, y)),
                         'properties', jsonb_build_object('_id', nid::text) || tags)
FROM g_n;

INSERT INTO content.edge (tdei_dataset_id, feature)
SELECT CASE WHEN e.d = 1 THEN :'ds1' ELSE :'ds2' END,
       json_build_object('type', 'Feature',
                         'geometry', json_build_object('type', 'LineString', 'coordinates',
                             CASE WHEN e.xm IS NULL THEN json_build_array(pg_temp.ll(e.d, e.x0, e.y0), pg_temp.ll(e.d, e.x1, e.y1))
                                  ELSE json_build_array(pg_temp.ll(e.d, e.x0, e.y0), pg_temp.ll(e.d, e.xm, e.ym), pg_temp.ll(e.d, e.x1, e.y1)) END),
                         'properties', jsonb_build_object('_id', ((CASE WHEN e.d = 1 THEN 40000000 ELSE 50000000 END)
                                                                  + row_number() OVER (PARTITION BY e.d ORDER BY e.kind, e.x0, e.y0, e.x1, e.y1))::text,
                                                          '_u_id', u.nid::text, '_v_id', v.nid::text) || e.tags)
FROM g_e e
JOIN g_n u ON u.d = e.d AND u.x = e.x0 AND u.y = e.y0
JOIN g_n v ON v.d = e.d AND v.x = e.x1 AND v.y = e.y1;

-- extension features (DS2 lacks 1 in 10)
INSERT INTO content.extension_polygon (tdei_dataset_id, feature)
SELECT CASE WHEN b.d = 1 THEN :'ds1' ELSE :'ds2' END,
       json_build_object('type', 'Feature',
           'geometry', json_build_object('type', 'Polygon', 'coordinates', json_build_array(json_build_array(
               pg_temp.ll(b.d, b.i * 100.0 + 15, b.j * 100.0 + 15), pg_temp.ll(b.d, b.i * 100.0 + 85, b.j * 100.0 + 15),
               pg_temp.ll(b.d, b.i * 100.0 + 85, b.j * 100.0 + 85), pg_temp.ll(b.d, b.i * 100.0 + 15, b.j * 100.0 + 85),
               pg_temp.ll(b.d, b.i * 100.0 + 15, b.j * 100.0 + 15)))),
           'properties', json_build_object('_id', (CASE WHEN b.d = 1 THEN 130000000 ELSE 135000000 END
                                                   + row_number() OVER (PARTITION BY b.d ORDER BY b.i, b.j))::text,
                                           'building', 'yes', 'name', 'Building ' || b.i || '-' || b.j))
FROM g_blk b WHERE b.d = 1 OR pg_temp.h(13, b.i, b.j) % 10 <> 0;

INSERT INTO content.extension_point (tdei_dataset_id, feature)
SELECT CASE WHEN b.d = 1 THEN :'ds1' ELSE :'ds2' END,
       json_build_object('type', 'Feature',
           'geometry', json_build_object('type', 'Point', 'coordinates', pg_temp.ll(b.d, p.x, p.y)),
           'properties', jsonb_build_object('_id', (CASE WHEN b.d = 1 THEN 110000000 ELSE 115000000 END
                                                    + row_number() OVER (PARTITION BY b.d ORDER BY b.i, b.j, p.k))::text) || p.tags)
FROM g_blk b
CROSS JOIN LATERAL (VALUES (1, b.i * 100.0 + 50, b.j * 100.0 + 8, '{"amenity":"bench"}'::jsonb),
                           (2, b.i * 100.0 + 9, b.j * 100.0 + 9, '{"emergency":"fire_hydrant"}'::jsonb)) p(k, x, y, tags)
WHERE b.d = 1 OR pg_temp.h(14, b.i, b.j, p.k) % 10 <> 0;

INSERT INTO content.extension_line (tdei_dataset_id, feature)
SELECT CASE WHEN b.d = 1 THEN :'ds1' ELSE :'ds2' END,
       json_build_object('type', 'Feature',
           'geometry', json_build_object('type', 'LineString', 'coordinates', json_build_array(
               pg_temp.ll(b.d, b.i * 100.0 + 20, b.j * 100.0 + 90), pg_temp.ll(b.d, b.i * 100.0 + 80, b.j * 100.0 + 90))),
           'properties', json_build_object('_id', (CASE WHEN b.d = 1 THEN 120000000 ELSE 125000000 END
                                                   + row_number() OVER (PARTITION BY b.d ORDER BY b.i, b.j))::text,
                                           'barrier', 'fence'))
FROM g_blk b WHERE pg_temp.h(15, b.i, b.j) % 4 = 0 AND (b.d = 1 OR pg_temp.h(17, b.i, b.j) % 10 <> 0);

-- zones: a pedestrian plaza on the block's four corner nodes
INSERT INTO content.zone (tdei_dataset_id, feature)
SELECT CASE WHEN b.d = 1 THEN :'ds1' ELSE :'ds2' END,
       json_build_object('type', 'Feature',
           'geometry', json_build_object('type', 'Polygon', 'coordinates', json_build_array(json_build_array(
               pg_temp.ll(b.d, b.cx[1], b.cy[1]), pg_temp.ll(b.d, b.cx[2], b.cy[2]), pg_temp.ll(b.d, b.cx[3], b.cy[3]),
               pg_temp.ll(b.d, b.cx[4], b.cy[4]), pg_temp.ll(b.d, b.cx[1], b.cy[1])))),
           'properties', json_build_object('_id', (CASE WHEN b.d = 1 THEN 100000000 ELSE 105000000 END
                                                   + row_number() OVER (PARTITION BY b.d ORDER BY b.i, b.j))::text,
                                           'highway', 'pedestrian', 'name', 'Plaza ' || b.i || '-' || b.j,
                                           '_w_id', json_build_array(n1.nid::text, n2.nid::text, n3.nid::text, n4.nid::text, n1.nid::text)))
FROM g_blk b
JOIN g_n n1 ON n1.d = b.d AND n1.x = b.cx[1] AND n1.y = b.cy[1]
JOIN g_n n2 ON n2.d = b.d AND n2.x = b.cx[2] AND n2.y = b.cy[2]
JOIN g_n n3 ON n3.d = b.d AND n3.x = b.cx[3] AND n3.y = b.cy[3]
JOIN g_n n4 ON n4.d = b.d AND n4.x = b.cx[4] AND n4.y = b.cy[4]
WHERE pg_temp.h(16, b.i, b.j) % 10 = 0 AND (b.d = 1 OR pg_temp.h(18, b.i, b.j) % 10 <> 0);
COMMIT;

ANALYZE content.node; ANALYZE content.edge; ANALYZE content.zone;
ANALYZE content.extension_point; ANALYZE content.extension_line; ANALYZE content.extension_polygon;

SELECT t AS "table", count(*) FILTER (WHERE ds = :'ds1') AS ds1, count(*) FILTER (WHERE ds = :'ds2') AS ds2 FROM (
    SELECT 'node' AS t, tdei_dataset_id AS ds FROM content.node WHERE tdei_dataset_id IN (:'ds1', :'ds2')
    UNION ALL SELECT 'edge', tdei_dataset_id FROM content.edge WHERE tdei_dataset_id IN (:'ds1', :'ds2')
    UNION ALL SELECT 'zone', tdei_dataset_id FROM content.zone WHERE tdei_dataset_id IN (:'ds1', :'ds2')
    UNION ALL SELECT 'point', tdei_dataset_id FROM content.extension_point WHERE tdei_dataset_id IN (:'ds1', :'ds2')
    UNION ALL SELECT 'line', tdei_dataset_id FROM content.extension_line WHERE tdei_dataset_id IN (:'ds1', :'ds2')
    UNION ALL SELECT 'polygon', tdei_dataset_id FROM content.extension_polygon WHERE tdei_dataset_id IN (:'ds1', :'ds2')) x
GROUP BY t ORDER BY t;
