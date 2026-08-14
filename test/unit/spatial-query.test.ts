import { InputException } from '../../src/exceptions/http/http-exceptions';
import { AssignmentMethod, SpatialJoinRequestParams } from '../../src/service/interface/interfaces';
describe('BackendService', () => {
    let spatialServiceParams: SpatialJoinRequestParams;

    beforeEach(() => {
        spatialServiceParams = SpatialJoinRequestParams.from({
            target_dimension: 'node',
            source_dimension: 'node',
            aggregate: ['ARRAY_AGG(ext:ramp_width_update_mt) as ramp_width_insert_mt', 'ARRAY_AGG(ext:unit_id) as unit_id', 'ARRAY_AGG(ext:condition) as conditions'],
            join_condition: `WITH candidates AS (
                            SELECT
                                s.id   AS update,
                                p.id   AS pole_id,
                                ST_LineMerge(s.geom) AS line_geom,
                                p.geom AS pole_geom
                            FROM sidewalks s
                            JOIN poles p
                                ON ST_DWithin(s.geom, p.geom, 2)         
                            WHERE (p.tags->>'amenity') = 'light_pole'
                            ),
                            located AS (
                            SELECT
                                update,
                                pole_id,
                                ST_LineLocatePoint(line_geom, pole_geom)      AS frac,
                                ST_LineInterpolatePoint(line_geom,             
                                                        ST_LineLocatePoint(line_geom, pole_geom)) AS proj_pt
                            FROM candidates
                            )
                            SELECT *
                            FROM located
                            WHERE frac BETWEEN 0.2 AND 0.8;
            `,
            join_filter_target: "frac BETWEEN 0.2 AND 0.8",
            join_filter_source: "frac BETWEEN 0.2 AND 0.8",
            target_dataset_id: '7d6ae05c-8928-4727-bb0d-4717e46242f1',
            source_dataset_id: '80296cbe-53ac-463b-b5f6-dad8b8e5e788'
        });
    });

    describe('buildSpatialQuery', () => {

        it('should build the spatial query correctly for edge target and extension source', () => {
            // Call the method under test
            const query = spatialServiceParams.buildSpatialQuery();
            console.log(query.join(';').toString());
            // Assertions
            expect(query.toString()).toContain('SELECT');
            expect(query.toString()).toContain('FROM');
            expect(query.toString()).toContain('LEFT JOIN');
            expect(query.toString()).toContain('WHERE');
            expect(query.toString()).toContain('GROUP BY');
            expect(query.toString()).not.toContain('geometry_target');
            expect(query.toString()).not.toContain('geometry_source');
            //reset assignment logic
        });

        it('should build the spatial query correctly for default query with EXCLUSIVE assignment_logic', () => {
            // Call the method under test
            spatialServiceParams.assignment_method = AssignmentMethod.EXCLUSIVE;
            const query = spatialServiceParams.buildSpatialQuery();
            console.log(query.join(';').toString());
            // Assertions
            expect(query.toString()).toContain('SELECT');
            expect(query.toString()).toContain('FROM');
            expect(query.toString()).toContain('LEFT JOIN');
            expect(query.toString()).toContain('WHERE');
            expect(query.toString()).toContain('GROUP BY');
            expect(query.toString()).toContain('tmp_final_assign');
            expect(query.toString()).not.toContain('geometry_target');
            expect(query.toString()).not.toContain('geometry_source');

            //reset assignment logic
            spatialServiceParams.assignment_method = AssignmentMethod.DEFAULT;
        });

        it('should build the spatial query correctly for default query with SHARED assignment_logic', () => {
            // Call the method under test
            spatialServiceParams.assignment_method = AssignmentMethod.SHARED;
            const query = spatialServiceParams.buildSpatialQuery();
            console.log(query.join(';').toString());
            // Assertions
            expect(query.toString()).toContain('SELECT');
            expect(query.toString()).toContain('FROM');
            expect(query.toString()).toContain('LEFT JOIN');
            expect(query.toString()).toContain('WHERE');
            expect(query.toString()).toContain('GROUP BY');
            expect(query.toString()).toContain('tmp_final_assign');
            expect(query.toString()).not.toContain('geometry_target');
            expect(query.toString()).not.toContain('geometry_source');

            //reset assignment logic
            spatialServiceParams.assignment_method = AssignmentMethod.DEFAULT;
        });

        it('should build the spatial query correctly for aggregate _id , _v_id, _u_id columns for edge source with db column', () => {
            // Call the method under test
            spatialServiceParams = SpatialJoinRequestParams.from({
                target_dimension: 'edge',
                source_dimension: 'edge',
                aggregate: ['ARRAY_AGG(_id) as SDOT_curb_ramp_id',
                    'ARRAY_AGG(_u_id) as source_id',
                    'ARRAY_AGG(_v_id) as dest_id'],
                join_condition: 'ST_Intersects(ST_Buffer(geometry_target, 2), geometry_source) and degrees( ST_Angle(geometry_target, geometry_source) ) < 30 ',
                join_filter_target: "",
                join_filter_source: "",
                target_dataset_id: '2e7b2904-f2e7-4784-b18a-aae31be0b1c0',
                source_dataset_id: 'a3afe3cf-8db5-4898-a602-8e5e49175c13'
            });
            const query = spatialServiceParams.buildSpatialQuery();
            console.log(query);
            // Assertions
            expect(query.toString()).toContain('SELECT');
            expect(query.toString()).toContain('FROM');
            expect(query.toString()).toContain('LEFT JOIN');
            expect(query.toString()).toContain('WHERE');
            expect(query.toString()).toContain('GROUP BY');
            expect(query.toString()).toContain('edge_id');
            expect(query.toString()).not.toContain('geometry_target');
            expect(query.toString()).not.toContain('geometry_source');
            expect(query.toString()).toContain('_u_id');
            expect(query.toString()).toContain('_v_id');
        });

        it('should build the spatial query correctly for aggregate _id , _v_id, _u_id columns for edge source with db column', () => {
            // Call the method under test
            spatialServiceParams = SpatialJoinRequestParams.from({
                target_dimension: 'edge',
                source_dimension: 'point',
                aggregate: ['ARRAY_AGG(_id) as SDOT_curb_ramp_id'],
                join_condition: 'ST_Intersects(ST_Buffer(geometry_target, 2), geometry_source) and degrees( ST_Angle(geometry_target, geometry_source) ) < 30 ',
                join_filter_target: "",
                join_filter_source: "",
                target_dataset_id: '2e7b2904-f2e7-4784-b18a-aae31be0b1c0',
                source_dataset_id: 'a3afe3cf-8db5-4898-a602-8e5e49175c13'
            });
            const query = spatialServiceParams.buildSpatialQuery();
            console.log(query);
            // Assertions
            expect(query.toString()).toContain('SELECT');
            expect(query.toString()).toContain('FROM');
            expect(query.toString()).toContain('LEFT JOIN');
            expect(query.toString()).toContain('WHERE');
            expect(query.toString()).toContain('GROUP BY');
            expect(query.toString()).toContain('_id');
            expect(query.toString()).not.toContain('geometry_target');
            expect(query.toString()).not.toContain('geometry_source');
        });

        it('should build the spatial query correctly for complex multiple join conditions', () => {
            // Call the method under test
            spatialServiceParams = SpatialJoinRequestParams.from({
                target_dimension: 'edge',
                source_dimension: 'edge',
                aggregate: ['ARRAY_AGG(point_id) as SDOT_curb_ramp_id',
                    'ARRAY_AGG(ext:unit_id) as SDOT_curb_ramp_unit_id',
                    'ARRAY_AGG(ramp_width_mt) as SDOT_curb_ramp_width',
                    'ARRAY_AGG(ada_compliant) as SDOT_curb_ramp_ada_compliant',
                    'ARRAY_AGG(ext:description) as SDOT_curb_ramp_desc',
                    'ARRAY_AGG(ext:sw_st_side) as SDOT_curb_ramp_sw_st_side',
                    'ARRAY_AGG(ext:direction) as SDOT_curb_ramp_direction',
                    'ARRAY_AGG(ext:condition) as SDOT_curb_ramp_condition',
                    'ARRAY_AGG(ext:style) as SDOT_curb_ramp_style'],
                join_condition: 'ST_Intersects(ST_Buffer(geometry_target, 2), geometry_source) and degrees( ST_Angle(geometry_target, geometry_source) ) < 30 ',
                join_filter_target: "",
                join_filter_source: "",
                target_dataset_id: '2e7b2904-f2e7-4784-b18a-aae31be0b1c0',
                source_dataset_id: 'a3afe3cf-8db5-4898-a602-8e5e49175c13'
            });
            const query = spatialServiceParams.buildSpatialQuery();
            console.log(query);
            // Assertions
            expect(query.toString()).toContain('SELECT');
            expect(query.toString()).toContain('FROM');
            expect(query.toString()).toContain('LEFT JOIN');
            expect(query.toString()).toContain('WHERE');
            expect(query.toString()).toContain('GROUP BY');
            expect(query.toString()).not.toContain('geometry_target');
            expect(query.toString()).not.toContain('geometry_source');
        });

        it('should build the spatial query correctly for edge target and point source', () => {
            spatialServiceParams = SpatialJoinRequestParams.from({
                target_dimension: 'edge',
                source_dimension: 'point',
                aggregate: ['ARRAY_AGG(highway) as lamps'],
                // join_condition: 'ST_Intersects(ST_Buffer(geometry_target, 5), geometry_source)',
                join_condition: 'ST_Intersects(ST_Buffer(geometry_target, 5), geometry_source)',
                join_filter_target: "highway='footway'",
                join_filter_source: "highway='street_lamp'",
                target_dataset_id: 'ddc9a128-1afb-4be3-a7dc-52bd201a6ebe',
                source_dataset_id: '0880b241-3c4c-4900-8005-7c99b1497641'
            });

            // Call the method under test
            const query = spatialServiceParams.buildSpatialQuery();
            console.log(query);
            // Assertions
            expect(query.toString()).toContain('SELECT');
            expect(query.toString()).toContain('FROM');
            expect(query.toString()).toContain('LEFT JOIN');
            expect(query.toString()).toContain('WHERE');
            expect(query.toString()).toContain('GROUP BY');
        });

        it('should throw an InputException for invalid target dimension', () => {
            // Set up the test data
            spatialServiceParams.target_dimension = 'invalid_dimension';
            spatialServiceParams.source_dimension = 'node';

            // Call the method under test
            expect(() => spatialServiceParams.buildSpatialQuery()).toThrow(InputException);
        });

        it('should throw an InputException for invalid source dimension', () => {
            // Set up the test data
            spatialServiceParams.target_dimension = 'edge';
            spatialServiceParams.source_dimension = 'invalid_dimension';

            // Call the method under test
            expect(() => spatialServiceParams.buildSpatialQuery()).toThrow(InputException);
        });

        it('should throw an InputException for invalid aggregate syntax', () => {
            // Set up the test data
            spatialServiceParams.aggregate = ['array_agg highway)'];

            // Call the method under test
            expect(() => spatialServiceParams.buildSpatialQuery()).toThrow(InputException);
        });

        describe('SQL injection protection', () => {
            const expectRejected = (overrides: Partial<{
                join_condition: string;
                join_filter_target: string;
                join_filter_source: string;
                aggregate: string[];
            }>) => {
                Object.assign(spatialServiceParams, overrides);
                expect(() => spatialServiceParams.buildSpatialQuery()).toThrow(InputException);
            };

            const expectAccepted = (overrides: Partial<{
                join_condition: string;
                join_filter_target: string;
                join_filter_source: string;
                aggregate: string[];
            }>) => {
                Object.assign(spatialServiceParams, overrides);
                expect(() => spatialServiceParams.buildSpatialQuery()).not.toThrow();
            };

            describe('allowed legitimate expressions', () => {
                it('should allow the default CTE join_condition fixture', () => {
                    expect(() => spatialServiceParams.buildSpatialQuery()).not.toThrow();
                });

                it('should allow property names that contain DATE/UPDATE/INSERT as substrings', () => {
                    expectAccepted({
                        join_filter_target: 'updatedat IS NOT NULL',
                        join_filter_source: 'created_at IS NOT NULL',
                        aggregate: [
                            'ARRAY_AGG(updatedat) as updatedat',
                            'ARRAY_AGG(ext:ramp_width_update_mt) as ramp_width_insert_mt'
                        ]
                    });
                });

                it('should allow ext: prefixed property names containing keywords', () => {
                    expectAccepted({
                        aggregate: ['ARRAY_AGG(ext:update) as updates', 'ARRAY_AGG(ext:condition) as conditions']
                    });
                });

                it('should allow PostGIS functions in the join condition', () => {
                    expectAccepted({
                        join_condition: 'ST_Intersects(ST_Buffer(geometry_target, 2), geometry_source) AND ST_Distance(geometry_target, geometry_source) < 10'
                    });
                });

                it('should allow general Postgres functions in filters and aggregates', () => {
                    expectAccepted({
                        join_filter_source: "lower(barrier) = 'kerb' AND round(width) > 2",
                        aggregate: ['JSONB_AGG(jsonb_build_object(barrier)) as barriers']
                    });
                });

                it('should allow casts, IN lists, BETWEEN, LIKE/ILIKE, and NOT', () => {
                    expectAccepted({
                        join_filter_target: "barrier::text = 'kerb' AND width BETWEEN 1 AND 10",
                        join_filter_source: "barrier IN ('kerb','bollard') AND name ILIKE '%ramp%' AND NOT (highway = 'steps')"
                    });
                });

                it('should allow boolean tautology-style filters that are structurally valid expressions', () => {
                    expectAccepted({ join_filter_target: "barrier='kerb' OR 1=1" });
                    expectAccepted({ join_filter_source: "barrier='kerb' OR '1'='1'" });
                    expectAccepted({ join_filter_target: "barrier='x' OR TRUE" });
                });

                it('should allow subqueries, EXISTS, set operations, and CTEs', () => {
                    expectAccepted({ join_filter_target: "barrier IN (SELECT barrier FROM content.node)" });
                    expectAccepted({ join_filter_target: 'EXISTS (SELECT 1 FROM content.node)' });
                    expectAccepted({ join_condition: '(SELECT count(*) FROM content.node) > 0' });
                    expectAccepted({ join_filter_target: 'CASE WHEN (SELECT 1) = 1 THEN true ELSE false END' });
                    expectAccepted({ join_filter_target: 'CAST((SELECT 1) AS int) > 0' });
                    expectAccepted({ join_filter_source: "name = (SELECT name FROM content.node LIMIT 1)" });
                    expectAccepted({ aggregate: ['(SELECT count(*) FROM content.node) as cnt'] });
                    expectAccepted({ join_filter_target: '1=1 UNION SELECT 1' });
                    expectAccepted({ join_filter_target: '1=1 UNION ALL SELECT 1' });
                    expectAccepted({ join_filter_target: '1=1 INTERSECT SELECT 1' });
                    expectAccepted({ join_filter_target: '1=1 EXCEPT SELECT 1' });
                    expectAccepted({ join_filter_target: '1=1 GROUP BY barrier' });
                    expectAccepted({ join_filter_target: '1=1 ORDER BY barrier LIMIT 1' });
                    expectAccepted({ join_filter_target: '1=1 HAVING count(*) > 0' });
                    expectAccepted({
                        join_condition: `WITH candidates AS (
                            SELECT s.id AS update, p.id AS pole_insert_id
                            FROM sidewalks s
                            JOIN poles p ON ST_DWithin(s.geom, p.geom, 2)
                        )
                        SELECT * FROM candidates WHERE update IS NOT NULL;`
                    });
                });

                it('should treat update as a column/alias name, not as an UPDATE statement', () => {
                    expectAccepted({ join_filter_target: 'update IS NOT NULL' });
                    expectAccepted({
                        join_condition: `WITH candidates AS (
                            SELECT s.id AS update FROM sidewalks s
                        ) SELECT * FROM candidates WHERE update IS NOT NULL`
                    });
                    expectRejected({ join_filter_target: "UPDATE content.node SET barrier = 'x'" });
                });

                it('should allow ORDER BY DESC without mistaking it for a DESC statement', () => {
                    expectAccepted({ join_condition: 'SELECT barrier FROM content.node ORDER BY barrier DESC' });
                    expectAccepted({ join_filter_target: '1=1 ORDER BY barrier DESC, updatedat ASC' });
                });

                it('should allow DML keywords that appear only inside string literals', () => {
                    expectAccepted({ join_filter_target: "barrier = 'DELETE FROM content.node'" });
                    expectAccepted({ join_filter_source: "barrier = 'pg_sleep(1)'" });
                });

                it('should allow escaped and backslash bearing string literals', () => {
                    expectAccepted({ join_filter_target: "barrier = E'a\\'b'" });
                    expectAccepted({ join_filter_source: "barrier ~ '\\d+'" });
                    expectAccepted({ join_filter_target: "barrier LIKE'kerb%'" });
                    expectAccepted({ join_filter_source: "barrier = 'it''s a kerb'" });
                });

                it('should allow columns named like statement keywords', () => {
                    expectAccepted({ join_filter_target: 'truncate IS NOT NULL' });
                    expectAccepted({ join_filter_source: 'grant IS NOT NULL AND copy IS NOT NULL' });
                    expectAccepted({ join_filter_target: 'revoke IS NULL AND grant_type IS NOT NULL' });
                    expectAccepted({ join_filter_target: 'cluster IS NOT NULL AND notify IS NULL AND reset IS NULL' });
                    expectAccepted({ join_filter_source: 'comment IS NOT NULL AND listen = 1 AND vacuum_status IS NULL' });
                    expectAccepted({ join_filter_target: 'execute IS NOT NULL AND prepare IS NOT NULL' });
                });
            });

            describe('stacked statements and comment evasion', () => {
                it.each([
                    ['semicolon stacking in filter', { join_filter_target: '1=1; DROP TABLE content.node' }],
                    ['semicolon stacking in join condition', { join_condition: 'ST_DWithin(geometry_target, geometry_source, 4); DROP TABLE content.node' }],
                    ['semicolon stacking in aggregate', { aggregate: ['ARRAY_AGG(highway); DROP TABLE dataset;'] }],
                    ['line comment evasion', { join_filter_target: "barrier='kerb' --" }],
                    ['line comment with trailing payload', { join_filter_target: "barrier='kerb' -- AND 1=0" }],
                    ['block comment tokens', { join_filter_target: "barrier='kerb'/*comment*/" }],
                    ['block comment obfuscation', { join_filter_target: "barrier='kerb'/**/OR/**/1=1" }],
                    ['multi-statement DROP in aggregate (legacy)', { aggregate: ['array_agg(highway); DROP TABLE dataset;'] }],
                ])('should reject %s', (_label, overrides) => {
                    expectRejected(overrides);
                });
            });

            describe('DML and DDL statements', () => {
                it.each([
                    ['DELETE', { join_filter_target: 'DELETE FROM content.node' }],
                    ['UPDATE', { join_filter_target: "UPDATE content.node SET barrier = 'x'" }],
                    ['INSERT', { join_filter_target: "INSERT INTO content.node(barrier) VALUES ('x')" }],
                    ['DROP', { join_condition: 'DROP TABLE content.node' }],
                    ['CREATE', { join_condition: 'CREATE TABLE evil (id int)' }],
                    ['TRUNCATE', { join_condition: 'TRUNCATE content.node' }],
                    ['ALTER', { join_condition: 'ALTER TABLE content.node ADD COLUMN x int' }],
                    ['GRANT', { join_condition: 'GRANT ALL ON content.node TO public' }],
                    ['GRANT with a column level privilege list', { join_condition: 'GRANT SELECT, UPDATE (barrier, kerb) ON TABLE content.node TO some_role' }],
                    ['GRANT split across lines', { join_condition: 'GRANT\n  SELECT\n  ON content.node\n  TO public' }],
                    ['GRANT of a role', { join_condition: 'GRANT admin_role TO app_user' }],
                    ['REVOKE', { join_condition: 'REVOKE ALL ON content.node FROM public' }],
                    ['REVOKE GRANT OPTION FOR', { join_condition: 'REVOKE GRANT OPTION FOR SELECT ON content.node FROM app_user' }],
                    ['GRANT with tabs and carriage returns', { join_condition: 'GRANT\r\n\tALL\tON\tcontent.node\r\n\tTO public' }],
                    ['REVOKE GRANT OPTION FOR split across lines', { join_condition: 'REVOKE\n GRANT\n OPTION\n FOR\n SELECT ON content.node FROM app_user' }],
                    ['bare DROP in aggregate', { aggregate: ['DROP TABLE content.node'] }],
                    ['DML nested in a CTE', { join_condition: 'WITH x AS (UPDATE content.node SET barrier = 1 RETURNING *) SELECT * FROM x' }],
                    ['DELETE split across lines', { join_filter_target: 'DELETE\n\tFROM\n   content.node' }],
                    ['INSERT with irregular spacing', { join_filter_target: "INSERT    INTO\ncontent.node(barrier)\nVALUES ('x')" }],
                    ['UPDATE with newlines and alias', { join_filter_target: "UPDATE\n  content.node AS n\n  SET barrier = 'x'" }],
                    ['DROP split across lines', { join_condition: 'DROP\n  TABLE content.node' }],
                    ['MERGE', { join_condition: 'MERGE INTO content.node t USING content.edge s ON t.id = s.id WHEN MATCHED THEN DELETE' }],
                    ['ALTER SYSTEM', { join_condition: "ALTER SYSTEM SET archive_command = 'x'" }],
                    ['REFRESH MATERIALIZED VIEW', { join_condition: 'REFRESH MATERIALIZED VIEW content.mv' }],
                    ['REINDEX', { join_condition: 'REINDEX TABLE content.node' }],
                    ['VACUUM', { join_condition: 'VACUUM FULL content.node' }],
                    ['CHECKPOINT', { join_condition: 'CHECKPOINT' }],
                    ['CLUSTER', { join_condition: 'CLUSTER content.node USING idx' }],
                    ['LOCK TABLE', { join_condition: 'LOCK TABLE content.node' }],
                    ['COMMENT ON', { join_condition: "COMMENT ON TABLE content.node IS 'x'" }],
                    ['SECURITY LABEL', { join_condition: "SECURITY LABEL ON TABLE content.node IS 'x'" }],
                    ['SELECT INTO a new table', { join_condition: 'SELECT * INTO evil_copy FROM content.node' }],
                    ['IMPORT FOREIGN SCHEMA', { join_condition: 'IMPORT FOREIGN SCHEMA public FROM SERVER s INTO public' }],
                    ['NOTIFY', { join_condition: "NOTIFY channel, 'payload'" }],
                    ['LISTEN', { join_condition: 'LISTEN channel' }],
                    ['RESET', { join_condition: 'RESET search_path' }],
                    ['DISCARD ALL', { join_condition: 'DISCARD ALL' }],
                ])('should reject %s', (_label, overrides) => {
                    expectRejected(overrides);
                });
            });

            describe('dynamic SQL that executes a string literal', () => {
                it.each([
                    ['EXECUTE of a literal', { join_condition: "execute 'pg_sleep(1)'" }],
                    ['EXECUTE IMMEDIATE', { join_condition: "EXECUTE IMMEDIATE 'DROP TABLE content.node'" }],
                    ['EXECUTE of a dollar quoted block', { join_condition: 'EXECUTE $$ DROP TABLE content.node $$' }],
                    ['EXECUTE of a prepared statement', { join_condition: 'EXECUTE stmt_name' }],
                    ['PREPARE', { join_condition: 'PREPARE evil AS SELECT 1' }],
                    ['DO block', { join_condition: 'DO $$ BEGIN PERFORM 1 END $$' }],
                    ['EXECUTE IMMEDIATE split across lines', { join_condition: "EXECUTE\n\tIMMEDIATE\n'DROP TABLE content.node'" }],
                    ['PREPARE split across lines', { join_condition: 'PREPARE\n evil\n AS SELECT 1' }],
                    ['dangerous function inside a dollar quoted block', { join_condition: 'SELECT $$pg_sleep(1)$$' }],
                ])('should reject %s', (_label, overrides) => {
                    expectRejected(overrides);
                });
            });

            describe('dangerous / denylisted functions', () => {
                it.each([
                    ['pg_sleep time-based', { join_filter_target: 'pg_sleep(10) IS NOT NULL' }],
                    ['pg_read_file', { join_filter_target: "pg_read_file('/etc/passwd') IS NOT NULL" }],
                    ['pg_terminate_backend', { join_filter_target: 'pg_terminate_backend(1) IS NOT NULL' }],
                    ['pg_ls_dir', { join_filter_target: "pg_ls_dir('.') IS NOT NULL" }],
                    ['dblink', { join_filter_target: "dblink('host=x','SELECT 1') IS NOT NULL" }],
                    ['lo_import', { join_filter_target: "lo_import('/etc/passwd') IS NOT NULL" }],
                    ['lo_export', { join_filter_target: "lo_export(1, '/tmp/x') IS NOT NULL" }],
                    ['current_setting', { join_filter_target: "current_setting('data_directory') IS NOT NULL" }],
                    ['set_config', { join_filter_target: "set_config('x','y',false) IS NOT NULL" }],
                    ['query_to_xml', { join_filter_target: "query_to_xml('SELECT 1', true, true, '') IS NOT NULL" }],
                    ['database_to_xml', { join_filter_target: "database_to_xml(true, true, '') IS NOT NULL" }],
                    ['table_to_xml', { join_filter_target: "table_to_xml('content.node'::regclass, true, true, '') IS NOT NULL" }],
                    ['pg_sleep inside aggregate', { aggregate: ['ARRAY_AGG(pg_sleep(1)) as x'] }],
                    ['pg_sleep in join_condition', { join_condition: 'pg_sleep(1) IS NOT NULL' }],
                    ['case-insensitive pg_Sleep', { join_filter_target: 'Pg_Sleep(5) IS NOT NULL' }],
                    ['pg_sleep inside subquery', { join_filter_target: '1 AND (SELECT pg_sleep(1)) IS NOT NULL' }],
                    ['pg_sleep inside CTE', { join_condition: 'WITH x AS (SELECT pg_sleep(1) AS s) SELECT * FROM x' }],
                    ['space between function name and parenthesis', { join_filter_target: 'pg_sleep   (10) IS NOT NULL' }],
                    ['line break between function name and parenthesis', { join_filter_target: 'pg_sleep\n(10) IS NOT NULL' }],
                    ['pg_sleep hidden behind an E-string escaped quote', {
                        join_filter_target: "barrier = E'\\'' OR pg_sleep(1) IS NOT NULL OR barrier = 'x'"
                    }],
                    ['quoted identifier function call', {
                        join_filter_target: '"pg_sleep"(1) IS NOT NULL'
                    }],
                    ['schema qualified pg_catalog.pg_sleep', { join_filter_target: 'pg_catalog.pg_sleep(1) IS NOT NULL' }],
                ])('should reject %s', (_label, overrides) => {
                    expectRejected(overrides);
                });
            });

            describe('injection across all interpolated fields', () => {
                it('should reject DML and dangerous functions in join_condition, filters, and aggregate alike', () => {
                    const payloads = [
                        { join_condition: '1=1; DROP TABLE content.node' },
                        { join_filter_target: 'DELETE FROM content.edge' },
                        { join_filter_source: "pg_read_file('/etc/passwd') IS NOT NULL" },
                        { aggregate: ['ARRAY_AGG(pg_sleep(1)) as x'] },
                    ];
                    for (const payload of payloads) {
                        // Reset to a known-good baseline between attempts
                        spatialServiceParams = SpatialJoinRequestParams.from({
                            target_dimension: 'node',
                            source_dimension: 'node',
                            aggregate: ['ARRAY_AGG(ext:update) as ramp_width_mt'],
                            join_condition: 'ST_DWithin(geometry_target, geometry_source, 4)',
                            join_filter_target: "barrier='kerb'",
                            join_filter_source: "barrier='kerb'",
                            target_dataset_id: '7d6ae05c-8928-4727-bb0d-4717e46242f1',
                            source_dataset_id: '80296cbe-53ac-463b-b5f6-dad8b8e5e788'
                        });
                        expectRejected(payload);
                    }
                });
            });
        });

        it('should execute query with empty aggregate and atribute input', () => {
            // Set up the test data
            spatialServiceParams.aggregate = [];

            // Call the method under test
            const query = spatialServiceParams.buildSpatialQuery();
            expect(query.toString()).toContain('SELECT');
        });

        it('should execute query with required input only', () => {
            // Set up the test data
            spatialServiceParams.aggregate = [];

            // Call the method under test
            const query = spatialServiceParams.buildSpatialQuery();
            console.log(query);
            expect(query.toString()).toContain('SELECT');
        });
    });
});
