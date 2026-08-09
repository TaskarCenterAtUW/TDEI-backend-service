import { InputException } from '../../src/exceptions/http/http-exceptions';
import { AssignmentMethod, SpatialJoinRequestParams } from '../../src/service/interface/interfaces';
describe('BackendService', () => {
    let spatialServiceParams: SpatialJoinRequestParams;

    beforeEach(() => {
        spatialServiceParams = SpatialJoinRequestParams.from({
            target_dimension: 'node',
            source_dimension: 'node',
            aggregate: ['ARRAY_AGG(ext:ramp_width_mt) as ramp_width_mt', 'ARRAY_AGG(ext:unit_id) as unit_id', 'ARRAY_AGG(ext:condition) as conditions'],
            join_condition: 'ST_DWithin(geometry_target, geometry_source, 4)',
            join_filter_target: "barrier='kerb'",
            join_filter_source: "barrier='kerb'",
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
                it('should allow property names that contain DATE/UPDATE as substrings', () => {
                    expectAccepted({
                        join_filter_target: 'updatedat IS NOT NULL',
                        join_filter_source: 'created_at IS NOT NULL',
                        aggregate: ['ARRAY_AGG(updatedat) as updatedat']
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
                    // These are valid filter expressions (no subquery / DDL / denylisted fn).
                    // Always-true filters widen results but cannot read other tables.
                    expectAccepted({ join_filter_target: "barrier='kerb' OR 1=1" });
                    expectAccepted({ join_filter_source: "barrier='kerb' OR '1'='1'" });
                    expectAccepted({ join_filter_target: "barrier='x' OR TRUE" });
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

            describe('set operations and clause smuggling', () => {
                it.each([
                    ['UNION', { join_filter_target: '1=1 UNION SELECT * FROM content.node' }],
                    ['UNION ALL', { join_filter_target: '1=1 UNION ALL SELECT * FROM content.node' }],
                    ['INTERSECT', { join_filter_target: '1=1 INTERSECT SELECT 1' }],
                    ['EXCEPT', { join_filter_target: '1=1 EXCEPT SELECT 1' }],
                    ['GROUP BY smuggling', { join_filter_target: '1=1 GROUP BY barrier' }],
                    ['ORDER BY / LIMIT smuggling', { join_filter_target: '1=1 ORDER BY barrier LIMIT 1' }],
                    ['HAVING smuggling', { join_filter_target: '1=1 HAVING count(*) > 0' }],
                ])('should reject %s', (_label, overrides) => {
                    expectRejected(overrides);
                });
            });

            describe('subqueries and data exfiltration', () => {
                it.each([
                    ['IN subquery in filter', { join_filter_target: "barrier IN (SELECT barrier FROM content.node)" }],
                    ['scalar subquery in join condition', { join_condition: '(SELECT count(*) FROM pg_catalog.pg_tables) > 0' }],
                    ['EXISTS subquery', { join_filter_target: 'EXISTS (SELECT 1 FROM content.node)' }],
                    ['subquery inside CASE', { join_filter_target: 'CASE WHEN (SELECT 1) = 1 THEN true ELSE false END' }],
                    ['CAST of subquery', { join_filter_target: 'CAST((SELECT 1) AS int) > 0' }],
                    ['nested subquery with pg_sleep', { join_filter_target: '1 AND (SELECT pg_sleep(1)) IS NOT NULL' }],
                    ['subquery in aggregate', { aggregate: ['(SELECT count(*) FROM content.node) as leaked'] }],
                    ['subquery in join_filter_source', { join_filter_source: "name = (SELECT name FROM content.node LIMIT 1)" }],
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
                ])('should reject %s', (_label, overrides) => {
                    expectRejected(overrides);
                });
            });

            describe('unparseable and malformed input', () => {
                it.each([
                    ['broken operators', { join_filter_target: "barrier = = 'kerb' OR OR" }],
                    ['unbalanced quotes', { join_filter_target: "barrier = 'kerb" }],
                    ['unbalanced parentheses', { join_filter_target: 'ST_DWithin(geometry_target, geometry_source' }],
                    ['empty operator chain', { join_filter_source: "AND OR" }],
                    ['garbage aggregate', { aggregate: ['!!!not-sql!!!'] }],
                ])('should reject %s', (_label, overrides) => {
                    expectRejected(overrides);
                });
            });

            describe('injection across all interpolated fields', () => {
                it('should reject harmful payloads in join_condition, filters, and aggregate alike', () => {
                    const payloads = [
                        { join_condition: '1=1; SELECT 1' },
                        { join_filter_target: 'EXISTS (SELECT 1 FROM content.edge)' },
                        { join_filter_source: "pg_read_file('/etc/passwd') IS NOT NULL" },
                        { aggregate: ['ARRAY_AGG(highway) UNION SELECT password FROM users'] },
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
