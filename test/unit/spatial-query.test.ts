import { InputException } from '../../src/exceptions/http/http-exceptions';
import { SpatialJoinRequestParams } from '../../src/service/interface/interfaces';
describe('BackendService', () => {
    let spatialServiceParams: SpatialJoinRequestParams;

    beforeEach(() => {
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
    });

    describe('buildSpatialQuery', () => {

        it('should build the spatial query correctly for edge target and extension source', () => {
            // Call the method under test
            const query = spatialServiceParams.buildSpatialQuery();
            console.log(query);
            // Assertions
            expect(query).toContain('SELECT');
            expect(query).toContain('FROM');
            expect(query).toContain('LEFT JOIN');
            expect(query).toContain('WHERE');
            expect(query).toContain('GROUP BY');
            expect(query).not.toContain('geometry_target');
            expect(query).not.toContain('geometry_source');
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
            expect(query).toContain('SELECT');
            expect(query).toContain('FROM');
            expect(query).toContain('LEFT JOIN');
            expect(query).toContain('WHERE');
            expect(query).toContain('GROUP BY');
            expect(query).toContain('edge_id');
            expect(query).not.toContain('geometry_target');
            expect(query).not.toContain('geometry_source');
            expect(query).not.toContain('_u_id');
            expect(query).not.toContain('_v_id');
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
            expect(query).toContain('SELECT');
            expect(query).toContain('FROM');
            expect(query).toContain('LEFT JOIN');
            expect(query).toContain('WHERE');
            expect(query).toContain('GROUP BY');
            expect(query).toContain('point_id');
            expect(query).not.toContain('geometry_target');
            expect(query).not.toContain('geometry_source');
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
            expect(query).toContain('SELECT');
            expect(query).toContain('FROM');
            expect(query).toContain('LEFT JOIN');
            expect(query).toContain('WHERE');
            expect(query).toContain('GROUP BY');
            expect(query).not.toContain('geometry_target');
            expect(query).not.toContain('geometry_source');
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
            expect(query).toContain('SELECT');
            expect(query).toContain('FROM');
            expect(query).toContain('LEFT JOIN');
            expect(query).toContain('WHERE');
            expect(query).toContain('GROUP BY');
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

        it('should throw an InputException for harmful query', () => {
            // Set up the test data
            spatialServiceParams.aggregate = ['array_agg(highway); DROP TABLE dataset;'];

            // Call the method under test
            expect(() => spatialServiceParams.buildSpatialQuery()).toThrow(InputException);
        });

        it('should execute query with empty aggregate and atribute input', () => {
            // Set up the test data
            spatialServiceParams.aggregate = [];

            // Call the method under test
            const query = spatialServiceParams.buildSpatialQuery();
            expect(query).toContain('SELECT');
        });

        it('should execute query with required input only', () => {
            // Set up the test data
            spatialServiceParams.aggregate = [];

            // Call the method under test
            const query = spatialServiceParams.buildSpatialQuery();
            console.log(query);
            expect(query).toContain('SELECT');
        });
    });
});