import { InputException } from '../../src/exceptions/http/http-exceptions';
import { SpatialJoinRequestParams } from '../../src/service/interface/interfaces';
describe('BackendService', () => {
    let spatialServiceParams: SpatialJoinRequestParams;

    beforeEach(() => {
        spatialServiceParams = SpatialJoinRequestParams.from({
            target_dimension: 'edge',
            source_dimension: 'extension',
            aggregate: ['array_agg(ext:qm:fixed || \' \' || ext:qm:fixed) as qm', 'MAX(ext:qm:fixed) as max_qm'],
            // join_condition: 'ST_Intersects(ST_Buffer(geometry_target, 5), geometry_source)',
            join_condition: 'ST_DWITHIN(geometry_target, geometry_source, 10)',
            join_filter_target: "highway='footway' AND footway='sidewalk'",
            join_filter_source: "amenity='bench'",
            target_dataset_id: 'd1d11dc0-2c2c-4c22-b090-19f67a5af292',
            source_dataset_id: '3f7e7db6-a9dd-4fb7-825a-8d5581a77929'
        });
    });

    describe('buildSpatialQuery', () => {
        it('should build the spatial query correctly for edge target and node source', () => {
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