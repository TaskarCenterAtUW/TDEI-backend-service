import { InputException } from '../../src/exceptions/http/http-exceptions';
import { SpatialJoinRequestParams } from '../../src/service/interface/interfaces';
describe('BackendService', () => {
    let spatialServiceParams: SpatialJoinRequestParams;

    beforeEach(() => {
        spatialServiceParams = SpatialJoinRequestParams.from({
            target_dimension: 'edge',
            source_dimension: 'point',
            aggregate: ['array_agg(DISTINCT(coalesce(highway, \'\'))) as my_way', 'MAX(footway) as max_footway'],
            join_condition: 'ST_Intersects(ST_Buffer(geometry_target, 5), geometry_source)',
            join_filter_target: "highway='footway' AND footway='sidewalk'",
            join_filter_source: "highway='street_lamp'",
            target_dataset_id: 'fa8e12ea-6b0c-4d3e-8b38-5b87b268e76b',
            source_dataset_id: '0d661b69495d47fb838862edf699fe09'
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