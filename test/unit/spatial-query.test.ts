import { InputException } from '../../src/exceptions/http/http-exceptions';
import { SpatialJoinRequestParams } from '../../src/service/interface/interfaces';
describe('BackendService', () => {
    let spatialServiceParams: SpatialJoinRequestParams;

    beforeEach(() => {
        spatialServiceParams = SpatialJoinRequestParams.from({
            target_dimension: 'edge',
            source_dimension: 'point',
            attributes: ['highway as my_highway', 'footway as my_footway'],
            aggregate: ['array_agg(highway) as my_way'],
            join_condition: 'ST_Intersects(geometry_target, geometry_source)',
            transform_target: 'ST_Buffer(geometry_target, 5)',
            transform_source: "",
            filter_target: "highway='footway' AND footway='sidewalk'",
            // filter_target: "   highway='footway'    AND footway='sidewalk' OR (highway2='footway' OR footway2='sidewalk')",
            filter_source: "highway='street_lamp'",
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
            expect(query.text).toContain('SELECT');
            expect(query.text).toContain('FROM');
            expect(query.text).toContain('INNER JOIN');
            expect(query.text).toContain('WHERE');
            expect(query.text).toContain('GROUP BY');
            // expect(query.values).toHaveLength(10);
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
    });
});