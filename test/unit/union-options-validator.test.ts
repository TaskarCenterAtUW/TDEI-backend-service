import {
    UNION_OPTION_PARENT_KEYS,
    validateUnionOptions,
} from '../../src/utility/union-options-validator';
import { InputException } from '../../src/exceptions/http/http-exceptions';

/** Full sample covering filters + duplicate settings across all six parents. */
const FULL_ENTITY_FILTERS_SAMPLE = {
    edge: {
        filters: [
            { highway: 'footway', footway: 'sidewalk' },
            { highway: 'footway', footway: 'crossing' },
        ],
        duplicate_buffer_width: 2,
        duplicate_overlap_percentage: 75,
    },
    node: {
        filters: [
            { barrier: 'kerb' },
        ],
    },
    line: {
        filters: [
            { barrier: 'fence' },
        ],
        duplicate_buffer_width: 1.5,
        duplicate_overlap_percentage: 65,
    },
    polygon: {
        filters: [
            { building: 'yes' },
        ],
        duplicate_overlap_percentage: 80,
    },
    zone: {
        duplicate_overlap_percentage: 75,
    },
    point: {
        filters: [
            { amenity: 'bench' },
            { highway: 'street_lamp' },
        ],
    },
};

describe('validateUnionOptions', () => {
    it('returns null when options are omitted', () => {
        expect(validateUnionOptions(undefined)).toBeNull();
        expect(validateUnionOptions(null)).toBeNull();
        expect(validateUnionOptions('')).toBeNull();
    });

    it('accepts the full multi-dimension entity_filters sample', () => {
        expect(validateUnionOptions(FULL_ENTITY_FILTERS_SAMPLE)).toEqual(FULL_ENTITY_FILTERS_SAMPLE);
    });

    it('accepts the full sample when provided as a JSON string', () => {
        expect(validateUnionOptions(JSON.stringify(FULL_ENTITY_FILTERS_SAMPLE)))
            .toEqual(FULL_ENTITY_FILTERS_SAMPLE);
    });

    it('accepts valid filter JSON for sidewalk edges', () => {
        const options = {
            edge: {
                filters: [{ highway: 'footway', footway: 'sidewalk' }],
            },
        };

        expect(validateUnionOptions(options)).toEqual(options);
    });

    it('accepts options provided as a JSON string', () => {
        const options = {
            edge: {
                filters: [{ highway: 'footway', footway: 'sidewalk' }],
            },
        };

        expect(validateUnionOptions(JSON.stringify(options))).toEqual(options);
    });

    it('accepts all six parent keys with empty filters', () => {
        const options = Object.fromEntries(
            UNION_OPTION_PARENT_KEYS.map((key) => [key, { filters: [] }])
        );

        expect(validateUnionOptions(options)).toEqual(options);
    });

    it('accepts extension attributes prefixed with ext:', () => {
        const options = {
            node: {
                filters: [{ 'ext:source': 'survey' }],
            },
        };

        expect(validateUnionOptions(options)).toEqual(options);
    });

    it('accepts duplicate-detection settings on supported dimensions', () => {
        const options = {
            edge: {
                duplicate_buffer_width: 3,
                duplicate_overlap_percentage: 70,
            },
            line: {
                duplicate_buffer_width: 1.5,
                duplicate_overlap_percentage: 70,
            },
            polygon: {
                duplicate_overlap_percentage: 80,
            },
            zone: {
                duplicate_overlap_percentage: 70,
            },
        };

        expect(validateUnionOptions(options)).toEqual(options);
    });

    it('accepts filters combined with duplicate-detection settings', () => {
        const options = {
            edge: {
                filters: [{ highway: 'footway', footway: 'sidewalk' }],
                duplicate_buffer_width: 3,
                duplicate_overlap_percentage: 70,
            },
        };

        expect(validateUnionOptions(options)).toEqual(options);
    });

    it('accepts multiple filter objects per dimension from the sample', () => {
        expect(validateUnionOptions({
            edge: FULL_ENTITY_FILTERS_SAMPLE.edge,
            point: FULL_ENTITY_FILTERS_SAMPLE.point,
        })).toEqual({
            edge: FULL_ENTITY_FILTERS_SAMPLE.edge,
            point: FULL_ENTITY_FILTERS_SAMPLE.point,
        });
    });

    it('accepts zone with only duplicate_overlap_percentage from the sample', () => {
        expect(validateUnionOptions({
            zone: FULL_ENTITY_FILTERS_SAMPLE.zone,
        })).toEqual({
            zone: FULL_ENTITY_FILTERS_SAMPLE.zone,
        });
    });

    it('rejects unsupported parent keys', () => {
        expect(() => validateUnionOptions({ edges: { filters: [] } })).toThrow(InputException);
        expect(() => validateUnionOptions({ edges: { filters: [] } })).toThrow(/unsupported parent key 'edges'/);
    });

    it('rejects invalid attributes for a parent key', () => {
        expect(() =>
            validateUnionOptions({
                edge: {
                    filters: [{ not_an_osw_attribute: 'x' }],
                },
            })
        ).toThrow(/not a valid OpenSidewalks attribute for 'edge'/);
    });

    it('rejects attributes that belong to a different dimension', () => {
        expect(() =>
            validateUnionOptions({
                edge: {
                    filters: [{ kerb: 'raised' }],
                },
            })
        ).toThrow(/not a valid OpenSidewalks attribute for 'edge'/);
    });

    it('rejects malformed filters arrays', () => {
        expect(() =>
            validateUnionOptions({
                edge: {
                    filters: 'highway=footway',
                },
            })
        ).toThrow(/'edge.filters' must be an array/);
    });

    it('rejects unsupported keys under a parent object', () => {
        expect(() =>
            validateUnionOptions({
                edge: {
                    buffer: 2,
                },
            })
        ).toThrow(/unsupported key 'edge.buffer'/);
    });

    it('rejects duplicate_buffer_width on unsupported dimensions', () => {
        for (const parent of ['node', 'point', 'polygon', 'zone'] as const) {
            expect(() =>
                validateUnionOptions({
                    [parent]: { duplicate_buffer_width: 3 },
                })
            ).toThrow(/'duplicate_buffer_width' is not supported/);
        }
    });

    it('rejects duplicate_buffer_width when added to sample node/point/polygon/zone blocks', () => {
        expect(() =>
            validateUnionOptions({
                ...FULL_ENTITY_FILTERS_SAMPLE,
                node: {
                    ...FULL_ENTITY_FILTERS_SAMPLE.node,
                    duplicate_buffer_width: 2,
                },
            })
        ).toThrow(/'duplicate_buffer_width' is not supported for 'node'/);

        expect(() =>
            validateUnionOptions({
                ...FULL_ENTITY_FILTERS_SAMPLE,
                polygon: {
                    ...FULL_ENTITY_FILTERS_SAMPLE.polygon,
                    duplicate_buffer_width: 2,
                },
            })
        ).toThrow(/'duplicate_buffer_width' is not supported for 'polygon'/);
    });

    it('rejects duplicate_overlap_percentage on unsupported dimensions', () => {
        for (const parent of ['node', 'point'] as const) {
            expect(() =>
                validateUnionOptions({
                    [parent]: { duplicate_overlap_percentage: 80 },
                })
            ).toThrow(/'duplicate_overlap_percentage' is not supported/);
        }
    });

    it('rejects duplicate_overlap_percentage when added to sample node/point blocks', () => {
        expect(() =>
            validateUnionOptions({
                ...FULL_ENTITY_FILTERS_SAMPLE,
                point: {
                    ...FULL_ENTITY_FILTERS_SAMPLE.point,
                    duplicate_overlap_percentage: 80,
                },
            })
        ).toThrow(/'duplicate_overlap_percentage' is not supported for 'point'/);
    });

    it('rejects duplicate_buffer_width outside valid range', () => {
        expect(() =>
            validateUnionOptions({
                edge: { duplicate_buffer_width: -1 },
            })
        ).toThrow(/must be >= 0/);
    });

    it('rejects duplicate_overlap_percentage outside 0-100', () => {
        expect(() =>
            validateUnionOptions({
                edge: { duplicate_overlap_percentage: 101 },
            })
        ).toThrow(/must be between 0 and 100/);

        expect(() =>
            validateUnionOptions({
                polygon: { duplicate_overlap_percentage: -5 },
            })
        ).toThrow(/must be between 0 and 100/);
    });

    it('rejects non-numeric duplicate settings', () => {
        expect(() =>
            validateUnionOptions({
                edge: { duplicate_buffer_width: '3' },
            })
        ).toThrow(/must be a finite number/);

        expect(() =>
            validateUnionOptions({
                edge: { duplicate_overlap_percentage: '70' },
            })
        ).toThrow(/must be a finite number/);
    });

    it('rejects invalid JSON strings', () => {
        expect(() => validateUnionOptions('{bad json')).toThrow(/must be valid JSON/);
    });
});
