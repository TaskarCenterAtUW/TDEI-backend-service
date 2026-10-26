import { InputException } from '../../src/exceptions/http/http-exceptions';
import {
    assertNoRemovedUnionParameters,
    MAX_UNION_CONFIG_BYTES,
    parseUnionConfig,
} from '../../src/utility/union-config';

describe('parseUnionConfig', () => {
    it('returns an empty config when none is given', () => {
        expect(parseUnionConfig(undefined)).toEqual({});
        expect(parseUnionConfig(null)).toEqual({});
        expect(parseUnionConfig('')).toEqual({});
    });

    it('accepts every section', () => {
        const config = {
            snap: { mode: 'custom', coincident: true, others: 0.5,
                    rules: [{ name: 'crossings', type: 'crossing', proximity: 1.5 }] },
            dedup: { mode: 'default', proximity: 1, min_overlap: 0.8 },
            connect: { mode: 'default', proximity: 1 },
            extensions: { mode: 'off' },
        };
        expect(parseUnionConfig(config)).toEqual(config);
    });

    it('parses a JSON string', () => {
        expect(parseUnionConfig('{"snap":{"mode":"default"}}')).toEqual({ snap: { mode: 'default' } });
    });

    it('leaves the rules to the database', () => {
        // an unknown mode is the union's to reject (content.union5_validate_config)
        expect(parseUnionConfig({ snap: { mode: 'sometimes' } })).toEqual({ snap: { mode: 'sometimes' } });
    });

    it.each([
        ['{bad json', 'must be valid JSON'],
        [[], 'expected a JSON object'],
        [42, 'expected a JSON object'],
        ['"snap"', 'expected a JSON object'],
        [{ snapping: { mode: 'default' } }, "unknown section 'snapping'"],
        [{ entity_filters: {} }, "unknown section 'entity_filters'"],
        [{ snap: 'default' }, "'snap' must be an object"],
        [{ connect: [] }, "'connect' must be an object"],
        [{ dedup: null }, "'dedup' must be an object"],
    ])('rejects %p', (config, message) => {
        expect(() => parseUnionConfig(config)).toThrow(InputException);
        expect(() => parseUnionConfig(config)).toThrow(message);
    });

    it('rejects a config larger than the limit', () => {
        const big = { connect: { mode: 'custom', rules: [{ name: 'x'.repeat(MAX_UNION_CONFIG_BYTES) }] } };
        expect(() => parseUnionConfig(big)).toThrow('larger than 64 KB');
        expect(() => parseUnionConfig(JSON.stringify(big))).toThrow('larger than 64 KB');
    });
});

describe('assertNoRemovedUnionParameters', () => {
    it('allows a 5.0 request', () => {
        expect(() => assertNoRemovedUnionParameters({ tdei_dataset_id_one: 'a', config: {} })).not.toThrow();
        expect(() => assertNoRemovedUnionParameters({ proximity: null, entity_filters: undefined })).not.toThrow();
        expect(() => assertNoRemovedUnionParameters(undefined)).not.toThrow();
    });

    it.each(['proximity', 'entity_filters'])('rejects %s', (key) => {
        expect(() => assertNoRemovedUnionParameters({ [key]: key === 'proximity' ? 0.5 : {} }))
            .toThrow(`'${key}' is no longer supported`);
    });
});
