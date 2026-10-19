import { InputException } from "../exceptions/http/http-exceptions";
import oswUnionFilterAttributes from "../constants/osw-union-filter-attributes.json";

export const UNION_OPTION_PARENT_KEYS = [
    "edge",
    "node",
    "point",
    "zone",
    "line",
    "polygon",
] as const;

export type UnionOptionParentKey = typeof UNION_OPTION_PARENT_KEYS[number];

type DimensionAttributeMap = Record<UnionOptionParentKey, string[]>;

const DIMENSION_ATTRIBUTES = oswUnionFilterAttributes as DimensionAttributeMap;
const PARENT_KEY_SET = new Set<string>(UNION_OPTION_PARENT_KEYS);
const EXT_ATTRIBUTE_PATTERN = /^ext:.+$/;

const DUPLICATE_BUFFER_WIDTH_KEY = "duplicate_buffer_width";
const DUPLICATE_OVERLAP_PERCENTAGE_KEY = "duplicate_overlap_percentage";
const FILTERS_KEY = "filters";

/** Metres — allowed on edge and line only. */
const BUFFER_WIDTH_PARENTS = new Set<UnionOptionParentKey>(["edge", "line"]);
/** 0–100 — allowed on edge, line, polygon, and zone. */
const OVERLAP_PERCENTAGE_PARENTS = new Set<UnionOptionParentKey>([
    "edge",
    "line",
    "polygon",
    "zone",
]);

/**
 * Validates and normalizes the optional entity_filters JSON passed to
 * content.tdei_union_dataset(..., entity_filters jsonb).
 *
 * Expected shape:
 * {
 *   "edge": {
 *     "filters": [{ "highway": "footway", "footway": "sidewalk" }],
 *     "duplicate_buffer_width": 3,
 *     "duplicate_overlap_percentage": 70
 *   },
 *   "polygon": { "duplicate_overlap_percentage": 80 }
 * }
 *
 * Only the six OSW dimension parent keys are allowed. Filter object keys must be
 * valid OpenSidewalks attributes for that dimension (or ext:* extensions).
 * Duplicate-detection settings are rejected on dimensions that cannot use them.
 */
export function validateUnionOptions(rawOptions: unknown): object | null {
    if (rawOptions === undefined || rawOptions === null || rawOptions === "") {
        return null;
    }

    let options: unknown = rawOptions;
    if (typeof rawOptions === "string") {
        try {
            options = JSON.parse(rawOptions);
        } catch {
            throw new InputException("Invalid entity_filters: must be valid JSON");
        }
    }

    if (typeof options !== "object" || Array.isArray(options) || options === null) {
        throw new InputException("Invalid entity_filters: expected a JSON object");
    }

    const record = options as Record<string, unknown>;
    for (const parentKey of Object.keys(record)) {
        if (!PARENT_KEY_SET.has(parentKey)) {
            throw new InputException(
                `Invalid entity_filters: unsupported parent key '${parentKey}'. Allowed keys: ${UNION_OPTION_PARENT_KEYS.join(", ")}`
            );
        }

        validateDimensionOptions(parentKey as UnionOptionParentKey, record[parentKey]);
    }

    return record;
}

function validateDimensionOptions(parentKey: UnionOptionParentKey, value: unknown): void {
    if (value === undefined || value === null) {
        return;
    }

    if (typeof value !== "object" || Array.isArray(value)) {
        throw new InputException(
            `Invalid entity_filters: '${parentKey}' must be an object`
        );
    }

    const dimension = value as Record<string, unknown>;
    for (const key of Object.keys(dimension)) {
        if (key === FILTERS_KEY) {
            validateFilters(parentKey, dimension.filters);
            continue;
        }

        if (key === DUPLICATE_BUFFER_WIDTH_KEY) {
            validateDuplicateBufferWidth(parentKey, dimension[key]);
            continue;
        }

        if (key === DUPLICATE_OVERLAP_PERCENTAGE_KEY) {
            validateDuplicateOverlapPercentage(parentKey, dimension[key]);
            continue;
        }

        throw new InputException(
            `Invalid entity_filters: unsupported key '${parentKey}.${key}'. Allowed keys: filters, ${DUPLICATE_BUFFER_WIDTH_KEY}, ${DUPLICATE_OVERLAP_PERCENTAGE_KEY}`
        );
    }
}

function validateFilters(parentKey: UnionOptionParentKey, filters: unknown): void {
    if (filters === undefined || filters === null) {
        return;
    }

    if (!Array.isArray(filters)) {
        throw new InputException(
            `Invalid entity_filters: '${parentKey}.filters' must be an array`
        );
    }

    const allowedAttributes = new Set(DIMENSION_ATTRIBUTES[parentKey] ?? []);
    filters.forEach((filter, index) => {
        validateFilterObject(parentKey, filter, index, allowedAttributes);
    });
}

function validateDuplicateBufferWidth(parentKey: UnionOptionParentKey, value: unknown): void {
    if (!BUFFER_WIDTH_PARENTS.has(parentKey)) {
        throw new InputException(
            `Invalid entity_filters: '${DUPLICATE_BUFFER_WIDTH_KEY}' is not supported for '${parentKey}'. Allowed on: edge, line`
        );
    }

    if (typeof value !== "number" || !Number.isFinite(value)) {
        throw new InputException(
            `Invalid entity_filters: '${parentKey}.${DUPLICATE_BUFFER_WIDTH_KEY}' must be a finite number (metres)`
        );
    }

    if (value < 0) {
        throw new InputException(
            `Invalid entity_filters: '${parentKey}.${DUPLICATE_BUFFER_WIDTH_KEY}' must be >= 0`
        );
    }
}

function validateDuplicateOverlapPercentage(parentKey: UnionOptionParentKey, value: unknown): void {
    if (!OVERLAP_PERCENTAGE_PARENTS.has(parentKey)) {
        throw new InputException(
            `Invalid entity_filters: '${DUPLICATE_OVERLAP_PERCENTAGE_KEY}' is not supported for '${parentKey}'. Allowed on: edge, line, polygon, zone`
        );
    }

    if (typeof value !== "number" || !Number.isFinite(value)) {
        throw new InputException(
            `Invalid entity_filters: '${parentKey}.${DUPLICATE_OVERLAP_PERCENTAGE_KEY}' must be a finite number`
        );
    }

    if (value < 0 || value > 100) {
        throw new InputException(
            `Invalid entity_filters: '${parentKey}.${DUPLICATE_OVERLAP_PERCENTAGE_KEY}' must be between 0 and 100`
        );
    }
}

function validateFilterObject(
    parentKey: UnionOptionParentKey,
    filter: unknown,
    index: number,
    allowedAttributes: Set<string>
): void {
    if (typeof filter !== "object" || filter === null || Array.isArray(filter)) {
        throw new InputException(
            `Invalid entity_filters: '${parentKey}.filters[${index}]' must be an object`
        );
    }

    const filterRecord = filter as Record<string, unknown>;
    const attributeKeys = Object.keys(filterRecord);
    if (attributeKeys.length === 0) {
        throw new InputException(
            `Invalid entity_filters: '${parentKey}.filters[${index}]' must include at least one attribute`
        );
    }

    for (const attribute of attributeKeys) {
        if (EXT_ATTRIBUTE_PATTERN.test(attribute)) {
            continue;
        }
        if (!allowedAttributes.has(attribute)) {
            throw new InputException(
                `Invalid entity_filters: '${attribute}' is not a valid OpenSidewalks attribute for '${parentKey}'`
            );
        }
    }
}
