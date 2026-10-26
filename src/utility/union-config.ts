import { InputException } from "../exceptions/http/http-exceptions";

/**
 * Union 5.0 — the job's `config` parameter.
 *
 * content.tdei_union_dataset(src_one, src_two, config jsonb) takes one config
 * object with up to four sections, each with a mode of off / default / custom
 * (a section left out is off; no config at all = the two datasets combined,
 * unchanged):
 *
 *   {
 *     "snap":       { "mode": "default", "proximity": 1 },
 *     "dedup":      { "mode": "default", "proximity": 1 },
 *     "connect":    { "mode": "custom", "rules": [ ... ] },
 *     "extensions": { "mode": "default" }
 *   }
 *
 * The rules themselves are validated by the database
 * (content.union5_validate_config, run first inside the union), which rejects
 * an invalid config with a message naming the stage, rule and field. This
 * module only checks the envelope: valid JSON, an object, known sections, a
 * sane size — so a malformed request fails fast without opening a transaction.
 */
export const UNION_CONFIG_SECTIONS = ["snap", "dedup", "connect", "extensions"] as const;

/** 4.x parameters removed in 5.0 (no backward compatibility). */
export const REMOVED_UNION_PARAMETERS = ["proximity", "entity_filters"] as const;

/** Output name of the union's `report` cursor inside the job's zip. */
export const UNION_REPORT_FILE_NAME = "union_report.json";

/** Largest config accepted (bytes of JSON). 50 rules per stage fit comfortably. */
export const MAX_UNION_CONFIG_BYTES = 64 * 1024;

const SECTION_SET = new Set<string>(UNION_CONFIG_SECTIONS);

/**
 * Rejects the 4.x parameters. A request still sending them was written for the
 * old union and would silently get different behaviour, so it is refused.
 */
export function assertNoRemovedUnionParameters(params: Record<string, unknown> | null | undefined): void {
    if (!params) return;
    for (const key of REMOVED_UNION_PARAMETERS) {
        if (params[key] !== undefined && params[key] !== null) {
            throw new InputException(
                `'${key}' is no longer supported: union 5.0 takes its settings in 'config' ` +
                `(sections ${UNION_CONFIG_SECTIONS.join(", ")})`
            );
        }
    }
}

/**
 * Parses and envelope-checks the `config` parameter.
 *
 * @returns the config object to pass to the union; `{}` when none was given
 * @throws InputException when it is not a JSON object of known sections
 */
export function parseUnionConfig(rawConfig: unknown): Record<string, unknown> {
    if (rawConfig === undefined || rawConfig === null || rawConfig === "") {
        return {};
    }

    let config: unknown = rawConfig;
    if (typeof rawConfig === "string") {
        if (rawConfig.length > MAX_UNION_CONFIG_BYTES) {
            throw new InputException(`Invalid config: larger than ${MAX_UNION_CONFIG_BYTES / 1024} KB`);
        }
        try {
            config = JSON.parse(rawConfig);
        } catch {
            throw new InputException("Invalid config: must be valid JSON");
        }
    }

    if (typeof config !== "object" || config === null || Array.isArray(config)) {
        throw new InputException("Invalid config: expected a JSON object");
    }

    if (JSON.stringify(config).length > MAX_UNION_CONFIG_BYTES) {
        throw new InputException(`Invalid config: larger than ${MAX_UNION_CONFIG_BYTES / 1024} KB`);
    }

    const record = config as Record<string, unknown>;
    for (const section of Object.keys(record)) {
        if (!SECTION_SET.has(section)) {
            throw new InputException(
                `Invalid config: unknown section '${section}'. Allowed sections: ${UNION_CONFIG_SECTIONS.join(", ")}`
            );
        }
        const value = record[section];
        if (typeof value !== "object" || value === null || Array.isArray(value)) {
            throw new InputException(`Invalid config: '${section}' must be an object, e.g. {"mode": "default"}`);
        }
    }

    return record;
}
