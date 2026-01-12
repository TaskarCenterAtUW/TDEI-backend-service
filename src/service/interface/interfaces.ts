import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import { AbstractDomainEntity, Prop } from "nodets-ms-core/lib/models";
import { InputException } from "../../exceptions/http/http-exceptions";
import { QueryConfig } from "pg";
import { Parser } from "node-sql-parser";
const parser = new Parser();
/**
 * Represents a backend request.
 */
export class BackendRequest {
    service!: string;
    parameters!: any;
    user_id!: string;
    constructor(init: Partial<BackendRequest>) {
        Object.assign(this, init);
    }
}

/**
 * Represents a backend request.
 */
export interface IBackendRequest {
    /**
     * Processes the backend request.
     * @param message The queue message containing the request data.
     * @returns A promise that resolves to a boolean indicating the success of the request processing.
     */
    backendRequestProcessor(message: QueueMessage): Promise<boolean>;
}

export interface IUploadContext {
    containerName: string;
    filePath: string;
    remoteUrls: string[];
    zipUrl: string;
    outputFileName?: string;
}

export interface IUploadXMLContext {
    tdei_dataset_id: string;
    containerName: string;
    filePath: string;
    remoteUrl: string;
}

export enum AssignmentMethod {
    DEFAULT = "default",
    EXCLUSIVE = "exclusive"
}

export interface AttributeDetails { alias: string, column: string[], aggregate?: string }

export class SpatialJoinRequestParams extends AbstractDomainEntity {

    @Prop()
    target_dataset_id!: string;
    @Prop()
    target_dimension!: string;
    @Prop()
    source_dataset_id!: string;
    @Prop()
    source_dimension!: string;
    @Prop()
    join_condition!: string;
    @Prop()
    join_filter_target!: string;
    @Prop()
    join_filter_source!: string;
    @Prop()
    aggregate: string[] = []; //attributes from source dimension to be aggregated
    @Prop()
    assignment_method: AssignmentMethod = AssignmentMethod.DEFAULT;
    /**
     * Basic SQL injection check
     * @param obj
     */
    private checkForSqlInjection(obj: any) {
        const harmfulKeywords = [';', 'DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE'];

        for (let key in obj) {
            if (typeof obj[key] === 'string') {
                for (let keyword of harmfulKeywords) {
                    if (obj[key].toUpperCase().includes(keyword)) {
                        throw new InputException(`Harmful keyword found in input : ${key}`);
                    }
                }
            } else if (Array.isArray(obj[key])) {
                for (let item of obj[key]) {
                    if (typeof item === 'string') {
                        for (let keyword of harmfulKeywords) {
                            if (item.toUpperCase().includes(keyword)) {
                                throw new InputException(`Harmful keyword found in input : ${key}`);
                            }
                        }
                    }
                }
            }
        }
    }

    private prefixColumns(query: string, prefix: string, isExtensionFile: boolean): string {
        // Regular expression to match column names (assuming column names are word characters)
        const columnPattern = /\b(?!geometry_target\b)(?!geometry_source\b)(\w+)\b(?=\s*(=|!=|>|<|>=|<=))/g;

        // Replace matched column names with 'source.' prepended
        // if (isExtensionFile || query.includes('ext:')) {
        //     return query.replace(columnPattern, `${prefix}.feature->'properties'->>'$1'::text`);
        // }
        // return query.replace(columnPattern, `${prefix}.$1`);
        return query.replace(columnPattern, `${prefix}.feature->'properties'->>'$1'::text`);
    }

    private removeExtraSpacesFromString(str: string): string {
        str = str.trim();
        // Regular expression to match one or more spaces
        const spacePattern = /\s{2,}/g;
        // Replace occurrences of more than one space with a single space
        return str.replace(spacePattern, ' ');
    }

    private cleanProperties(): void {
        // Clean each property of the class
        for (const key in this) {
            if (this.hasOwnProperty(key) && typeof this[key] === 'string') {
                // Clean string properties
                this[key] = this.removeExtraSpacesFromString(this[key] as string) as any;
            } else if (this.hasOwnProperty(key) && Array.isArray(this[key])) {
                // Clean arrays within class properties
                this[key] = (this[key] as any).map((item: any) => {
                    if (typeof item === 'string') {
                        return this.removeExtraSpacesFromString(item);
                    }
                    return item;
                });
            }
        }
    }

    /**
     * Builds the spatial query
     * @returns The spatial query
     */
    buildSpatialQuery(): string[] {
        const MAX_KNN = 2;      // cap per target

        this.checkForSqlInjection(this);
        this.cleanProperties();

        const meta = this.getDimensionMetadata();

        // Compile the aggregate fields
        let aggregate_compiled: AttributeDetails[] = [];
        try {
            if (this.aggregate?.length) {
                aggregate_compiled = this.aggregate.map((aggregate) => {
                    const { alias, column, aggregate: modifiedAggregate } = this.replaceColumnNamesFromAggregate(aggregate);
                    return { alias, column, aggregate: modifiedAggregate };
                });
            }
        } catch (error) {
            throw new InputException('Invalid aggregate syntax');
        }


        // Prepare filters
        const filter_target = this.processFilter(this.join_filter_target, 'target', false, meta.transform_geometry_target);
        const filter_source = this.processFilter(this.join_filter_source, 'source', meta.isExtensionFile, meta.transform_geometry_source);

        const caseStatements = this.generateCaseStatements(aggregate_compiled, filter_target, meta.sourceJoinKey);

        // Select attributes
        let select_attributes = `${meta.target_select_required_fields}`;
        if (aggregate_compiled.length == 0) {
            select_attributes += `, (target.feature::jsonb)::json`;
        }

        const group_by = `${meta.target_select_required_fields}, target.feature::jsonb`;

        // Transform the join geometry conditionally
        let join_condition_compiled = this.join_condition.replace(/geometry_target/g, meta.transform_geometry_target);
        join_condition_compiled = join_condition_compiled.replace(/geometry_source/g, meta.transform_geometry_source);

        let querySteps: string[] = [];

        if (this.assignment_method === AssignmentMethod.EXCLUSIVE) {

            querySteps = [

                /* 1. tmp_candidates with KNN and limited neighbors */
                `
            CREATE TEMP TABLE tmp_candidates ON COMMIT DROP AS
            SELECT target.${this.target_dimension}_id AS t_id,
                   source.${this.source_dimension === 'extension' ? 'ext_id' : this.source_dimension + '_id'} AS s_id,
                   ST_Distance(${meta.transform_geometry_target}, ${meta.transform_geometry_source}) AS dist_m,
                   ROW_NUMBER() OVER (
                     PARTITION BY target.${this.target_dimension}_id
                     ORDER BY ${meta.transform_geometry_target} <-> ${meta.transform_geometry_source}
                   ) AS source_rank
            FROM ${meta.target_table}
            LEFT JOIN LATERAL (
                SELECT *
                FROM ${meta.source_table}
                WHERE 
                SOURCE.tdei_dataset_id = '${this.source_dataset_id}'
                AND ${join_condition_compiled}
                ${filter_source ? `AND (${filter_source})` : ''}
                ORDER BY ${meta.transform_geometry_target} <-> ${meta.transform_geometry_source}
                LIMIT ${MAX_KNN}
            ) source ON TRUE
            WHERE target.tdei_dataset_id = '${this.target_dataset_id}'
                ${filter_target ? `AND (${filter_target})` : ''}
            `,

                /* 2. indexes */
                `CREATE INDEX idx_tmp_cand_t_s ON tmp_candidates (t_id, s_id)`,
                `CREATE INDEX idx_tmp_cand_s_dist ON tmp_candidates (s_id, dist_m)`,

                /* 3. tmp_final_assign */
                `
            CREATE TEMP TABLE tmp_final_assign ON COMMIT DROP AS
            WITH Comp AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY s_id ORDER BY dist_m) AS target_priority
                FROM tmp_candidates
            ),
            Winners AS (
                SELECT t_id, s_id
                FROM Comp
                WHERE source_rank = 1
                  AND target_priority = 1
            ),
            Losers AS (
                SELECT t_id
                FROM Comp
                WHERE source_rank = 1
                  AND target_priority > 1
            )
            SELECT t_id, s_id FROM Winners
            UNION ALL
            SELECT l.t_id, tc.s_id
            FROM Losers l
            JOIN tmp_candidates tc
              ON l.t_id = tc.t_id
            WHERE tc.source_rank = 2
            `,

                /* 4. index */
                `CREATE INDEX idx_tmp_final_t_id ON tmp_final_assign (t_id)`,
                `CREATE INDEX idx_tmp_final_s_id ON tmp_final_assign (s_id)`,

                /* 5. final result */
                `
            CREATE TEMP TABLE temp_dataset_join_result ON COMMIT DROP AS
            SELECT
                ${meta.target_select_required_fields},
                ${aggregate_compiled.length
                    ? `JSONB_SET(
                         target.feature::jsonb,
                         '{properties}',
                         COALESCE(target.feature::jsonb -> 'properties', '{}'::jsonb)
                         || (${caseStatements}),
                         TRUE
                       )::json AS feature`
                    : `(target.feature::jsonb)::json AS feature`}
            FROM ${meta.target_table}
            LEFT JOIN tmp_final_assign tfa
              ON target.${this.target_dimension}_id = tfa.t_id
            LEFT JOIN ${meta.source_table}
              ON tfa.s_id = source.${this.source_dimension === 'extension' ? 'ext_id' : this.source_dimension + '_id'}
                AND source.tdei_dataset_id = '${this.source_dataset_id}'
              ${filter_source ? `AND (${filter_source})` : ''}
            WHERE target.tdei_dataset_id = '${this.target_dataset_id}'
            GROUP BY ${meta.target_select_required_fields}, target.feature::jsonb
            `
            ];

        } else {

            /* Default : ONE-TO-MANY logic */
            querySteps = [
                `
            CREATE TEMP TABLE temp_dataset_join_result ON COMMIT DROP AS
            SELECT
                ${meta.target_select_required_fields},
                ${aggregate_compiled.length
                    ? `JSONB_SET(
                         target.feature::jsonb,
                         '{properties}',
                         COALESCE(target.feature::jsonb -> 'properties', '{}'::jsonb)
                         || (${caseStatements}),
                         TRUE
                       )::json AS feature`
                    : `(target.feature::jsonb)::json AS feature`}
            FROM ${meta.target_table}
            LEFT JOIN ${meta.source_table}
              ON ${join_condition_compiled}
              AND source.tdei_dataset_id = '${this.source_dataset_id}'
              ${filter_source ? `AND (${filter_source})` : ''}
            WHERE target.tdei_dataset_id = '${this.target_dataset_id}'
            GROUP BY ${meta.target_select_required_fields}, target.feature::jsonb
            `
            ];
        }

        return querySteps.map(q => q.replace(/\s+/g, ' ').trim());
    }

    private processFilter(filter: string, prefix: string, isExt: boolean, geom: string): string {
        if (!filter || filter === '') return '';
        let processed = this.prefixColumns(filter, prefix, isExt);
        return processed.replace(new RegExp(`geometry_${prefix}`, 'g'), geom);
    }

    getDimensionMetadata() {
        let meta: any = {};


        //based on the target dimension, select the required fields, target table, and transform the geometry
        switch (this.target_dimension) {
            case 'edge':
                meta.target_table = 'content.edge target';
                meta.transform_geometry_target = 'target.edge_loc_3857';
                meta.target_select_required_fields = 'target.edge_id, target.edge_loc, target.orig_node_id, target.dest_node_id';
                break;
            case 'node':
                meta.target_table = 'content.node target';
                meta.transform_geometry_target = 'target.node_loc_3857';
                meta.target_select_required_fields = 'target.node_id, target.node_loc';
                break;
            case 'zone':
                meta.target_table = 'content.zone target';
                meta.transform_geometry_target = 'target.zone_loc_3857';
                meta.target_select_required_fields = 'target.zone_id, target.zone_loc, target.node_ids';
                break;
            default:
                throw new InputException('Invalid target dimension');
        }


        //based on the source dimension, select the source table, and transform the geometry
        switch (this.source_dimension) {
            case 'edge':
                meta.source_table = 'content.edge source';
                meta.transform_geometry_source = 'source.edge_loc_3857';
                meta.sourceJoinKey = 'source.edge_id';
                break;
            case 'node':
                meta.source_table = 'content.node source';
                meta.transform_geometry_source = 'source.node_loc_3857';
                meta.sourceJoinKey = 'source.node_id';
                break;
            case 'zone':
                meta.source_table = 'content.zone source';
                meta.transform_geometry_source = 'source.zone_loc_3857';
                meta.sourceJoinKey = 'source.zone_id';
                break;
            case 'point':
                meta.source_table = 'content.extension_point source';
                meta.transform_geometry_source = 'source.point_loc_3857';
                meta.sourceJoinKey = 'source.point_id';
                break;
            case 'line':
                meta.source_table = 'content.extension_line source';
                meta.transform_geometry_source = 'source.line_loc_3857';
                meta.sourceJoinKey = 'source.line_id';
                break;
            case 'polygon':
                meta.source_table = 'content.extension_polygon source';
                meta.transform_geometry_source = 'source.polygon_loc_3857';
                meta.sourceJoinKey = 'source.polygon_id';
                break;
            case 'extension':
                meta.source_table = 'content.extension source';
                meta.transform_geometry_source = 'source.ext_loc_3857';
                meta.isExtensionFile = true;
                meta.sourceJoinKey = 'source.ext_id';
                break;
            default:
                throw new InputException('Invalid source dimension');
        }
        return meta;
    }


    /**
     * Generates the case statements for aggregating and non-aggregating attributes.
     * 
     * @param aggregatedAttributes - An array of AttributeDetails objects representing the aggregated attributes.
     * @param nonAggregatedAttributes - An array of AttributeDetails objects representing the non-aggregated attributes.
     * @returns A string representing the generated case statements.
     */
    generateCaseStatements(aggregatedAttributes: AttributeDetails[], filterTarget?: string, sourceJoinKey?: string) {
        return aggregatedAttributes
            .map(attr => {
                const columns = Array.isArray(attr.column) ? attr.column : [attr.column];
                const notNullClause = columns.map(col => `${col} IS NOT NULL`).join(' AND ');

                const sourceGate = `COUNT(${sourceJoinKey}) FILTER (WHERE ${notNullClause}) > 0`;
                const targetGate = filterTarget ? `BOOL_OR(${filterTarget})` : 'TRUE';

                return `
        CASE
          WHEN ${targetGate}
           AND ${sourceGate}
          THEN JSONB_BUILD_OBJECT(
            'ext:${attr.alias}',
            ${attr.aggregate} FILTER (WHERE ${notNullClause})
          )
          ELSE '{}'::jsonb
        END
      `;
            })
            .join(' || ');
    }

    /**
     * Replaces column names in an aggregate with the corresponding source prefix.
     * 
     * @param aggregate - The aggregate to be modified.
     * @returns An object containing the modified aggregate, alias, and column.
     */
    replaceColumnNamesFromAggregate(aggregate: string): { alias: string, column: string[], aggregate: string } {
        const parsedQuery = parser.astify(`SELECT ${aggregate} FROM dummy_table`);
        let alias_name = '';
        let columnNames: Set<string> = new Set();

        function traverse(node: any) {
            if (node.type === 'column_ref') {
                columnNames.add(node.column);
            }
            if (node.as) {
                alias_name = node.as;
            }
            if (node.left) traverse(node.left);
            if (node.right) traverse(node.right);
            if (node.expr) traverse(node.expr);
            if (node.args?.value) node.args.value.forEach((arg: any) => traverse(arg));
            if (node.args?.expr) traverse(node.args?.expr);
            if (node.value) traverse(node.value);
            if (node.columns) node.columns.forEach((col: any) => traverse(col));
        }

        traverse(parsedQuery);

        let modifiedAggregate = aggregate;
        let columnNamesReplaced: string[] = [];
        columnNames.forEach(columnName => {
            if (columnName) {
                const regex = new RegExp(`\\b${columnName}\\b`, 'g');
                // if (isExtensionFile || columnName.includes('ext:')) {
                //     columnName = `(source.feature->'properties'->>'${columnName}'::text)`;
                //     columnNamesReplaced.push(columnName);
                // }
                // else {

                //     if (columnName == "_id") {
                //         columnName = `source.${sourceDimension}${columnName}`;
                //     }
                //     else if (columnName == "_u_id" && sourceDimension == "edge") {
                //         columnName = `source.orig_node_id`;
                //     }
                //     else if (columnName == "_v_id" && sourceDimension == "edge") {
                //         columnName = `source.dest_node_id`;
                //     }
                //     else {
                //         columnName = `source.${columnName}`;
                //     }
                //     columnNamesReplaced.push(columnName);
                // }
                if (columnName == "geometry") {
                    columnName = `(source.feature->'${columnName}'::text)`;
                }
                else {
                    columnName = `(source.feature->'properties'->>'${columnName}'::text)`;
                }
                columnNamesReplaced.push(columnName);
                //remove the alias from the aggregate
                modifiedAggregate = modifiedAggregate.replace(regex, `${columnName}`).split(' as ')[0];

            }
        });


        return { alias: alias_name || Array.from(columnNames).join('_'), column: columnNamesReplaced, aggregate: modifiedAggregate };
    }

    /**
     * Replaces placeholders in a query text with corresponding values.
     * 
     * @param query - The query object containing the text and values.
     * @returns The query text with placeholders replaced by values.
     */
    substituteValues(query: any) {
        let text = query.text;
        let values = query.values;
        for (let i = 0; i < values.length; i++) {
            text = text.replace('$' + (i + 1), values[i]);
        }
        return text;
    }
}