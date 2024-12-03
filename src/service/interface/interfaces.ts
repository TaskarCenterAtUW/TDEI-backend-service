import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import { AbstractDomainEntity, Prop } from "nodets-ms-core/lib/models";
import { InputException } from "../../exceptions/http/http-exceptions";
import { QueryConfig } from "pg";

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

export interface AttributeDetails { alias: string, column: string, aggregate?: string }

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

    private prefixColumns(query: string, prefix: string): string {
        // Regular expression to match column names (assuming column names are word characters)
        const columnPattern = /\b(?!geometry_target\b)(?!geometry_source\b)(\w+)\b(?=\s*(=|!=|>|<|>=|<=))/g;

        // Replace matched column names with 'source.' prepended
        return query.replace(columnPattern, `${prefix}.$1`);
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
    buildSpatialQuery() {

        this.checkForSqlInjection(this);
        this.cleanProperties();

        let target_table = '';
        let transform_geometry_target = '';
        let transform_geometry_source = '';
        let select_attributes = '';
        let target_select_required_fields = '';
        let group_by = '';

        //based on the target dimension, select the required fields, target table, and transform the geometry
        switch (this.target_dimension) {
            case 'edge':
                target_table = 'content.edge target';
                transform_geometry_target = 'target.edge_loc_3857';
                target_select_required_fields = 'target.edge_id, target.edge_loc, target.orig_node_id, target.dest_node_id';

                break;
            case 'node':
                target_table = 'content.node target';
                transform_geometry_target = 'target.node_loc_3857';
                target_select_required_fields = 'target.node_id, target.node_loc';
                break;
            case 'zone':
                target_table = 'content.zone target';
                transform_geometry_target = 'target.zone_loc_3857';
                target_select_required_fields = 'target.zone_id, target.zone_loc, target.node_ids';
                break;
            default:
                throw new InputException('Invalid target dimension');
        }

        let source_table = '';

        //based on the source dimension, select the source table, and transform the geometry
        switch (this.source_dimension) {
            case 'edge':
                source_table = 'content.edge source';
                transform_geometry_source = 'source.edge_loc_3857';
                break;
            case 'node':
                source_table = 'content.node source';
                transform_geometry_source = 'source.node_loc_3857';
                break;
            case 'zone':
                source_table = 'content.zone source';
                transform_geometry_source = 'source.zone_loc_3857';
                break;
            case 'point':
                source_table = 'content.extension_point source';
                transform_geometry_source = 'ST_Transform(source.point_loc, 3857)';
                break;
            case 'line':
                source_table = 'content.extension_line source';
                transform_geometry_source = 'ST_Transform(source.line_loc, 3857)';
                break;
            case 'polygon':
                source_table = 'content.extension_polygon source';
                transform_geometry_source = 'ST_Transform(source.polygon_loc, 3857)';
                break;
            default:
                throw new InputException('Invalid source dimension');
        }

        //compile the aggregate fields
        let aggregate_compiled: AttributeDetails[] = [];
        try {
            if (this.aggregate?.length) {
                aggregate_compiled = this.aggregate.map((aggregate) => {
                    const name = aggregate.split('(')[1].split(')')[0];
                    aggregate = aggregate.replace(name, `source.${name}`);
                    let columnName = `source.${name}`;

                    //if aggregate has alias then take the alias as the name
                    if (aggregate.toLowerCase().includes(' as ')) {
                        const alias_name = aggregate.toLowerCase().split(' as ')[1];
                        //remove the alias from the aggregate
                        aggregate = aggregate.toLowerCase().split(' as ')[0];
                        return { alias: `${alias_name}`, column: columnName, aggregate: aggregate };
                    }
                    else {
                        return { alias: `${name}`, column: columnName, aggregate: aggregate };
                    }
                });
            }
        } catch (error) {
            throw new InputException('Invalid aggregate syntax');
        }

        group_by = `${target_select_required_fields}, target.feature::jsonb`;

        //Transform the join geometry conditionally
        let join_condition_compiled = this.join_condition.replace('geometry_target', transform_geometry_target);
        join_condition_compiled = join_condition_compiled.replace('geometry_source', transform_geometry_source);

        //Filter filter attribute alias names
        if (this.join_filter_target && this.join_filter_target != '') {
            this.join_filter_target = this.prefixColumns(this.join_filter_target, 'target');
        }

        if (this.join_filter_source && this.join_filter_source != '') {
            this.join_filter_source = this.prefixColumns(this.join_filter_source, 'source');
        }
        //In the case filters are on geometry, transform the geometry
        this.join_filter_target = this.join_filter_target?.replace('geometry_target', transform_geometry_target);
        this.join_filter_source = this.join_filter_source?.replace('geometry_source', transform_geometry_source);

        //Select attributes
        select_attributes = `${target_select_required_fields}`;
        if (aggregate_compiled.length == 0) {
            select_attributes += `, (target.feature::jsonb)::json`;
        }

        const caseStatements = this.generateCaseStatements(aggregate_compiled);

        let param_counter = 1;
        let query: QueryConfig = {
            text:
                `
                SELECT 
                $${param_counter++} 
                ${aggregate_compiled.length ? `, JSONB_SET(
                    target.feature::jsonb,
                    '{properties}',
                    COALESCE(
                      target.feature::jsonb -> 'properties', '{}'::jsonb
                    ) || ($${param_counter++}),
                    TRUE
                  )::json AS feature` : `$${param_counter++}`
                    } 
                FROM $${param_counter++}
                LEFT JOIN $${param_counter++} on  $${param_counter++}
                AND source.tdei_dataset_id = '$${param_counter++}'
                ${this.join_filter_target ? `AND $${param_counter++}` : `$${param_counter++}`}
                ${this.join_filter_source ? `AND $${param_counter++}` : `$${param_counter++}`}
                WHERE
                target.tdei_dataset_id = '$${param_counter++}'
                GROUP BY $${param_counter++}
                `.replace(/\s+/g, ' ').trim(),
            values: [select_attributes, aggregate_compiled.length ? caseStatements : '', target_table, source_table,
                join_condition_compiled, this.source_dataset_id, this.join_filter_target ?? '', this.join_filter_source ?? '', this.target_dataset_id, group_by]
        };

        return this.substituteValues(query);
    }


    /**
     * Generates the case statements for aggregating and non-aggregating attributes.
     * 
     * @param aggregatedAttributes - An array of AttributeDetails objects representing the aggregated attributes.
     * @param nonAggregatedAttributes - An array of AttributeDetails objects representing the non-aggregated attributes.
     * @returns A string representing the generated case statements.
     */
    generateCaseStatements(aggregatedAttributes: AttributeDetails[]) {
        const aggCases = aggregatedAttributes.map(attr => {
            return `
            CASE
              WHEN ${attr.aggregate} FILTER (WHERE ${attr.column} IS NOT NULL) IS NOT NULL THEN
                JSONB_BUILD_OBJECT('ext:${attr.alias}', ${attr.aggregate} FILTER (WHERE ${attr.column} IS NOT NULL))
              ELSE '{}'::jsonb
            END
          `;
        });

        return [...aggCases].join(' || ');
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