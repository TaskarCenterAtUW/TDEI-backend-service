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
    transform_target!: string;
    @Prop()
    transform_source!: string;
    @Prop()
    filter_target!: string;
    @Prop()
    filter_source!: string;
    @Prop()
    aggregate: string[] = []; //attributes from source dimension to be aggregated
    @Prop()
    attributes: string[] = []; //attributes from source dimension to be added to the target feature properties

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
        let extension_attributes_required = this.aggregate?.length || this.attributes?.length;

        //based on the target dimension, select the required fields, target table, and transform the geometry
        switch (this.target_dimension) {
            case 'edge':
                target_table = 'content.edge target';
                transform_geometry_target = 'ST_Transform(target.edge_loc, 3857)';
                target_select_required_fields = 'target.edge_id, target.edge_loc, target.orig_node_id, target.dest_node_id';

                break;
            case 'node':
                target_table = 'content.node target';
                transform_geometry_target = 'ST_Transform(target.node_loc, 3857)';
                target_select_required_fields = 'target.node_id, target.node_loc';
                break;
            case 'zone':
                target_table = 'content.zone target';
                transform_geometry_target = 'ST_Transform(target.zone_loc, 3857)';
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
                transform_geometry_source = 'ST_Transform(source.edge_loc, 3857)';
                break;
            case 'node':
                source_table = 'content.node source';
                transform_geometry_source = 'ST_Transform(source.node_loc, 3857)';
                break;
            case 'zone':
                source_table = 'content.zone source';
                transform_geometry_source = 'ST_Transform(source.zone_loc, 3857)';
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
        let aggregate_compiled: { name: string, aggregate: string }[] = [];
        try {
            if (this.aggregate?.length) {
                aggregate_compiled = this.aggregate.map((aggregate) => {
                    const name = aggregate.split('(')[1].split(')')[0];
                    aggregate = aggregate.replace(name, `source.${name}`);

                    //if aggregate has alias then take the alias as the name
                    if (aggregate.toLowerCase().includes(' as ')) {
                        const alias_name = aggregate.toLowerCase().split(' as ')[1];
                        //remove the alias from the aggregate
                        aggregate = aggregate.toLowerCase().split(' as ')[0];
                        return { name: alias_name, aggregate };
                    }
                    else {
                        return { name, aggregate };
                    }
                });
            }
        } catch (error) {
            throw new InputException('Invalid aggregate syntax');
        }

        //compile the attribute fields
        let attribute_names_compiled: { name: string, attribute: string }[] = [];
        if (this.attributes?.length) {
            attribute_names_compiled = this.attributes.map((attribute) => {
                if (attribute.toLowerCase().includes(' as ')) {
                    const alias_name = attribute.toLowerCase().split(' as ')[1];
                    //remove the alias from the attribute
                    attribute = attribute.toLowerCase().split(' as ')[0];
                    return { name: alias_name, attribute };
                }
                return { name: attribute, attribute };
            });
        }

        //extension fields to be added to the feature properties
        let extension_attributes = aggregate_compiled.map((aggregate) => { return `'ext:${aggregate.name}', ${aggregate.aggregate}` }).join(', ');
        extension_attributes = extension_attributes && attribute_names_compiled.length ? `${extension_attributes}, ` : extension_attributes;
        extension_attributes += attribute_names_compiled.map((attribute) => { return `'ext:${attribute.name}', source.${attribute.attribute}` }).join(', ');

        group_by = `${target_select_required_fields}`;
        //if attribute fields exists, then add them to the group by
        if (attribute_names_compiled.length) {
            group_by += `, ${attribute_names_compiled.map((attribute) => { return `source.${attribute.attribute}` }).join(', ')}`;
        }

        if (extension_attributes.length) {
            //to add extension fields to the feature properties, feature to be added to group by
            group_by += `, target.feature:: jsonb`;
        }


        let target_transform_compiled = undefined;
        let source_transform_compiled = undefined;
        //Transform the target and source geometries
        if (this.transform_target) {
            target_transform_compiled = this.transform_target.replace('geometry_target', transform_geometry_target);
        }
        if (this.transform_source) {
            source_transform_compiled = this.transform_source.replace('geometry_source', transform_geometry_source);
        }
        //Transform the join geometry conditionally
        let join_condition_compiled = this.join_condition.replace('geometry_target', target_transform_compiled ?? transform_geometry_target);
        join_condition_compiled = join_condition_compiled.replace('geometry_source', source_transform_compiled ?? transform_geometry_source);

        //Filter filter attribute alias names
        if (this.filter_target && this.filter_target != '') {
            this.filter_target = this.prefixColumns(this.filter_target, 'target');
        }

        if (this.filter_source && this.filter_source != '') {
            this.filter_source = this.prefixColumns(this.filter_source, 'source');
        }
        //In the case filters are on geometry, transform the geometry
        let target_filter = this.filter_target?.replace('geometry_target', transform_geometry_target);
        let source_filter = this.filter_source?.replace('geometry_source', transform_geometry_source);

        //Select attributes
        select_attributes = `${target_select_required_fields}`;
        if (extension_attributes == '') {
            select_attributes += `, (target.feature::jsonb)::json`;
            group_by += `, target.feature::jsonb`;
        }

        let param_counter = 1;
        let query: QueryConfig = {
            text:
                `
                SELECT 
                $${param_counter++} 
                ${extension_attributes_required ? `, jsonb_set(
                    target.feature::jsonb, 
                    '{properties}', 
                    target.feature::jsonb->'properties' || jsonb_build_object(
                        $${param_counter++}
                    ), 
                    true
                )::json AS feature` : `$${param_counter++}`
                    } 
                FROM $${param_counter++}
                INNER JOIN $${param_counter++} on  $${param_counter++}
                WHERE
                target.tdei_dataset_id = '$${param_counter++}'
                AND source.tdei_dataset_id = '$${param_counter++}'
                ${target_filter ? `AND $${param_counter++}` : `$${param_counter++}`}
                ${source_filter ? `AND $${param_counter++}` : `$${param_counter++}`}
                GROUP BY $${param_counter++}
                `.replace(/\s+/g, ' ').trim(),
            values: [select_attributes, extension_attributes ?? '', target_table, source_table,
                join_condition_compiled, this.target_dataset_id, this.source_dataset_id, target_filter ?? '', source_filter ?? '', group_by]
        };

        return this.substituteValues(query);
    }

    substituteValues(query: any) {
        let text = query.text;
        let values = query.values;
        for (let i = 0; i < values.length; i++) {
            text = text.replace('$' + (i + 1), values[i]);
        }
        return text;
    }

}