import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import { AbstractOSWBackendRequest } from "../base/osw-backend-abstract";
import { BackendRequest } from "../interface/interfaces";
import { Utility } from "../../utility/utility";
import { QueryConfig } from "pg";
import { InputException } from "../../exceptions/http/http-exceptions";
import { PostgresError } from "../../constants/pg-error-constants";
import {
    assertNoRemovedUnionParameters,
    parseUnionConfig,
    UNION_REPORT_FILE_NAME,
} from "../../utility/union-config";

/**
 * Union 5.0 job: content.tdei_union_dataset(src_one, src_two, config jsonb).
 *
 * Parameters: tdei_dataset_id_one, tdei_dataset_id_two (required) and an
 * optional `config` object (see utility/union-config.ts and docs/union-FAQs.md).
 * The output zip holds the six OSW files plus union_report.json, the union's
 * run report (effective config, per-rule counts, warnings, totals).
 */
export class UnionQueryService extends AbstractOSWBackendRequest {

    constructor(public servicesConfig: any) {
        super(servicesConfig);
    }

    public async executeUnionQuery(message: QueueMessage) {
        return new Promise(async (resolve, reject) => {
            const backendRequest = message.data as BackendRequest;
            const params: any = backendRequest.parameters;
            var uploadContext = {
                containerName: "osw",
                filePath: `backend-jobs/${message.messageId}/${params.tdei_dataset_id_one}_${params.tdei_dataset_id_two}`,
                remoteUrls: [],
                zipUrl: "",
                outputFileName: `union_dataset-jobId_${message.messageId}.zip`
            };

            let config: Record<string, unknown>;
            try {
                assertNoRemovedUnionParameters(params);
                if (params.tdei_dataset_id_one === params.tdei_dataset_id_two) {
                    throw new InputException("tdei_dataset_id_one and tdei_dataset_id_two must be different datasets");
                }
                config = parseUnionConfig(params.config);
            } catch (error) {
                const errorMessage = error instanceof InputException ? error.message : 'Invalid config parameter';
                await Utility.publishMessage(message, false, errorMessage);
                return reject(errorMessage);
            }

            try {
                const unionQueryConfig: QueryConfig = {
                    text: 'SELECT * FROM content.tdei_union_dataset($1,$2,$3::jsonb)',
                    values: [
                        params.tdei_dataset_id_one,
                        params.tdei_dataset_id_two,
                        JSON.stringify(config),
                    ],
                }

                await this.process_upload_dataset(params.tdei_dataset_id_one, uploadContext, message, unionQueryConfig,
                    { jsonOutputs: { report: UNION_REPORT_FILE_NAME } });
                return resolve(true);

            } catch (error: any) {
                // The union validates the config before reading any data and rejects a bad
                // one with SQLSTATE 22023 and a message naming the stage, rule and field.
                if (error?.code === PostgresError.INVALID_PARAMETER_VALUE && error?.message) {
                    const errorMessage: string = error.message;   // e.g. "config.connect.rules[2].operation: is required — one of point, geometry"
                    await Utility.publishMessage(message, false, errorMessage);
                    return reject(errorMessage);
                }
                console.error('Error executing query:', error);
                await Utility.publishMessage(message, false, 'Error executing query');
                reject(`Error executing query: ${error}`);
            }
        });
    }

}
