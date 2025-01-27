import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import { AbstractOSWBackendRequest } from "../base/osw-backend-abstract";
import { BackendRequest } from "../interface/interfaces";
import { Utility } from "../../utility/utility";
import { QueryConfig } from "pg";

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

            try {

                const unionQueryConfig: QueryConfig = {
                    text: 'SELECT * FROM content.tdei_union_dataset($1,$2)',
                    values: [params.tdei_dataset_id_one, params.tdei_dataset_id_two],
                }

                this.process_upload_dataset(params.tdei_dataset_id_one, uploadContext, message, unionQueryConfig);
                return resolve(true);

            } catch (error) {
                console.error('Error executing query:', error);
                await Utility.publishMessage(message, false, 'Error executing query');
                reject(`Error executing query: ${error}`);
            }
        });
    }

}
