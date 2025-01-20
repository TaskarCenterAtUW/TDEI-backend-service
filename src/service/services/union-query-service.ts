import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import dbClient from "../../database/data-source";
import { AbstractOSWBackendRequest } from "../base/osw-backend-abstract";
import { BackendRequest } from "../interface/interfaces";
import { Utility } from "../../utility/utility";

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
                const databaseClient = await dbClient.getDbClient();

                const resultQuery = {
                    text: 'SELECT * FROM content.tdei_union_dataset($1,$2)',
                    values: [params.tdei_dataset_id_one, params.tdei_dataset_id_two],
                }
                const result = await databaseClient.query(resultQuery);

                this.process_upload_dataset(params.tdei_dataset_id_one, uploadContext, message, databaseClient, result);
                return resolve(true);

            } catch (error) {
                console.error('Error executing query:', error);
                await Utility.publishMessage(message, false, 'Error executing query');
                reject(`Error executing query: ${error}`);
            }
        });
    }

}
