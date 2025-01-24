import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import dbClient from "../../database/data-source";
import { AbstractOSWBackendRequest } from "../base/osw-backend-abstract";
import { BackendRequest } from "../interface/interfaces";
import { Utility } from "../../utility/utility";

export class DatasetRoadTagService extends AbstractOSWBackendRequest {

    constructor(public servicesConfig: any) {
        super(servicesConfig);
    }

    public async datasetTagRoad(message: QueueMessage) {
        return new Promise(async (resolve, reject) => {
            const backendRequest = message.data as BackendRequest;
            const params = backendRequest.parameters;
            var uploadContext = {
                containerName: "osw",
                filePath: `backend-jobs/${message.messageId}/${params.target_dataset_id}`,
                remoteUrls: [],
                zipUrl: "",
                outputFileName: `dataset-tag-road-${message.messageId}.zip`
            };

            try {
                //Tag the target dataset with the source dataset roads
                const updateQuery = {
                    text: 'SELECT content.dataset_tag_road($1, $2)',
                    values: [params.target_dataset_id, params.source_dataset_id],
                }
                //Update query
                await dbClient.query(updateQuery);

                const databaseClient = await dbClient.getDbClient();

                const resultQuery = {
                    text: 'SELECT * FROM content.extract_dataset_new($1)',
                    values: [params.target_dataset_id],
                }
                const result = await databaseClient.query(resultQuery);

                this.process_upload_dataset(params.target_dataset_id, uploadContext, message, databaseClient, result);
                return resolve(true);

            } catch (error) {
                console.error('Error executing query:', error);
                await Utility.publishMessage(message, false, 'Error executing query');
                reject(`Error executing query: ${error}`);
            }

        });
    }

}
