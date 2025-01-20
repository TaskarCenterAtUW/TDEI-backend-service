import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import dbClient from "../../database/data-source";
import { AbstractOSWBackendRequest } from "../base/osw-backend-abstract";
import { BackendRequest, IUploadContext } from "../interface/interfaces";
import { Utility } from "../../utility/utility";

export class BboxIntersectService extends AbstractOSWBackendRequest {

    constructor(public servicesConfig: any) {
        super(servicesConfig);
    }

    /**
    * Executes a query to perform a bounding box intersection and streams the results to Azure Blob Storage.
    * @param message - The queue message containing the backend request.
    * @returns A Promise that resolves when the query execution and data streaming are completed.
    */
    public async bboxIntersect(message: QueueMessage) {
        return new Promise(async (resolve, reject) => {
            const backendRequest = message.data as BackendRequest;
            const params = backendRequest.parameters;
            var uploadContext: IUploadContext = {
                containerName: "osw",
                filePath: `backend-jobs/${message.messageId}/${params.tdei_dataset_id}`,
                remoteUrls: [],
                zipUrl: "",
                outputFileName: `bbox-intersect-${message.messageId}.zip`
            };

            try {
                //Validate bbox parameters
                if (typeof (params.bbox) == 'string') {
                    params.bbox = params.bbox.split(',').map(Number);
                }
                if (params.bbox.length != 4) {
                    await Utility.publishMessage(message, false, 'Invalid bbox parameters');
                    return reject('Invalid bbox parameters');
                }

                // Start a transaction
                const databaseClient = await dbClient.getDbClient();

                // Execute the Bbox query
                const resultQuery = {
                    text: 'SELECT * FROM content.bbox_intersect_new($1, $2, $3, $4, $5)',
                    values: [params.tdei_dataset_id, params.bbox[0], params.bbox[1], params.bbox[2], params.bbox[3]],
                }
                const result = await databaseClient.query(resultQuery);
                this.process_upload_dataset(params.tdei_dataset_id, uploadContext, message, databaseClient, result);

                return resolve(true);
            } catch (error) {
                console.error('Error executing query:', error);
                await Utility.publishMessage(message, false, 'Error executing query');
                reject(`Error executing query: ${error}`);
            }

        });
    }
}
