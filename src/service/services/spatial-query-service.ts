import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import { AbstractOSWBackendRequest } from "../base/osw-backend-abstract";
import { BackendRequest, SpatialJoinRequestParams } from "../interface/interfaces";
import { Utility } from "../../utility/utility";
import { QueryConfig } from "pg";
import { InputException } from "../../exceptions/http/http-exceptions";
import DatabaseException from "../../exceptions/db/database-exceptions";

export class SpatialQueryService extends AbstractOSWBackendRequest {

    constructor(public servicesConfig: any) {
        super(servicesConfig);
    }

    public async executeSpatialQuery(message: QueueMessage) {
        return new Promise(async (resolve, reject) => {
            const backendRequest = message.data as BackendRequest;
            const params: any = backendRequest.parameters;
            var uploadContext = {
                containerName: "osw",
                filePath: `backend-jobs/${message.messageId}/${params.target_dataset_id}`,
                remoteUrls: [],
                zipUrl: "",
                outputFileName: `spatial_join-jobId_${message.messageId}.zip`
            };

            try {
                let spatialQueryService = SpatialJoinRequestParams.from(params);
                let dynamicQuery = spatialQueryService.buildSpatialQuery();

                const spatialQueryConfig: QueryConfig = {
                    text: 'SELECT * FROM content.tdei_dataset_spatial_join($1, $2, $3)',
                    values: [spatialQueryService.target_dataset_id, dynamicQuery, spatialQueryService.target_dimension],
                }

                await this.process_upload_dataset(spatialQueryService.target_dataset_id, uploadContext, message, spatialQueryConfig);
                return resolve(true);

            } catch (error) {
                console.error('Error executing query:', error);
                if (error instanceof InputException || error instanceof DatabaseException)
                    await Utility.publishMessage(message, false, error.message);
                else
                    await Utility.publishMessage(message, false, 'Error executing query');
                reject(`Error executing query: ${error}`);
            }
        });

    }

}
