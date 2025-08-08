import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import { BackendRequest } from "../interface/interfaces";
import { Utility } from "../../utility/utility";
import { QueryConfig } from "pg";
import { AbstractOSMBackendRequest } from "../base/osm-backend-abstract";
import dbClient from "../../database/data-source";

export class OswOsmQueryService extends AbstractOSMBackendRequest {

    constructor(public servicesConfig: any) {
        super(servicesConfig);
    }

    public async executeXMLQuery(message: QueueMessage) {
        return new Promise(async (resolve, reject) => {
            const backendRequest = message.data as BackendRequest;
            const params: any = backendRequest.parameters;
            var uploadContext = {
                containerName: "osw",
                filePath: `backend-jobs/${message.messageId}/${params.tdei_dataset_id}`,
                remoteUrl: "",
                tdei_dataset_id: params.tdei_dataset_id
            };

            try {

                const databaseClient = await dbClient.getDbClient();
                //Get dataset details
                const datasetQuery = {
                    text: 'SELECT tdei_dataset_id, name FROM content.dataset WHERE tdei_dataset_id = $1 limit 1',
                    values: [params.tdei_dataset_id],
                }
                const datasetResult = await databaseClient.query(datasetQuery);
                if (datasetResult.rowCount === 0) {
                    await dbClient.releaseDbClient(databaseClient);
                    return reject(`Dataset with ID ${params.tdei_dataset_id} not found.`);
                }

                const osmQueryConfig: QueryConfig = {
                    text: 'SELECT export_osm_xml as line FROM content.export_osm_xml($1)',
                    values: [params.tdei_dataset_id],
                }

                await this.process_upload_dataset(uploadContext, message, osmQueryConfig);
                message.data.file_upload_path = uploadContext.remoteUrl;
                await Utility.publishMessage(message, true, 'OSM uploaded successfully!');
                return resolve(true);

            } catch (error) {
                console.error('Error executing query:', error);
                await Utility.publishMessage(message, false, 'Error executing query');
                reject(`Error executing query: ${error}`);
            }
        });
    }

}
