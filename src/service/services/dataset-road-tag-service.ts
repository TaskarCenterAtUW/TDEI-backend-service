import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import QueryStream from "pg-query-stream";
import dbClient from "../../database/data-source";
import { AbstractOSWBackendRequest } from "../base/osw-backend-abstract";
import { BackendRequest } from "../interface/interfaces";
import { Readable } from "stream";
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

                //Get dataset details
                const datasetQuery = {
                    text: 'SELECT event_info as edges, node_info as nodes, zone_info as zones, ext_point_info as extensions_points, ext_line_info as extensions_lines, ext_polygon_info as extensions_polygons FROM content.dataset WHERE tdei_dataset_id = $1',
                    values: [params.target_dataset_id],
                }
                const datasetResult = await dbClient.query(datasetQuery);

                // Create a query stream, Extract dataset
                const query = new QueryStream('SELECT * FROM content.extract_dataset($1) ', [params.target_dataset_id]);
                // Execute the query
                const databaseClient = await dbClient.getDbClient();
                const stream = await dbClient.queryStream(databaseClient, query);
                //Build run context
                const dataObject = this.dataTypes.reduce((obj: any, dataType: any) => {
                    obj[dataType] = {
                        // Constant JSON string to be used for all the data types
                        constJson: this.buildAdditionalInfo(datasetResult.rows[0][`${dataType}`]),
                        stream: new Readable({ read() { } }),
                        firstFlag: true
                    };
                    return obj;
                }, {});
                // Create streams for each data type
                stream.on('data', async data => {
                    try {
                        await this.handleStreamDataEvent(data, dataObject, uploadContext);
                    } catch (error) {
                        await Utility.publishMessage(message, false, 'Error streaming data');
                        reject(`Error streaming data: ${error}`);
                    }
                });

                // Event listener for end event
                stream.on('end', async () => {
                    await this.handleStreamEndEvent(dataObject, uploadContext, message);
                    await dbClient.releaseDbClient(databaseClient);
                    resolve(true);
                });

                // Event listener for error event
                stream.on('error', async error => {
                    console.error('Error streaming data:', error);
                    await Utility.publishMessage(message, false, 'Error streaming data');
                    reject(`Error streaming query: ${error}`);
                });
            } catch (error) {
                console.error('Error executing query:', error);
                await Utility.publishMessage(message, false, 'Error executing query');
                reject(`Error executing query: ${error}`);
            }

        });
    }

}
