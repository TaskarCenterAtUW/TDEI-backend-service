import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import QueryStream from "pg-query-stream";
import dbClient from "../../database/data-source";
import { AbstractOSWBackendRequest } from "../base/osw-backend-abstract";
import { BackendRequest, IUploadContext } from "../interface/interfaces";
import { Readable } from "stream";
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
                //Get dataset details
                const datasetQuery = {
                    text: 'SELECT event_info as edges, node_info as nodes, zone_info as zones, ext_point_info as extensions_points, ext_line_info as extensions_lines, ext_polygon_info as extensions_polygons FROM content.dataset WHERE tdei_dataset_id = $1',
                    values: [params.tdei_dataset_id],
                }
                const datasetResult = await dbClient.query(datasetQuery);

                //Validate bbox parameters
                if (typeof (params.bbox) == 'string') {
                    params.bbox = params.bbox.split(',').map(Number);
                }
                if (params.bbox.length != 4) {
                    await Utility.publishMessage(message, false, 'Invalid bbox parameters');
                    return reject('Invalid bbox parameters');
                }
                // Create a query stream
                const query = new QueryStream('SELECT * FROM content.bbox_intersect($1, $2, $3, $4, $5) ', [params.tdei_dataset_id, params.bbox[0], params.bbox[1], params.bbox[2], params.bbox[3]]);
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
