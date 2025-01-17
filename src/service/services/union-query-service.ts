import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import QueryStream from "pg-query-stream";
import dbClient from "../../database/data-source";
import { AbstractOSWBackendRequest } from "../base/osw-backend-abstract";
import { BackendRequest } from "../interface/interfaces";
import { Readable } from "stream";
import { Utility } from "../../utility/utility";
import { InputException } from "../../exceptions/http/http-exceptions";

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
                outputFileName: ''
            };

            try {
                //Get dataset details
                const datasetQuery = {
                    text: 'SELECT name, event_info as edges, node_info as nodes, zone_info as zones, ext_point_info as extensions_points, ext_line_info as extensions_lines, ext_polygon_info as extensions_polygons FROM content.dataset WHERE tdei_dataset_id in ($1, $2)',
                    values: [params.tdei_dataset_id_one, params.tdei_dataset_id_two],
                }
                const datasetResult = await dbClient.query(datasetQuery);

                uploadContext.outputFileName = `union_dataset-jobId_${message.messageId}.zip`;

                // Create a query stream
                const query = new QueryStream(`SELECT * FROM content.tdei_union_dataset($1,$2) `, [params.tdei_dataset_id_one, params.tdei_dataset_id_two], { highWaterMark: 100 });
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

                if (error instanceof InputException) {
                    console.error('Error executing query:', error);
                    await Utility.publishMessage(message, false, error.message);
                    reject(`Error executing query: ${error}`);
                    return;
                }

                console.error('Error executing query:', error);
                await Utility.publishMessage(message, false, 'Error executing query');
                reject(`Error executing query: ${error}`);
            }
        });
    }

}
