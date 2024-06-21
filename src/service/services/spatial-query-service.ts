import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import QueryStream from "pg-query-stream";
import dbClient from "../../database/data-source";
import { AbstractOSWBackendRequest } from "../base/osw-backend-abstract";
import { BackendRequest, SpatialJoinRequestParams } from "../interface/interfaces";
import { Readable } from "stream";
import { Utility } from "../../utility/utility";
import { InputException } from "../../exceptions/http/http-exceptions";

export class SpatialQueryService extends AbstractOSWBackendRequest {

    constructor(public servicesConfig: any) {
        super(servicesConfig);
    }

    public async executeSpatialQuery(message: QueueMessage) {
        const backendRequest = message.data as BackendRequest;
        const params: any = backendRequest.parameters;
        var uploadContext = {
            containerName: "osw",
            filePath: `backend-jobs/${message.messageId}/${params.target_dataset_id}`,
            remoteUrls: [],
            zipUrl: "",
            outputFileName: `spatial-query-${message.messageId}.zip`
        };

        try {

            let spatialQueryService = SpatialJoinRequestParams.from(params);
            let dynamicQuery = spatialQueryService.buildSpatialQuery();

            //Get dataset details
            const datasetQuery = {
                text: 'SELECT event_info as edges, node_info as nodes, zone_info as zones, ext_point_info as extensions_points, ext_line_info as extensions_lines, ext_polygon_info as extensions_polygons FROM content.dataset WHERE tdei_dataset_id = $1',
                values: [params.target_dataset_id],
            }
            const datasetResult = await dbClient.query(datasetQuery);


            // Create a query stream
            const query = new QueryStream('SELECT * FROM content.tdei_dataset_spatial_join($1, $2, $3) ', [spatialQueryService.target_dataset_id, dynamicQuery, spatialQueryService.target_dimension]);
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
                await this.handleStreamDataEvent(data, dataObject, uploadContext);
            });

            // Event listener for end event
            stream.on('end', async () => {
                await this.handleStreamEndEvent(dataObject, uploadContext, message);
                await dbClient.releaseDbClient(databaseClient);
            });

            // Event listener for error event
            stream.on('error', async error => {
                console.error('Error streaming data:', error);
                await Utility.publishMessage(message, false, 'Error streaming data : ' + error.message);
            });
        } catch (error) {

            if (error instanceof InputException) {
                console.error('Error executing query:', error);
                await Utility.publishMessage(message, false, error.message);
                return;
            }

            console.error('Error executing query:', error);
            await Utility.publishMessage(message, false, 'Error executing query');
        }
    }

}
