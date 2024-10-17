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

            // let spatialQueryService = SpatialJoinRequestParams.from(params);
            // let dynamicQuery = spatialQueryService.buildSpatialQuery();

            //Get dataset details
            const datasetQuery = {
                text: 'SELECT name, event_info as edges, node_info as nodes, zone_info as zones, ext_point_info as extensions_points, ext_line_info as extensions_lines, ext_polygon_info as extensions_polygons FROM content.dataset WHERE tdei_dataset_id in ($1, $2)',
                values: [params.tdei_dataset_id_one, params.tdei_dataset_id_two],
            }
            const datasetResult = await dbClient.query(datasetQuery);
            let datasetname_one: string = datasetResult.rows[0].name;
            let datasetname_two: string = datasetResult.rows[1].name;
            //Safe url name
            datasetname_one = datasetname_one.replace(/[^a-zA-Z0-9]/g, '_');
            datasetname_two = datasetname_two.replace(/[^a-zA-Z0-9]/g, '_');
            uploadContext.outputFileName = `${datasetname_one}_${datasetname_two}-union_dataset-jobId_${message.messageId}.zip`;

            // Create a query stream
            const query = new QueryStream('SELECT * FROM content.tdei_union_dataset($1, $2) ', [params.tdei_dataset_id_one, params.tdei_dimension_two]);
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
