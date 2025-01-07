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
                await databaseClient.query('BEGIN');
                //Get dataset details
                const datasetQuery = {
                    text: 'SELECT event_info as edge, node_info as node, zone_info as zone, ext_point_info as point, ext_line_info as line, ext_polygon_info as polygon FROM content.dataset WHERE tdei_dataset_id = $1 limit 1',
                    values: [params.tdei_dataset_id],
                }
                const datasetResult = await databaseClient.query(datasetQuery);
                //Get dataset extension details
                const datasetExtensionQuery = {
                    text: 'SELECT file_meta, name FROM content.extension_file WHERE tdei_dataset_id = $1 limit 1',
                    values: [params.tdei_dataset_id],
                }
                const datasetExtensionResult = await databaseClient.query(datasetExtensionQuery);

                // Execute the Bbox query
                const resultQuery = {
                    text: 'SELECT * FROM content.bbox_intersect_new($1, $2, $3, $4, $5)',
                    values: [params.tdei_dataset_id, params.bbox[0], params.bbox[1], params.bbox[2], params.bbox[3]],
                }
                const result = await databaseClient.query(resultQuery);

                //Build file metadata
                let dataObject: any = [];
                datasetResult.rows.forEach((row: any) => {
                    Object.keys(row).forEach((key: any) => {
                        dataObject[key] = {
                            constJson: this.buildAdditionalInfo(row[key]),
                            stream: new Readable({ read() { } }),
                            firstFlag: true
                        };
                    });
                });
                datasetExtensionResult.rows.forEach((row: any) => {
                    Object.keys(row).forEach((key: any) => {
                        dataObject[key] = {
                            constJson: this.buildAdditionalInfo(row[key]),
                            stream: new Readable({ read() { } }),
                            firstFlag: true
                        };
                    });
                });

                // Execute the query
                let success = true;
                for (const row of result.rows) {
                    const { file_name, cursor_ref } = row;

                    // Stream the cursor's data
                    const query = new QueryStream(`FETCH 100 FROM "${cursor_ref}"`);
                    const stream = await dbClient.queryStream(databaseClient, query);

                    stream.on('data', async data => {
                        try {
                            await this.handleStreamDataEvent(data, file_name, dataObject, uploadContext);
                        } catch (error) {
                            await Utility.publishMessage(message, false, 'Error streaming data');
                            reject(`Error streaming data: ${error}`);
                        }
                    });

                    try {
                        await new Promise((resolveCur, rejectCur) => {
                            // Event listener for end event
                            stream.on('end', async () => {
                                resolveCur(await this.handleStreamEndEvent(dataObject, file_name));
                            });

                            // Event listener for error event
                            stream.on('error', async error => {
                                console.error('Error streaming data:', error);
                                await Utility.publishMessage(message, false, 'Error streaming data');
                                rejectCur(error);
                            });
                        });
                    } catch (error) {
                        success = false;
                        break;
                    }

                    // Close the cursor
                    await databaseClient.query(`CLOSE "${cursor_ref}"`);
                }

                // Commit the transaction
                await dbClient.query('COMMIT');
                await dbClient.releaseDbClient(databaseClient);

                if (success)
                    await this.zipAndUpload(uploadContext, message);
                return resolve(true);
            } catch (error) {
                console.error('Error executing query:', error);
                await Utility.publishMessage(message, false, 'Error executing query');
                reject(`Error executing query: ${error}`);
            }

        });
    }
}
