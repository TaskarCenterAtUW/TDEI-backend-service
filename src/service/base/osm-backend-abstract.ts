import { Utility } from "../../utility/utility";
import { IUploadXMLContext } from "../interface/interfaces";
import { AbstractBackendService } from "./backend-abstract-base";
import { Core } from "nodets-ms-core";
import { Readable } from "stream";
import QueryStream from "pg-query-stream";
import { QueryConfig, QueryResult } from "pg";
import dbClient from "../../database/data-source";

export abstract class AbstractOSMBackendRequest extends AbstractBackendService {

    /**
     * Uploads a stream to an Azure Blob storage container.
     * 
     * @param stream - The stream to upload.
     * @param blobDetails - The details of the Azure Blob storage container.
     * @param fileName - The name of the file to create in the container.
     */
    async uploadStreamToAzureBlob(stream: Readable, blobDetails: IUploadXMLContext, fileName: string) {
        const client = Core.getStorageClient();
        const container = await client?.getContainer(blobDetails.containerName);
        const file = container?.createFile(`${blobDetails.filePath}/${fileName}`, "application/json");
        blobDetails.remoteUrl = file?.remoteUrl || "";
        await file?.uploadStream(stream);
    }

    /**
 * Handles the end event and performs necessary operations.
 * @param dataObject - The data object.
 * @param uploadContext - The upload context.
 * @param message - The message object.
 */
    public async handleStreamEndEvent(dataObject: any, file_name: string) {

        dataObject.stream.push(null);

        console.log(`Finished streaming ${file_name}`);
    }

    /**
     * Handles the data event.
     *
     * @param data - The data object.
     * @param dataObject - The data object to be updated.
     * @param uploadContext - The upload context.
     */
    public async handleStreamDataEvent(data: any, file_name: string, dataObject: any, uploadContext: IUploadXMLContext) {
        return new Promise(async (resolve, reject) => {
            try {
                dataObject.stream.push(`${data.line}\n`);
                if (dataObject.firstFlag) {
                    dataObject.firstFlag = false;

                    await this.uploadStreamToAzureBlob(dataObject.stream, uploadContext, `osm.${file_name}.xml`);
                    console.log(`Uploaded ${file_name} to Storage`);
                    resolve(true);
                }
            } catch (error) {
                console.error('Error streaming data:', error);
                reject(`Error streaming data:, ${error}`);
            }
        });
    }

    async process_upload_dataset(uploadContext: IUploadXMLContext, message: any, queryConfig: QueryConfig) {
        return new Promise(async (resolve, reject) => {
            let databaseClient: any = null;
            try {
                databaseClient = await dbClient.getDbClient();
                await databaseClient.query('BEGIN');

                let dataObject: any = {
                    stream: new Readable({ read() { } }),
                    firstFlag: true
                };

                const query = new QueryStream(`${queryConfig.text}`, queryConfig.values, {
                    batchSize: 5000 // optional
                });
                // Execute the query
                let success = true;

                const stream = await dbClient.queryStream(databaseClient, query);

                stream.on('data', async (data: any) => {
                    try {
                        await this.handleStreamDataEvent(data, uploadContext.tdei_dataset_id, dataObject, uploadContext);
                    } catch (error) {
                        stream.destroy();
                        reject(`Error streaming data: ${error}`);
                    }
                });

                try {
                    await new Promise((resolveCur, rejectCur) => {
                        // Event listener for end event
                        stream.on('end', async () => {
                            resolveCur(await this.handleStreamEndEvent(dataObject, uploadContext.tdei_dataset_id));
                        });

                        // Event listener for error event
                        stream.on('error', async (error: any) => {
                            console.error('Error streaming data:', error);
                            await Utility.publishMessage(message, false, 'Error streaming data');
                            stream.destroy();
                            rejectCur(error);
                        });
                    });
                } catch (error) {
                    console.error('Error in stream processing:', error);
                    success = false;
                    reject(`Error in stream processing: ${error}`);
                }

                // Commit the transaction
                await databaseClient.query('COMMIT');
                //Wait for blob to be available
                if (success) {
                    await Utility.waitForBlobAvailability(uploadContext.tdei_dataset_id, uploadContext.remoteUrl);
                }

                resolve(true);
            } catch (error) {
                console.error('Error executing query:', error);
                reject(`Error executing query: ${error}`);
            } finally {
                // Always release the database connection
                if (databaseClient) {
                    await dbClient.releaseDbClient(databaseClient);
                }
            }
        });
    }
}
