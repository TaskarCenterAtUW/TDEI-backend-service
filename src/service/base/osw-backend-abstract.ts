import { Utility } from "../../utility/utility";
import { IUploadContext } from "../interface/interfaces";
import { AbstractBackendService } from "./backend-abstract-base";
import AdmZip from "adm-zip";
import { Core } from "nodets-ms-core";
import { Readable } from "stream";
import stream from 'stream';
import { environment } from "../../environment/environment";
import QueryStream from "pg-query-stream";
import { QueryResult } from "pg";

export abstract class AbstractOSWBackendRequest extends AbstractBackendService {

    /**
     * Uploads a stream to an Azure Blob storage container.
     * 
     * @param stream - The stream to upload.
     * @param blobDetails - The details of the Azure Blob storage container.
     * @param fileName - The name of the file to create in the container.
     */
    async uploadStreamToAzureBlob(stream: Readable, blobDetails: any, fileName: string) {
        const client = Core.getStorageClient();
        const container = await client?.getContainer(blobDetails.containerName);
        const file = container?.createFile(`${blobDetails.filePath}/${fileName}`, "application/json");
        blobDetails.remoteUrls.push(file?.remoteUrl);
        await file?.uploadStream(stream);
    }

    /**
    * Zips the files specified in the upload context and uploads the zip archive to Azure Blob Storage.
    * @param uploadContext - The upload context containing the remote URLs of the files to be zipped.
    * @throws Error if the storage client is not configured.
    */
    public async zipStream(uploadContext: IUploadContext) {
        // Create a new instance of AdmZip
        const zip = new AdmZip();
        const storageClient = Core.getStorageClient();

        if (!storageClient) {
            throw new Error("Storage not configured");
        }

        const addFileToZip = async (url: string) => {
            const fileEntity = await storageClient.getFileFromUrl(url);
            const fileBuffer = await Utility.stream2buffer(await fileEntity.getStream());
            zip.addFile(url.split('/').pop()!, fileBuffer);
        };

        await Promise.all(uploadContext.remoteUrls.map(addFileToZip));

        // Prepare the zip archive
        const zipBuffer = zip.toBuffer();

        // Create a readable stream from the zip buffer
        const readStream = new stream.PassThrough();
        readStream.end(zipBuffer);

        await this.uploadStreamToAzureBlob(readStream, uploadContext, uploadContext.outputFileName ?? 'data.zip');
        uploadContext.zipUrl = uploadContext.remoteUrls.pop() as string;
    }

    /**
 * Handles the end event and performs necessary operations.
 * @param dataObject - The data object.
 * @param uploadContext - The upload context.
 * @param message - The message object.
 */
    public async handleStreamEndEvent(dataObject: any, file_name: string) {

        dataObject[file_name].stream.push("]}");
        dataObject[file_name].stream.push(null);

        console.log(`Finished streaming ${file_name}`);
    }

    /**
     * Zips and uploads the data to Azure Blob Storage.
     * @param uploadContext - The upload context.
     * @param message - The message object.
     */
    public async zipAndUpload(uploadContext: IUploadContext, message: any) {
        return new Promise(async (resolve, reject) => {
            await Utility.sleep(5000);
            //Verify if atlease one file is uploaded
            if (uploadContext.remoteUrls.length == 0) {
                await Utility.publishMessage(message, true, 'No data found for given prarameters.');
                return resolve(true);
            }
            await Utility.sleep(15000);
            this.zipStream(uploadContext).then(async () => {
                console.log('Zip file uploaded.');
                message.data.file_upload_path = uploadContext.zipUrl;
                await Utility.publishMessage(message, true, 'Dataset uploaded successfully!');
                resolve(true);
            }).catch(async (error) => {
                console.error('Error zipping data:', error);
                await Utility.publishMessage(message, false, 'Error zipping data');
                resolve(true);
            });
        });
    }
    /**
     * Handles the data event.
     *
     * @param data - The data object.
     * @param dataObject - The data object to be updated.
     * @param uploadContext - The upload context.
     */
    public async handleStreamDataEvent(data: any, file_name: string, dataObject: any, uploadContext: IUploadContext) {
        return new Promise(async (resolve, reject) => {
            try {
                dataObject[file_name].stream.push(`${dataObject[file_name].firstFlag ? dataObject[file_name].constJson : ","}${JSON.stringify(data["feature"])}`);

                if (dataObject[file_name].firstFlag) {
                    dataObject[file_name].firstFlag = false;
                    await this.uploadStreamToAzureBlob(dataObject[file_name].stream, uploadContext, `osw.${file_name}.geojson`);
                    console.log(`Uploaded ${file_name} to Storage`);
                    return resolve(true);
                }
            } catch (error) {
                console.error('Error streaming data:', error);
                return reject(`Error streaming data:, ${error}`);
            }
        });
    }

    private buildAdditionalInfo(info: any): string {
        const jsonParts: string[] = [];
        if (info) {
            Object.keys(info).forEach((key: any) => {
                const data = this.getData(info?.[key]);
                jsonParts.push(data && data != "" ? `"${key}": ${data},` : '');
            });
        }
        if (!jsonParts.toString().includes('$schema'))
            jsonParts.push(`"$schema": "${environment.oswSchemaUrl}",`);

        jsonParts.push('"type": "FeatureCollection", "features": [');

        return `{ ${jsonParts.join(' ').trimStart()} `;
    }

    private getData(data: any): string {
        return data && data != '' ? JSON.stringify(data) : "";
    }


    async process_upload_dataset(tdei_dataset_id: string, uploadContext: IUploadContext, message: any, databaseClient: any, result: QueryResult<any>) {
        return new Promise(async (resolve, reject) => {
            await databaseClient.query('BEGIN');
            //Get dataset details
            const datasetQuery = {
                text: 'SELECT event_info as edge, node_info as node, zone_info as zone, ext_point_info as point, ext_line_info as line, ext_polygon_info as polygon FROM content.dataset WHERE tdei_dataset_id = $1 limit 1',
                values: [tdei_dataset_id],
            }
            const datasetResult = await databaseClient.query(datasetQuery);
            //Get dataset extension details
            const datasetExtensionQuery = {
                text: 'SELECT file_meta, name FROM content.extension_file WHERE tdei_dataset_id = $1',
                values: [tdei_dataset_id],
            }
            const datasetExtensionResult = await databaseClient.query(datasetExtensionQuery);

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
                dataObject[row["name"]] = {
                    constJson: this.buildAdditionalInfo(row["file_meta"]),
                    stream: new Readable({ read() { } }),
                    firstFlag: true
                };
            });

            // Execute the query
            let success = true;
            for (const row of result.rows) {
                const { file_name, cursor_ref } = row;

                // Stream the cursor's data
                const query = new QueryStream(`FETCH ALL FROM "${cursor_ref}"`);
                const stream = await databaseClient.queryStream(databaseClient, query);

                stream.on('data', async (data: any) => {
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
                        stream.on('error', async (error: any) => {
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
            await databaseClient.query('COMMIT');
            await databaseClient.releaseDbClient(databaseClient);

            if (success)
                await this.zipAndUpload(uploadContext, message);
            resolve(true);
        });
    }
}
