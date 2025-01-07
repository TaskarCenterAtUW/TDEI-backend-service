import { Utility } from "../../utility/utility";
import { IUploadContext } from "../interface/interfaces";
import { AbstractBackendService } from "./backend-abstract-base";
import AdmZip from "adm-zip";
import { Core } from "nodets-ms-core";
import { Readable } from "stream";
import stream from 'stream';
import { environment } from "../../environment/environment";

export abstract class AbstractOSWBackendRequest extends AbstractBackendService {
    dataTypes = ['edges', 'nodes', 'zones', 'extensions_points', 'extensions_polygons', 'extensions_lines'];

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
    public async handleStreamEndEvent(dataObject: any, uploadContext: IUploadContext, message: any) {
        return new Promise(async (resolve, reject) => {
            for (const dataType of this.dataTypes) {
                dataObject[dataType].stream.push("]}");
                dataObject[dataType].stream.push(null);
            }
            console.log('All result sets streamed and uploaded.');
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
    public async handleStreamDataEvent(data: any, dataObject: any, uploadContext: IUploadContext) {
        return new Promise(async (resolve, reject) => {
            try {
                let input_dataType = "";
                for (const dataType of this.dataTypes) {
                    if (data[dataType]) {
                        input_dataType = dataType;
                        dataObject[dataType].stream.push(`${dataObject[dataType].firstFlag ? dataObject[dataType].constJson : ","}${JSON.stringify(data[dataType])}`);
                    }
                }
                if (dataObject[input_dataType].firstFlag) {
                    dataObject[input_dataType].firstFlag = false;
                    await this.uploadStreamToAzureBlob(dataObject[input_dataType].stream, uploadContext, `osw.${input_dataType.replace("extensions_", "")}.geojson`);
                    console.log(`Uploaded ${input_dataType} to Storage`);
                    return resolve(true);
                }
            } catch (error) {
                console.error('Error streaming data:', error);
                return reject(`Error streaming data:, ${error}`);
            }
        });
    }

    buildAdditionalInfo(info: any): string {
        const properties = ['dataSource', 'region', 'dataTimestamp', 'pipelineVersion', '$schema'];
        let jsonParts = properties.map(prop => {
            const data = this.getData(info?.[prop]);
            return data && data != "" ? `"${prop}": ${data},` : '';
        });

        if (!jsonParts.toString().includes('$schema'))
            jsonParts.push(`"$schema": "${environment.oswSchemaUrl}",`);

        jsonParts.push('"type": "FeatureCollection", "features": [');

        return `{ ${jsonParts.join(' ').trimStart()} `;
    }

    getData(data: any): string {
        return data && data != '' ? JSON.stringify(data) : "";
    }
}
