import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import { Core } from "nodets-ms-core";
import { environment } from "../environment/environment";
import QueryStream from "pg-query-stream";
import dbClient from "../database/data-source";
import { Readable } from "stream";
import Ajv, { ErrorObject } from "ajv";
import services from '../services.json';
import { Utility } from "../utility/utility";
import AdmZip from "adm-zip";
import stream from 'stream';


// Define the schema for the incoming message
const messageSchema = {
    type: 'object',
    properties: {
        messageId: { type: 'string' },
        messageType: { type: 'string' },
        data: {
            type: 'object',
            properties: {
                service: { type: 'string' },
                parameters: { type: 'object' },
                user_id: { type: 'string' },
            },
            required: ['service', 'parameters', 'user_id'],
            additionalProperties: false
        }
    },
    required: ['messageId', 'messageType', 'data'],
    additionalProperties: false
};

// Compile the schema
const ajv = new Ajv({ allErrors: true, removeAdditional: true });
const validateMessage = ajv.compile(messageSchema);

/**
 * Represents a backend request.
 */
export class BackendRequest {
    service!: string;
    parameters!: any;
    user_id!: string;
    constructor(init: Partial<BackendRequest>) {
        Object.assign(this, init);
    }
}

/**
 * Represents a backend request.
 */
export interface IBackendRequest {
    /**
     * Processes the backend request.
     * @param message The queue message containing the request data.
     * @returns A promise that resolves to a boolean indicating the success of the request processing.
     */
    backendRequestProcessor(message: QueueMessage): Promise<boolean>;
}

/**
 * Represents a backend service that processes requests and performs various operations.
 */
export class BackendService {
    constructor() { }


    /**
     * Processes the backend request.
     * 
     * @param message - The queue message containing the request.
     * @returns A promise that resolves to a boolean indicating the success of the request processing.
     */
    public async backendRequestProcessor(message: QueueMessage): Promise<boolean> {

        // Validate the message
        const validMessage = validateMessage(message);
        if (!validMessage) {
            const error_message = validateMessage.errors?.map((error: ErrorObject) => error.instancePath.replace('/', "") + " " + error.message).join(", \n");
            console.error(error_message);
            await this.publishMessage(message, false, error_message as string);
            return false;
        }

        // Find the service in the services array
        const service = services.find(s => s.service === message.data.service);
        if (!service) {
            console.error('Service not found');
            await this.publishMessage(message, false, 'Service not found');
            return false;
        }

        // Validate the parameters
        for (const param of service.parameters) {
            if (param.required && !(param.name in message.data.parameters)) {
                console.error(`Missing required parameter: ${param.name}`);
                await this.publishMessage(message, false, `Missing required parameter: ${param.name}`);
                return false;
            }
        }


        switch (service.service) {
            case "bbox_intersect":
                await this.bboxIntersect(message);
                break;
            default:
                await this.publishMessage(message, false, "Service not found");
                return false;
        }
        return true;
    }

    /**
     * Uploads a stream to an Azure Blob storage container.
     * 
     * @param stream - The stream to upload.
     * @param blobDetails - The details of the Azure Blob storage container.
     * @param fileName - The name of the file to create in the container.
     */
    public async uploadStreamToAzureBlob(stream: Readable, blobDetails: any, fileName: string) {
        const client = Core.getStorageClient();
        const container = await client?.getContainer(blobDetails.containerName);
        const file = container?.createFile(`${blobDetails.filePath}/${fileName}`, "application/json");
        blobDetails.remoteUrls.push(file?.remoteUrl);
        await file?.uploadStream(stream);
    }


    /**
     * Executes a query to perform a bounding box intersection and streams the results to Azure Blob Storage.
     * @param message - The queue message containing the backend request.
     * @returns A Promise that resolves when the query execution and data streaming are completed.
     */
    public async bboxIntersect(message: QueueMessage) {
        const backendRequest = message.data as BackendRequest;
        const params = backendRequest.parameters;
        var uploadContext = {
            containerName: "osw",
            filePath: `backend-jobs/${message.messageId}/${params.tdei_dataset_id}`,
            remoteUrls: [],
            zipUrl: ""
        };
        let success = false;

        try {
            const datasetQuery = {
                text: 'SELECT event_info, node_info, ext_point_info, ext_line_info, ext_polygon_info FROM content.dataset WHERE tdei_dataset_id = $1',
                values: [params.tdei_dataset_id],
            }
            const datasetResult = await dbClient.query(datasetQuery);

            // Create a query stream
            const query = new QueryStream('SELECT * FROM content.bbox_intersect($1, $2, $3, $4, $5) ', [params.tdei_dataset_id, params.bbox[0], params.bbox[1], params.bbox[2], params.bbox[3]]);
            // Execute the query
            const stream = await dbClient.queryStream(query);
            // Constant JSON string to be used for all the data types
            // let constJson = `{ "DataSource": { "name": "TDEI" }, "type": "FeatureCollection", "features": [`;
            const constJson: { [key: string]: string } = {
                edges: this.buildAdditionalInfo(datasetResult.rows[0].event_info),
                nodes: this.buildAdditionalInfo(datasetResult.rows[0].node_info),
                extensions_points: this.buildAdditionalInfo(datasetResult.rows[0].ext_point_info),
                extensions_polygons: this.buildAdditionalInfo(datasetResult.rows[0].ext_polygon_info),
                extensions_lines: this.buildAdditionalInfo(datasetResult.rows[0].ext_line_info)
            };

            // Event listener for data event
            const dataTypes = ['edges', 'nodes', 'extensions_points', 'extensions_polygons', 'extensions_lines'];
            // Create readable streams for edges and nodes
            const streams: { [key: string]: Readable } = {
                edges: new Readable({ read() { } }),
                nodes: new Readable({ read() { } }),
                extensions_points: new Readable({ read() { } }),
                extensions_polygons: new Readable({ read() { } }),
                extensions_lines: new Readable({ read() { } })
            };
            // Flag to check if the first data is being streamed
            const firstFlags: { [key: string]: boolean } = {
                edges: true,
                nodes: true,
                extensions_points: true,
                extensions_polygons: true,
                extensions_lines: true
            };
            // Create streams for each data type
            stream.on('data', async data => {
                let input_dataType: string = "";
                // Loop through the data types
                for (const dataType of dataTypes) {
                    // Check if the data type is present in the data
                    if (data[dataType]) {
                        input_dataType = dataType;
                        // Push the data to the respective stream
                        streams[dataType].push(`${firstFlags[dataType] ? constJson[dataType] : ","}${JSON.stringify(data[dataType])}`);
                    }
                }
                if (firstFlags[input_dataType]) {
                    firstFlags[input_dataType] = false;
                    await this.uploadStreamToAzureBlob(streams[input_dataType], uploadContext, `${input_dataType.replace("extensions_", "")}.OSW.geojson`)
                        .then(() => console.log(`Uploaded ${input_dataType} to Storage`));
                }
            });

            // Event listener for end event
            stream.on('end', async () => {
                //loop streams
                for (const dataType of dataTypes) {
                    //push null to the streams
                    streams[dataType].push("]}");
                    streams[dataType].push(null);
                }
                console.log(uploadContext.remoteUrls.map((obj: any) => obj.url).join(","));
                console.log('All result sets streamed and uploaded.');

                // setTimeout(() => {
                //     this.zipStream(uploadContext).then(async () => {
                //         console.log('Zip file uploaded.');
                //         success = true;
                //         message.data.file_upload_path = uploadContext.zipUrl;
                //         await this.publishMessage(message, success, 'Data streamed and uploaded to Azure Blob Storage');
                //     }).catch(async (error) => {
                //         console.error('Error zipping data:', error);
                //         await this.publishMessage(message, false, 'Error zipping data');
                //     }
                //     );
                // }, 5000);
                await Utility.sleep(10000);
                this.zipStream(uploadContext).then(async () => {
                    console.log('Zip file uploaded.');
                    success = true;
                    message.data.file_upload_path = uploadContext.zipUrl;
                    await this.publishMessage(message, success, 'Dataset uploaded successfully!');
                }).catch(async (error) => {
                    console.error('Error zipping data:', error);
                    await this.publishMessage(message, false, 'Error zipping data');
                }
                );

            });

            // Event listener for error event
            stream.on('error', async error => {
                console.error('Error streaming data:', error);
                await this.publishMessage(message, false, 'Error streaming data');
            });
        } catch (error) {
            console.error('Error executing query:', error);
            await this.publishMessage(message, false, 'Error executing query');
        }
    }

    /**
     * Zips the files specified in the upload context and uploads the zip archive to Azure Blob Storage.
     * @param uploadContext - The upload context containing the remote URLs of the files to be zipped.
     * @throws Error if the storage client is not configured.
     */
    public async zipStream(uploadContext: any) {
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

        await this.uploadStreamToAzureBlob(readStream, uploadContext, 'bbox_intersect.zip');
        uploadContext.zipUrl = uploadContext.remoteUrls.pop() as string;
    }

    /**
     * Publishes a message to the backend response topic.
     * 
     * @param message - The original queue message.
     * @param success - Indicates whether the operation was successful.
     * @param resText - The response text.
     */
    public async publishMessage(message: QueueMessage, success: boolean, resText: string) {
        var data = {
            message: resText,
            success: success,
            file_upload_path: message.data.file_upload_path
        }
        message.data = data;
        await Core.getTopic(environment.eventBus.backendResponseTopic as string).publish(message);
    }

    private buildAdditionalInfo(info: any): string {
        const properties = ['dataSource', 'region', 'dataTimestamp', 'pipelineVersion'];
        const jsonParts = properties.map(prop => {
            const data = this.getData(info?.[prop]);
            return data && data != "" ? `"${prop}": ${data},` : '';
        });

        jsonParts.push('"type": "FeatureCollection", "features": [');

        return `{ ${jsonParts.join(' ').trimStart()} `;
    }

    private getData(data: any): string {
        return data && data != '' ? JSON.stringify(data) : "";
    }

}

const backendService = new BackendService();
export default backendService;

