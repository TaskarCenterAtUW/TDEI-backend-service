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

export interface IUploadContext {
    containerName: string;
    filePath: string;
    remoteUrls: string[];
    zipUrl: string;
    outputFileName?: string;
}


abstract class AbstractBackendService {
    constructor(public servicesConfig: any) { }
    validate(message: any) {
        return validateMessage(message);
    }

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
            file_upload_path: message.data.file_upload_path ?? ""
        }
        message.data = data;
        await Core.getTopic(environment.eventBus.backendResponseTopic as string).publish(message);
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
}

/**
 * Represents a backend service that processes requests and performs various operations.
 */
export class BackendService extends AbstractBackendService {

    dataTypes = ['edges', 'nodes', 'zones', 'extensions_points', 'extensions_polygons', 'extensions_lines'];

    constructor(public servicesConfig: any) { super(servicesConfig); }

    /**
     * Processes the backend request.
     * 
     * @param message - The queue message containing the request.
     * @returns A promise that resolves to a boolean indicating the success of the request processing.
     */
    public async backendRequestProcessor(message: QueueMessage): Promise<boolean> {

        // Validate the message
        const validMessage = this.validate(message);
        if (!validMessage) {
            const error_message = validateMessage.errors?.map((error: ErrorObject) => error.instancePath.replace('/', "") + " " + error.message).join(", \n");
            console.error(error_message);
            await this.publishMessage(message, false, error_message as string);
            return false;
        }

        // Find the service in the services array
        const service = this.servicesConfig.find((s: any) => s.service === message.data.service);
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
            case "dataset_tag_road":
                await this.datasetTagRoad(message);
                break;
            default:
                await this.publishMessage(message, false, "Service not found");
                return false;
        }
        return true;
    }



    public async datasetTagRoad(message: QueueMessage) {
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
                await this.publishMessage(message, false, 'Error streaming data');
            });
        } catch (error) {
            console.error('Error executing query:', error);
            await this.publishMessage(message, false, 'Error executing query');
        }
    }
    /**
     * Executes a query to perform a bounding box intersection and streams the results to Azure Blob Storage.
     * @param message - The queue message containing the backend request.
     * @returns A Promise that resolves when the query execution and data streaming are completed.
     */
    public async bboxIntersect(message: QueueMessage) {
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
                await this.publishMessage(message, false, 'Invalid bbox parameters');
                return;
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
                await this.publishMessage(message, false, 'Error streaming data');
            });
        } catch (error) {
            console.error('Error executing query:', error);
            await this.publishMessage(message, false, 'Error executing query');
        }
    }

    /**
     * Handles the end event and performs necessary operations.
     * @param dataObject - The data object.
     * @param uploadContext - The upload context.
     * @param message - The message object.
     */
    public async handleStreamEndEvent(dataObject: any, uploadContext: IUploadContext, message: any) {

        for (const dataType of this.dataTypes) {
            dataObject[dataType].stream.push("]}");
            dataObject[dataType].stream.push(null);
        }
        console.log('All result sets streamed and uploaded.');
        //Verify if atlease one file is uploaded
        if (uploadContext.remoteUrls.length == 0) {
            await this.publishMessage(message, true, 'No data found for given prarameters.');
            return;
        }
        await Utility.sleep(15000);
        this.zipStream(uploadContext).then(async () => {
            console.log('Zip file uploaded.');
            message.data.file_upload_path = uploadContext.zipUrl;
            await this.publishMessage(message, true, 'Dataset uploaded successfully!');
        }).catch(async (error) => {
            console.error('Error zipping data:', error);
            await this.publishMessage(message, false, 'Error zipping data');
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
                await this.uploadStreamToAzureBlob(dataObject[input_dataType].stream, uploadContext, `${input_dataType.replace("extensions_", "")}.OSW.geojson`)
                    .then(() => console.log(`Uploaded ${input_dataType} to Storage`));
            }
        } catch (error) {
            console.error('Error streaming data:', error);
        }
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

const backendService = new BackendService(services);
export default backendService;


