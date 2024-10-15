import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import Ajv, { ErrorObject } from "ajv";
import services from '../services.json';
import { Utility } from "../utility/utility";
import { BboxIntersectService } from "./services/bbox-intersect-service";
import { DatasetRoadTagService } from "./services/dataset-road-tag-service";
import { SpatialQueryService } from "./services/spatial-query-service";
import { UnionQueryService } from "./services/union-query-service";


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
 * Represents a backend service that processes requests and performs various operations.
 */
export class BackendService {

    bboxService: BboxIntersectService;
    datasetTagRoadService: DatasetRoadTagService;
    spatialQueryService: SpatialQueryService;
    unionQueryService: UnionQueryService;

    constructor(private servicesConfig: any) {
        this.bboxService = new BboxIntersectService(this.servicesConfig);
        this.datasetTagRoadService = new DatasetRoadTagService(this.servicesConfig);
        this.spatialQueryService = new SpatialQueryService(this.servicesConfig);
        this.unionQueryService = new UnionQueryService(this.servicesConfig);
    }

    validate(message: any) {
        return validateMessage(message);
    }

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
            await Utility.publishMessage(message, false, error_message as string);
            return false;
        }

        // Find the service in the services array
        const service = this.servicesConfig.find((s: any) => s.service === message.data.service);
        if (!service) {
            console.error('Service not found');
            await Utility.publishMessage(message, false, 'Service not found');
            return false;
        }

        // Validate the parameters
        for (const param of service.parameters) {
            if (param.required && !(param.name in message.data.parameters)) {
                console.error(`Missing required parameter: ${param.name}`);
                await Utility.publishMessage(message, false, `Missing required parameter: ${param.name}`);
                return false;
            }
        }

        switch (service.service) {
            case "bbox_intersect":
                await this.bboxService.bboxIntersect(message);
                break;
            case "dataset_tag_road":
                await this.datasetTagRoadService.datasetTagRoad(message);
                break;
            case "spatial_join":
                await this.spatialQueryService.executeSpatialQuery(message);
                break;
            case "union_join":
                await this.unionQueryService.executeUnionQuery(message);
                break;
            default:
                await Utility.publishMessage(message, false, "Service not found");
                return false;
        }
        return true;
    }

}

const backendService = new BackendService(services);
export default backendService;


