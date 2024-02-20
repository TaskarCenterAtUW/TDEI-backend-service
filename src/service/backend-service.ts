import { QueueMessage } from "nodets-ms-core/lib/core/queue";
import { Core } from "nodets-ms-core";
import { environment } from "../environment/environment";
import { PermissionRequest } from "nodets-ms-core/lib/core/auth/model/permission_request";

export class BackendRequest {
    service!: string;
    params!: any;
    user_id!: string;
    constructor(init: Partial<BackendRequest>) {
        Object.assign(this, init);
    }
}

export interface IBackendRequest {
    backendRequestProcessor(message: QueueMessage): Promise<boolean>;
}


export class BackendService {
    constructor() { }

    public async backendRequestProcessor(message: QueueMessage): Promise<boolean> {
        const backendRequest = message.data as BackendRequest;
        const service = backendRequest.service;
        const params = backendRequest.params;
        const user_id = backendRequest.user_id;
        const tdei_project_group_id = '';

        //authorize the user
        const authProvider = Core.getAuthorizer({ provider: "Hosted", apiUrl: environment.authPermissionUrl });
        const permissionRequest = new PermissionRequest({
            userId: user_id as string,
            projectGroupId: tdei_project_group_id,
            permssions: ["tdei_admin", "poc", "osw_data_generator", "flex_data_generator", "pathways_data_generator"],
            shouldSatisfyAll: false
        });

        const response = await authProvider?.hasPermission(permissionRequest);
        if (!response) {
            await this.publishMessage(message, false, "Unauthorized user");
            return false;
        }

        switch (service) {
            case "bbox_intersect":
                await this.bboxIntersect(message);
                break;
            default:
                await this.publishMessage(message, false, "Service not found");
                return false;
        }
        return true;
    }

    private async bboxIntersect(message: QueueMessage) {
    }

    private async publishMessage(message: QueueMessage, success: boolean, resText: string) {
        var data = {
            message: resText,
            success: success,
            data_type: message.data.data_type,
            file_upload_path: message.data.file_upload_path
        }
        message.data = data;
        await Core.getTopic(environment.eventBus.backendResponseTopic as string).publish(message);
    }

}

const backendService: IBackendRequest = new BackendService();
export default backendService;


