import { environment } from "../environment/environment";
import { Readable } from "stream";
import { FileEntity } from "nodets-ms-core/lib/core/storage";
import { Core } from "nodets-ms-core";
import { QueueMessage } from "nodets-ms-core/lib/core/queue";

export class Utility {
    static async waitForBlobAvailability(tdei_dataset_id: string, remoteUrl: string) {

        // Verify the file is uploaded and check for 2 retries every 5 seconds
        let retries = 0;
        const maxRetries = 2;
        const retryDelay = 5000; // 5 seconds
        //Initial wait before checking for file availability
        await Utility.sleep(retryDelay);
        //Check for file availability with retries
        while (retries < maxRetries) {
            try {
                const client = Core.getStorageClient();
                const containerWithFile = await client?.getFileFromUrl(remoteUrl);
                console.log(`File for dataset ${tdei_dataset_id} is available in Azure Blob Storage. File name: ${containerWithFile?.fileName}`);
                return; // File is found, exit the function
            } catch (error) {
                retries++;
                console.warn(`File not available. Retry ${retries}/${maxRetries} in ${retryDelay / 1000} seconds...`);
                if (retries >= maxRetries) {
                    console.error(`File for dataset ${tdei_dataset_id} not found after ${maxRetries} retries.`);
                }
                await Utility.sleep(retryDelay);
            }
        }
    }

    public static sleep(ms: number): Promise<void> {
        return new Promise(resolve => {
            setTimeout(resolve, ms);
        });
    }

    public static async stream2buffer(stream: NodeJS.ReadableStream): Promise<Buffer> {

        return new Promise<Buffer>((resolve, reject) => {

            const _buf = Array<any>();

            stream.on("data", chunk => _buf.push(chunk));
            stream.on("end", () => resolve(Buffer.concat(_buf)));
            stream.on("error", err => reject(`error converting stream - ${err}`));

        });
    }

    /**
  * Publishes a message to the backend response topic.
  * 
  * @param message - The original queue message.
  * @param success - Indicates whether the operation was successful.
  * @param resText - The response text.
  */
    public static async publishMessage(message: QueueMessage, success: boolean, resText: string) {
        var data = {
            message: resText,
            success: success,
            file_upload_path: message.data.file_upload_path ?? ""
        }
        message.data = data;
        await Core.getTopic(environment.eventBus.backendResponseTopic as string).publish(message);
    }
}

/**
 * Stream reader for FileEntity. Needed for zip download of
 * the files.
 */
export class FileEntityStream extends Readable {
    constructor(private fileEntity: FileEntity) {
        super();
    }

    async _read(size: number): Promise<void> {
        const fileStream = await this.fileEntity.getStream();

        fileStream.on('data', (chunk) => {
            if (!this.push(chunk)) {
                // If the internal buffer is full, pause until the consumer is ready
                fileStream.pause();
            }
        });

        fileStream.on('end', () => {
            this.push(null); // No more data to push
        });

        fileStream.on('error', (err) => {
            this.emit('error', err);
        });
    }
}
