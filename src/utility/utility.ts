import { environment } from "../environment/environment";
import { Readable } from "stream";
import { FileEntity } from "nodets-ms-core/lib/core/storage";
import { Core } from "nodets-ms-core";
import { QueueMessage } from "nodets-ms-core/lib/core/queue";

export class Utility {

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
