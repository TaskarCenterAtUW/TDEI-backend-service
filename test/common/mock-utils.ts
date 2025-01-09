import { Core } from "nodets-ms-core"
import { IAuthorizer } from "nodets-ms-core/lib/core/auth/abstracts/IAuthorizer";
import { Topic } from "nodets-ms-core/lib/core/queue/topic";
import { FileEntity, StorageClient, StorageContainer } from "nodets-ms-core/lib/core/storage"
import { Readable } from "stream";


export function getMockFileEntity() {
    const fileEntity: FileEntity = {
        fileName: "test_file_name",
        mimeType: "csv",
        filePath: "test_file_path",
        remoteUrl: "your-remote-url",
        getStream: function (): Promise<Readable> {
            let mockedStream = new Readable({ read() { } });
            mockedStream.push(JSON.stringify({ "test": "test-data" }));
            mockedStream.push(null);
            return Promise.resolve(mockedStream);
        },
        getBodyText: function (): Promise<string> {
            return Promise.resolve("Sample body test");
        },
        upload: function (): Promise<FileEntity> {
            return Promise.resolve(this);
        },
        uploadStream: function (stream: Readable): Promise<void> {
            return Promise.resolve();
        },
        deleteFile: function (): Promise<void> {
            throw new Error("Function not implemented.");
        }
    };
    return fileEntity;
}

export function getMockStream(): Readable {
    let mockedStream = new Readable({ read() { } });
    mockedStream.push(JSON.stringify({ "test": "test-data" }));
    mockedStream.push(null);
    return mockedStream;
}

export function getMockStorageClient() {
    const storageClientObj: StorageClient = {
        getContainer: function (): Promise<StorageContainer> {
            return Promise.resolve(getMockStorageContainer());
        },
        getFile: function (): Promise<FileEntity> {
            return Promise.resolve(getMockFileEntity());
        },
        getFileFromUrl: function (): Promise<FileEntity> {
            return Promise.resolve(getMockFileEntity());
        },
        getSASUrl: function (containerName: string, filePath: string, expiryInHours: number): Promise<string> {
            throw new Error("Function not implemented.");
        },
        cloneFile: function (fileUrl: string, destinationContainerName: string, destinationFilePath: string): Promise<FileEntity> {
            throw new Error("Function not implemented.");
        }
    };
    return storageClientObj;
}

export function getMockStorageContainer() {
    const storageContainerObj: StorageContainer = {
        name: "test_container",
        listFiles: function (): Promise<FileEntity[]> {
            return Promise.resolve([getMockFileEntity()]);
        },
        createFile: function (): FileEntity {
            return getMockFileEntity();
        }
    };
    return storageContainerObj;
}

export function getMockTopic() {
    const mockTopic: Topic = new Topic({ provider: "Azure" }, "test");
    mockTopic.publish = (): Promise<void> => {
        return Promise.resolve();
    }

    return mockTopic;
}


export function getMockAuthorizer(result: boolean) {
    const authorizor: IAuthorizer = {
        hasPermission(permissionRequest) {
            return Promise.resolve(result);
        },
    }
    return authorizor;
}

export function mockCoreAuth(result: boolean) {
    jest.spyOn(Core, 'getAuthorizer').mockImplementation(() => { return getMockAuthorizer(result); })

}


export function mockCore() {
    jest.spyOn(Core, "initialize");
    jest.spyOn(Core, "getStorageClient").mockImplementation(() => { return getMockStorageClient(); });
    jest.spyOn(Core, "getTopic").mockImplementation(() => { return getMockTopic(); });


}
