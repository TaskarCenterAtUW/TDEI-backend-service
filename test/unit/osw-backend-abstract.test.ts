import { AbstractOSWBackendRequest } from '../../src/service/base/osw-backend-abstract';
import { IUploadContext } from '../../src/service/interface/interfaces';
import { ClientBase, PoolClient, QueryConfig, QueryResult } from 'pg';
import dbClient from '../../src/database/data-source';
import { Utility } from '../../src/utility/utility';
import { Readable } from 'stream';
import services from '../../src/services.json';

import { Pool } from 'pg';

// Mock the Pool class
jest.mock('pg', () => {
    const mClient = {
        query: jest.fn(),
        release: jest.fn(),
    };
    const mPool: any = {
        connect: jest.fn().mockResolvedValue(mClient),
        query: jest.fn(),
        end: jest.fn(),
    };
    return { Pool: jest.fn(() => mPool) };
});

class DummyService extends AbstractOSWBackendRequest { }

describe('AbstractOSWBackendRequest', () => {
    let service: DummyService;
    let mockUploadContext: IUploadContext;
    let mockQueryConfig: QueryConfig;
    let mockMessage: any;
    let pool: any;
    let client: any;

    beforeEach(() => {
        service = new DummyService(services);
        mockUploadContext = {
            remoteUrls: [],
            zipUrl: '',
            outputFileName: '',
            containerName: '',
            filePath: ''
        };
        mockQueryConfig = {
            text: '',
            values: ['']
        };
        mockMessage = {};
        pool = new Pool();
        client = pool.connect();
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    it('should process and upload dataset successfully', async () => {
        const pool = new Pool();
        const mockClient = await pool.connect();
        // Set up the mock return value for the query method

        const getdbClientSpy = jest.spyOn(dbClient, 'getDbClient').mockResolvedValueOnce(mockClient as PoolClient);
        const queryBeginTransactionSpy = jest.spyOn(mockClient, 'query').mockReturnValueOnce({} as any);
        const datasetQuerySpy = jest.spyOn(mockClient, 'query').mockReturnValueOnce({
            rows: [
                { edge: 'edgeData', node: 'nodeData', zone: 'zoneData', ext_point_info: 'pointData', ext_line_info: 'lineData', ext_polygon_info: 'polygonData' }
            ]
        } as any).mockReturnValueOnce({
            rows: [
                { file_meta: 'fileMetaData', name: 'fileName' }
            ]
        } as any).mockReturnValueOnce({
            rows: [
                { file_name: 'fileName', cursor_ref: 'cursorRef' }
            ]
        } as any);
        const queryCommitTransactionSpy = jest.spyOn(mockClient, 'query').mockReturnValueOnce({} as any);

        const mockStream = new Readable({ read() { } });
        dbClient.queryStream = jest.fn().mockResolvedValue(mockStream);

        mockStream.on = jest.fn((event, callback) => {
            if (event === 'data') {
                callback({ feature: 'featureData' });
            } else if (event === 'end') {
                callback();
            }
            return mockStream;
        });

        // const releasedbClientSpy = jest.spyOn(dbClient, 'releaseDbClient').mockResolvedValueOnce();
        const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
        const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);
        const serviceHandleStreamSpy = jest.spyOn(service, 'handleStreamDataEvent').mockReturnThis();

        service.zipAndUpload = jest.fn().mockResolvedValue(undefined);

        await service.process_upload_dataset('tdei_dataset_id', mockUploadContext, mockMessage, mockQueryConfig);

        expect(queryBeginTransactionSpy).toHaveBeenCalledWith('BEGIN');
        expect(datasetQuerySpy).toHaveBeenCalledWith(expect.objectContaining({
            text: expect.stringContaining('SELECT event_info as edge, node_info as node, zone_info as zone'),
            values: ['tdei_dataset_id']
        }));
        expect(datasetQuerySpy).toHaveBeenCalledWith(expect.objectContaining({
            text: expect.stringContaining('SELECT file_meta, name FROM content.extension_file'),
            values: ['tdei_dataset_id']
        }));
        expect(datasetQuerySpy).toHaveBeenCalledWith(mockQueryConfig);
        expect(queryCommitTransactionSpy).toHaveBeenCalledWith('COMMIT');
        expect(serviceHandleStreamSpy).toHaveBeenCalled();
        // expect(dbClient.releaseDbClient).toHaveBeenCalledWith(releasedbClientSpy);
        expect(service.zipAndUpload).toHaveBeenCalledWith(mockUploadContext, mockMessage);
    }, 10000);
});