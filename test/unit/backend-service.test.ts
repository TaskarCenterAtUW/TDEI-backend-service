import { BackendService } from '../../src/service/backend-service';
import dbClient from '../../src/database/data-source';
import { Readable } from 'stream';
import { mockCore } from '../common/mock-utils';
import services from '../../src/services.json';
import { Utility } from '../../src/utility/utility';
import { PoolClient } from 'pg';
import { IUploadContext } from '../../src/service/interface/interfaces';
import e from 'express';

describe('BackendService', () => {
  let backendService: BackendService;

  beforeEach(() => {
    backendService = new BackendService(services);
  });

  describe('backendRequestProcessor', () => {
    it('should process the backend request successfully', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          service: 'bbox_intersect',
          parameters: {
            tdei_dataset_id: 'your-tdei-dataset-id',
            bbox: [0, 0, 1, 1]
          }
        },
        messageId: 'your-message-id'
      };
      const validateMessageMock = jest.spyOn(backendService, 'validate').mockReturnValue(true);
      const publishMessageMock = jest.spyOn(Utility, 'publishMessage').mockResolvedValue(undefined);
      const bboxIntersectMock = jest.spyOn(backendService.bboxService, 'bboxIntersect').mockResolvedValue(undefined);

      // Call the method under test
      const result = await backendService.backendRequestProcessor(message);

      // Assertions
      expect(validateMessageMock).toHaveBeenCalledWith(message);
      expect(publishMessageMock).not.toHaveBeenCalled();
      expect(bboxIntersectMock).toHaveBeenCalledWith(message);
      expect(result).toBe(true);
    });

    it('should handle invalid message and publish error message', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          service: 'invalid_service',
          parameters: {}
        },
        messageId: 'your-message-id'
      };
      const validateMessageMock = jest.spyOn(backendService, 'validate').mockReturnValue(false);
      const publishMessageMock = jest.spyOn(Utility, 'publishMessage').mockResolvedValue(undefined);
      const bboxIntersectMock = jest.spyOn(backendService.bboxService, 'bboxIntersect').mockResolvedValue(undefined);

      // Call the method under test
      const result = await backendService.backendRequestProcessor(message);

      // Assertions
      expect(validateMessageMock).toHaveBeenCalledWith(message);
      expect(publishMessageMock).toHaveBeenCalledWith(message, false, undefined);
      expect(bboxIntersectMock).not.toHaveBeenCalled();
      expect(result).toBe(false);
    });

    it('should handle service not found and publish error message', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          service: 'invalid_service',
          parameters: {}
        },
        messageId: 'your-message-id'
      };
      const validateMessageMock = jest.spyOn(backendService, 'validate').mockReturnValue(true);
      const publishMessageMock = jest.spyOn(Utility, 'publishMessage').mockResolvedValue(undefined);
      const bboxIntersectMock = jest.spyOn(backendService.bboxService, 'bboxIntersect').mockResolvedValue(undefined);

      // Call the method under test
      const result = await backendService.backendRequestProcessor(message);

      // Assertions
      expect(validateMessageMock).toHaveBeenCalledWith(message);
      expect(publishMessageMock).toHaveBeenCalledWith(message, false, 'Service not found');
      expect(bboxIntersectMock).not.toHaveBeenCalled();
      expect(result).toBe(false);
    });

    // Add more test cases for different scenarios
  });

  describe('uploadStreamToAzureBlob', () => {
    it('should upload the stream to Azure Blob storage successfully', async () => {
      // Mock the necessary dependencies
      const stream = new Readable();
      const blobDetails = {
        containerName: 'your-container-name',
        filePath: 'your-file-path',
        remoteUrls: []
      };

      mockCore();
      // Call the method under test
      await backendService.bboxService.uploadStreamToAzureBlob(stream, blobDetails, 'your-file-name');

      // Assertions
      expect(blobDetails.remoteUrls).toEqual(['your-remote-url']);
    });
  });

  describe('bboxIntersect', () => {
    it('should execute the query and handle the data and end events', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id: 'your-tdei-dataset-id',
            bbox: [0, 0, 1, 1]
          }
        },
        messageId: 'your-message-id'
      };
      let dbStream = new Readable({ read() { } });
      dbStream.push(JSON.stringify({ "edges": "your-edges-data" }));
      dbStream.push(null);
      const queryStreamMock = jest.fn().mockReturnValueOnce(dbStream);
      const queryMock = jest.fn().mockResolvedValueOnce({ rows: [{ edges: 'your-edges-data' }] });
      const getDbClientMock = jest.spyOn(dbClient, 'getDbClient').mockResolvedValueOnce({} as PoolClient);
      const queryStreamSpy = jest.spyOn(dbClient, 'queryStream').mockImplementation(queryStreamMock);
      const releaseDbClientSpy = jest.spyOn(dbClient, 'releaseDbClient').mockImplementation(undefined);
      const querySpy = jest.spyOn(dbClient, 'query').mockImplementation(queryMock);
      const handleStreamDataEventMock = jest.spyOn(backendService.bboxService, 'handleStreamDataEvent').mockResolvedValueOnce(undefined);
      const handleStreamEndEventMock = jest.spyOn(backendService.bboxService, 'handleStreamEndEvent').mockResolvedValueOnce(undefined);
      const publishMessageMock = jest.spyOn(Utility, 'publishMessage').mockResolvedValueOnce(undefined);

      // Call the method under test
      await backendService.bboxService.bboxIntersect(message);
      await Utility.sleep(1);

      // Assertions
      expect(getDbClientMock).toHaveBeenCalled();
      expect(queryStreamSpy).toHaveBeenCalledWith(expect.any(Object), expect.any(Object));
      expect(querySpy).toHaveBeenCalledWith(expect.any(Object));
      expect(releaseDbClientSpy).toHaveBeenCalled();
      expect(handleStreamDataEventMock).toHaveBeenCalled();
      expect(handleStreamEndEventMock).toHaveBeenCalled();
      expect(publishMessageMock).not.toHaveBeenCalled();
    }, 100);

    it('should handle error during query execution and publish error message', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id: 'your-tdei-dataset-id',
            bbox: [0, 0, 1, 1]
          }
        },
        messageId: 'your-message-id'
      };
      const getDbClientMock = jest.spyOn(dbClient, 'getDbClient').mockResolvedValueOnce({} as PoolClient);
      const queryMock = jest.fn().mockResolvedValueOnce({ rows: [{ edges: 'your-edges-data' }] });
      const querySpy = jest.spyOn(dbClient, 'query').mockImplementation(queryMock);
      const queryStreamMock = jest.fn().mockRejectedValueOnce(new Error('Query execution error'));
      const queryStreamSpy = jest.spyOn(dbClient, 'queryStream').mockImplementation(queryStreamMock);
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);

      // Call the method under test
      await expect(backendService.bboxService.bboxIntersect(message)).rejects.toContain('Error executing query');

      // Assertions
      expect(getDbClientMock).toHaveBeenCalled();
      expect(querySpy).toHaveBeenCalledWith(expect.any(Object));
      expect(queryStreamSpy).toHaveBeenCalledWith(expect.any(Object), expect.any(Object));
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'Error executing query');
    });
  });

  describe('dataset tag road', () => {
    it('should execute the query and handle the data and end events', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id: 'your-tdei-dataset-id',
            bbox: [0, 0, 1, 1]
          }
        },
        messageId: 'your-message-id'
      };
      let dbStream = new Readable({ read() { } });
      dbStream.push(JSON.stringify({ "edges": "your-edges-data" }));
      dbStream.push(null);
      const query2Spy = jest.spyOn(dbClient, 'query').mockResolvedValueOnce({} as any);
      const queryStreamMock = jest.fn().mockReturnValueOnce(dbStream);
      const queryMock = jest.fn().mockResolvedValueOnce({ rows: [{ edges: 'your-edges-data' }] });
      const getDbClientMock = jest.spyOn(dbClient, 'getDbClient').mockResolvedValueOnce({} as PoolClient);
      const queryStreamSpy = jest.spyOn(dbClient, 'queryStream').mockImplementation(queryStreamMock);
      const releaseDbClientSpy = jest.spyOn(dbClient, 'releaseDbClient').mockImplementation(undefined);
      const querySpy = jest.spyOn(dbClient, 'query').mockImplementation(queryMock);
      const handleStreamDataEventMock = jest.spyOn(backendService.datasetTagRoadService, 'handleStreamDataEvent').mockResolvedValueOnce(undefined);
      const handleStreamEndEventMock = jest.spyOn(backendService.datasetTagRoadService, 'handleStreamEndEvent').mockResolvedValueOnce(undefined);
      const publishMessageMock = jest.spyOn(Utility, 'publishMessage').mockResolvedValueOnce(undefined);

      // Call the method under test
      await backendService.datasetTagRoadService.datasetTagRoad(message);
      await Utility.sleep(1);

      // Assertions
      expect(query2Spy).toHaveBeenCalled();
      expect(getDbClientMock).toHaveBeenCalled();
      expect(queryStreamSpy).toHaveBeenCalledWith(expect.any(Object), expect.any(Object));
      expect(querySpy).toHaveBeenCalledWith(expect.any(Object));
      expect(releaseDbClientSpy).toHaveBeenCalled();
      expect(handleStreamDataEventMock).toHaveBeenCalled();
      expect(handleStreamEndEventMock).toHaveBeenCalled();
      expect(publishMessageMock).not.toHaveBeenCalled();
    }, 100);

    it('should handle error during query execution and publish error message', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id: 'your-tdei-dataset-id',
            bbox: [0, 0, 1, 1]
          }
        },
        messageId: 'your-message-id'
      };
      const getDbClientMock = jest.spyOn(dbClient, 'getDbClient').mockResolvedValueOnce({} as PoolClient);
      const queryMock = jest.fn().mockResolvedValueOnce({ rows: [{ edges: 'your-edges-data' }] });
      const querySpy = jest.spyOn(dbClient, 'query').mockImplementation(queryMock);
      const queryStreamMock = jest.fn().mockRejectedValueOnce(new Error('Query execution error'));
      const queryStreamSpy = jest.spyOn(dbClient, 'queryStream').mockImplementation(queryStreamMock);
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);

      // Call the method under test
      await expect(backendService.datasetTagRoadService.datasetTagRoad(message)).rejects.toContain('Error executing query');

      // Assertions
      expect(getDbClientMock).toHaveBeenCalled();
      expect(querySpy).toHaveBeenCalledWith(expect.any(Object));
      expect(queryStreamSpy).toHaveBeenCalledWith(expect.any(Object), expect.any(Object));
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'Error executing query');
    });
  });

  describe('handleStreamEndEvent', () => {
    it('should handle the end event and publish success message', async () => {
      // Mock the necessary dependencies
      const dataObject = {
        edges: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        },
        nodes: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        },
        extensions_points: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        },
        extensions_polygons: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        },
        extensions_lines: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        },
        zones: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        }
      };
      const uploadContext = {
        containerName: 'your-container-name',
        filePath: 'your-file-path',
        remoteUrls: ['your-remote-url'],
        zipUrl: ''
      };
      const message = {
        data: {
          file_upload_path: ''
        }
      };
      const utilitySleep = jest.spyOn(Utility, 'sleep').mockImplementation(() => Promise.resolve());
      const utilitySleep2 = jest.spyOn(Utility, 'sleep').mockImplementation(() => Promise.resolve());
      const publishMessageMock = jest.spyOn(Utility, 'publishMessage').mockResolvedValueOnce(undefined);
      const zipStreamMock = jest.spyOn(backendService.bboxService, 'zipStream').mockResolvedValueOnce(undefined);

      // Call the method under test
      await backendService.bboxService.handleStreamEndEvent(dataObject, uploadContext, message);

      // Assertions
      expect(utilitySleep).toHaveBeenCalled();
      expect(utilitySleep2).toHaveBeenCalled();
      expect(zipStreamMock).toHaveBeenCalled();
      expect(publishMessageMock).toHaveBeenCalledWith(message, true, 'Dataset uploaded successfully!');
    }, 10000);

    it('should publish unsuccessful message when no data is uploaded to storage', async () => {
      // Mock the necessary dependencies
      const dataObject = {
        edges: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        },
        nodes: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        },
        extensions_points: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        },
        extensions_polygons: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        },
        extensions_lines: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        },
        zones: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        }
      };
      const uploadContext = {
        containerName: 'your-container-name',
        filePath: 'your-file-path',
        remoteUrls: [],
        zipUrl: ''
      };
      const message = {
        data: {
          file_upload_path: ''
        }
      };
      const utilitySleep = jest.spyOn(Utility, 'sleep').mockImplementation(() => Promise.resolve());
      const publishMessageMock = jest.spyOn(Utility, 'publishMessage').mockResolvedValueOnce(undefined);
      mockCore();

      // Call the method under test
      await backendService.bboxService.handleStreamEndEvent(dataObject, uploadContext, message);

      // Assertions
      expect(publishMessageMock).toHaveBeenCalledWith(message, true, 'No data found for given prarameters.');
    }, 15000);
  });

  describe('handleStreamDataEvent', () => {
    it('should handle the data event and upload the stream to Azure Blob storage', async () => {
      // Mock the necessary dependencies
      const data = {
        edges: 'your-edges-data'
      };
      const dataObject = {
        edges: {
          stream: new Readable({ read() { } }),
          firstFlag: true
        }
      };
      const uploadStreamMock = jest.fn().mockResolvedValueOnce(undefined);
      const uploadStreamSpy = jest.spyOn(backendService.bboxService, 'uploadStreamToAzureBlob').mockImplementation(uploadStreamMock);

      // Call the method under test
      await backendService.bboxService.handleStreamDataEvent(data, dataObject, {} as IUploadContext);

      // Assertions
      expect(uploadStreamSpy).toHaveBeenCalledWith(expect.anything(), expect.any(Object), 'osw.edges.geojson');
    });
  });

  describe('zipStream', () => {
    it('should zip the files and upload the zip archive to Azure Blob storage', async () => {
      // Mock the necessary dependencies
      mockCore();
      const uploadStreamMock = jest.fn().mockResolvedValueOnce(undefined);
      const uploadStreamSpy = jest.spyOn(backendService.bboxService, 'uploadStreamToAzureBlob').mockImplementation(uploadStreamMock);

      // Call the method under test
      await backendService.bboxService.zipStream(<any>{
        remoteUrls: ['your-remote-url-1', 'your-remote-url-2']
      });

      // Assertions
      expect(uploadStreamSpy).toHaveBeenCalledWith(expect.any(Object), expect.any(Object), expect.any(String));
    }, 10000);
  });

  describe('publishMessage', () => {
    it('should publish a message to the backend response topic', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          file_upload_path: ''
        }
      };
      const publishMessageMock = jest.spyOn(Utility, 'publishMessage').mockResolvedValueOnce(undefined);
      mockCore();

      // Call the method under test
      await Utility.publishMessage(message, true, 'Dataset uploaded successfully!');

      // Assertions
      expect(publishMessageMock).toHaveBeenCalledWith(message, true, 'Dataset uploaded successfully!');
    });

    // Add more test cases for different scenarios
  });

  describe('Union Dataset', () => {
    it('should execute the query and handle the data and end events', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id_one: 'your-tdei-dataset-id',
            tdei_dataset_id_two: 'your-tdei-dataset-id'
          }
        },
        messageId: 'your-message-id'
      };
      let dbStream = new Readable({ read() { } });
      dbStream.push(JSON.stringify({ "edges": "your-edges-data" }));
      dbStream.push(null);
      const queryStreamMock = jest.fn().mockReturnValueOnce(dbStream);
      // const queryMock1 = jest.fn().mockResolvedValueOnce({ rows: [{ name: 'dataset-name' }] });
      const queryMock = jest.fn().mockResolvedValueOnce({ rows: [{ edges: 'your-edges-data' }] });
      const querySpy1 = jest.spyOn(dbClient, 'query').mockResolvedValueOnce(<any>{ rows: [{ name: 'dataset-name' }, { name: 'dataset-name2' }] });
      const getDbClientMock = jest.spyOn(dbClient, 'getDbClient').mockResolvedValueOnce({} as PoolClient);
      const queryStreamSpy = jest.spyOn(dbClient, 'queryStream').mockImplementation(queryStreamMock);
      const releaseDbClientSpy = jest.spyOn(dbClient, 'releaseDbClient').mockImplementation(undefined);
      const querySpy = jest.spyOn(dbClient, 'query').mockImplementation(queryMock);
      const handleStreamDataEventMock = jest.spyOn(backendService.unionQueryService, 'handleStreamDataEvent').mockResolvedValueOnce(undefined);
      const handleStreamEndEventMock = jest.spyOn(backendService.unionQueryService, 'handleStreamEndEvent').mockResolvedValueOnce(undefined);
      const publishMessageMock = jest.spyOn(Utility, 'publishMessage').mockResolvedValueOnce(undefined);

      // Call the method under test
      await backendService.unionQueryService.executeUnionQuery(message);
      await Utility.sleep(1);

      // Assertions
      expect(querySpy1).toHaveBeenCalled();
      expect(getDbClientMock).toHaveBeenCalled();
      expect(queryStreamSpy).toHaveBeenCalledWith(expect.any(Object), expect.any(Object));
      expect(querySpy).toHaveBeenCalledWith(expect.any(Object));
      expect(releaseDbClientSpy).toHaveBeenCalled();
      expect(handleStreamDataEventMock).toHaveBeenCalled();
      expect(handleStreamEndEventMock).toHaveBeenCalled();
      expect(publishMessageMock).not.toHaveBeenCalled();
    }, 100);

    it('should handle error during query execution and publish error message', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id: 'your-tdei-dataset-id',
            bbox: [0, 0, 1, 1]
          }
        },
        messageId: 'your-message-id'
      };
      const querySpy1 = jest.spyOn(dbClient, 'query').mockResolvedValueOnce(<any>{ rows: [{ name: 'dataset-name' }, { name: 'dataset-name2' }] });
      const getDbClientMock = jest.spyOn(dbClient, 'getDbClient').mockResolvedValueOnce({} as PoolClient);
      const queryMock = jest.fn().mockResolvedValueOnce({ rows: [{ edges: 'your-edges-data' }] });
      const querySpy = jest.spyOn(dbClient, 'query').mockImplementation(queryMock);
      const queryStreamMock = jest.fn().mockRejectedValueOnce(new Error('Query execution error'));
      const queryStreamSpy = jest.spyOn(dbClient, 'queryStream').mockImplementation(queryStreamMock);
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);

      // Call the method under test
      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toContain('Error executing query');

      // Assertions
      expect(querySpy1).toHaveBeenCalled();
      expect(getDbClientMock).toHaveBeenCalled();
      expect(querySpy).toHaveBeenCalledWith(expect.any(Object));
      expect(queryStreamSpy).toHaveBeenCalledWith(expect.any(Object), expect.any(Object));
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'Error executing query');
    });
  });
});