import { BackendService } from '../../src/service/backend-service';
import dbClient from '../../src/database/data-source';
import { Readable } from 'stream';
import { mockCore } from '../common/mock-utils';
import services from '../../src/services.json';
import { Utility } from '../../src/utility/utility';
import { PoolClient } from 'pg';
import { IUploadContext } from '../../src/service/interface/interfaces';

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
      const handleStreamDataEventMock = jest.spyOn(backendService.bboxService, 'process_upload_dataset').mockResolvedValueOnce(true);

      // Call the method under test
      await backendService.bboxService.bboxIntersect(message);
      await Utility.sleep(1);

      // Assertions
      expect(handleStreamDataEventMock).toHaveBeenCalled();
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
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);

      const handleStreamDataEventMock = jest.spyOn(backendService.bboxService, 'process_upload_dataset').mockImplementationOnce(() => {
        throw new Error('Error executing query');
      });
      // Call the method under test
      await expect(backendService.bboxService.bboxIntersect(message)).rejects.toContain('Error executing query');

      // Assertions
      expect(handleStreamDataEventMock).toHaveBeenCalled();
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'Error executing query');
    });
  });

  describe('dataset tag road', () => {
    it('should execute the query and handle the data and end events', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          parameters: {
            target_dataset_id: 'your-tdei-dataset-id',
            source_dataset_id: 'your-tdei-dataset-id'
          }
        },
        messageId: 'your-message-id'
      };

      const query2Spy = jest.spyOn(dbClient, 'query').mockResolvedValueOnce({} as any);
      const handleStreamDataEventMock = jest.spyOn(backendService.datasetTagRoadService, 'process_upload_dataset').mockResolvedValueOnce(true);

      // Call the method under test
      await backendService.datasetTagRoadService.datasetTagRoad(message);
      await Utility.sleep(1);

      // Assertions
      expect(query2Spy).toHaveBeenCalled();
      expect(handleStreamDataEventMock).toHaveBeenCalled();
    }, 10000);

    it('should handle error during query execution and publish error message', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          parameters: {
            target_dataset_id: 'your-tdei-dataset-id',
            source_dataset_id: 'your-tdei-dataset-id'
          }
        },
        messageId: 'your-message-id'
      };
      const query2Spy = jest.spyOn(dbClient, 'query').mockResolvedValueOnce({} as any);
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);

      const handleStreamDataEventMock = jest.spyOn(backendService.datasetTagRoadService, 'process_upload_dataset').mockImplementationOnce(() => {
        throw new Error('Error executing query');
      });
      // Call the method under test
      await expect(backendService.datasetTagRoadService.datasetTagRoad(message)).rejects.toContain('Error executing query');

      // Assertions
      expect(query2Spy).toHaveBeenCalled();
      expect(handleStreamDataEventMock).toHaveBeenCalled();
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'Error executing query');
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
    const unionMessage = (parameters: any): any => ({
      data: { parameters: { tdei_dataset_id_one: 'dataset-one', tdei_dataset_id_two: 'dataset-two', ...parameters } },
      messageId: 'your-message-id'
    });
    const unionSql = 'SELECT * FROM content.tdei_union_dataset($1,$2,$3::jsonb)';

    it('should run the union with an empty config when none is given', async () => {
      const message = unionMessage({});
      const processSpy = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset').mockResolvedValueOnce(true);

      await backendService.unionQueryService.executeUnionQuery(message);

      expect(processSpy).toHaveBeenCalledWith(
        'dataset-one',
        expect.any(Object),
        message,
        expect.objectContaining({ text: unionSql, values: ['dataset-one', 'dataset-two', '{}'] }),
        { jsonOutputs: { report: 'union_report.json' } }
      );
    });

    it('should pass the config object to tdei_union_dataset', async () => {
      const config = {
        snap: { mode: 'default', proximity: 1 },
        dedup: { mode: 'default', proximity: 1, max_angle: 30 },
        connect: {
          mode: 'custom',
          rules: [{ name: 'kerbed-crossing', from: 'crossing', to: 'sidewalk', operation: 'geometry',
                    proximity: 1, node_tags: { barrier: 'kerb' }, edge_tags: { 'crossing:markings': 'zebra' } }]
        },
        extensions: { mode: 'default', points: { proximity: 1 }, lines: { proximity: 2.5 }, polygons: { enabled: false } }
      };
      const message = unionMessage({ config });
      const processSpy = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset').mockResolvedValueOnce(true);

      await backendService.unionQueryService.executeUnionQuery(message);

      expect(processSpy).toHaveBeenCalledWith(
        'dataset-one', expect.any(Object), message,
        expect.objectContaining({ text: unionSql, values: ['dataset-one', 'dataset-two', JSON.stringify(config)] }),
        { jsonOutputs: { report: 'union_report.json' } }
      );
    });

    it('should accept the config as a JSON string', async () => {
      const config = { connect: { mode: 'default', proximity: 2 } };
      const message = unionMessage({ config: JSON.stringify(config) });
      const processSpy = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset').mockResolvedValueOnce(true);

      await backendService.unionQueryService.executeUnionQuery(message);

      expect(processSpy).toHaveBeenCalledWith(
        'dataset-one', expect.any(Object), message,
        expect.objectContaining({ values: ['dataset-one', 'dataset-two', JSON.stringify(config)] }),
        expect.any(Object)
      );
    });

    it.each([
      ['proximity', { proximity: 0.5 }],
      ['entity_filters', { entity_filters: { edge: { filters: [{ highway: 'footway' }] } } }],
    ])('should reject the removed 4.x parameter %s', async (name, parameters) => {
      const message = unionMessage(parameters);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockResolvedValue(undefined);
      const processSpy = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset');

      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toContain(`'${name}' is no longer supported`);

      expect(processSpy).not.toHaveBeenCalled();
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, expect.stringContaining(`'${name}' is no longer supported`));
    });

    it('should reject a config with an unknown section before running the union', async () => {
      const message = unionMessage({ config: { snapping: { mode: 'default' } } });
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockResolvedValue(undefined);
      const processSpy = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset');

      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toContain("unknown section 'snapping'");

      expect(processSpy).not.toHaveBeenCalled();
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, expect.stringContaining("unknown section 'snapping'"));
    });

    it('should reject the same dataset twice', async () => {
      const message = unionMessage({ tdei_dataset_id_two: 'dataset-one' });
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockResolvedValue(undefined);
      const processSpy = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset');

      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toContain('must be different datasets');

      expect(processSpy).not.toHaveBeenCalled();
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, expect.stringContaining('must be different datasets'));
    });

    it("should publish the union's own message when it rejects the config", async () => {
      const message = unionMessage({ config: { connect: { mode: 'custom', rules: [{ name: 'x', from: 'crossing', to: 'road' }] } } });
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockResolvedValue(undefined);
      const dbError: any = new Error('config.connect.rules[1].operation: is required — one of point, geometry');
      dbError.code = '22023';
      jest.spyOn(backendService.unionQueryService, 'process_upload_dataset').mockRejectedValueOnce(dbError);

      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toContain('config.connect.rules[1].operation');

      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'config.connect.rules[1].operation: is required — one of point, geometry');
    });

    it.each([
      ['a connect rule with tags', { connect: { mode: 'custom', rules: [{ name: 'x', from: 'crossing', to: 'road', operation: 'point', tags: { kerb: 'lowered' } }] } },
        'config.connect.rules[1].tags: a connect rule takes node_tags (for the junction node) and edge_tags (for the connector edge) instead of tags'],
      ['every extension type off', { extensions: { mode: 'default', points: false, lines: false, polygons: false, zones: false } },
        'config.extensions: points, lines, polygons and zones are all off — use "mode": "off" instead'],
      ['a dedup angle above 90', { dedup: { mode: 'default', max_angle: 120 } },
        'config.dedup.max_angle: must be greater than 0 and at most 90 (got 120)'],
    ])("should pass %s to the union and publish its message", async (_name, config, dbMessage) => {
      const message = unionMessage({ config });
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockResolvedValue(undefined);
      const dbError: any = new Error(dbMessage);
      dbError.code = '22023';
      const processSpy = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset').mockRejectedValueOnce(dbError);

      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toBe(dbMessage);

      expect(processSpy).toHaveBeenCalledWith(
        'dataset-one', expect.any(Object), message,
        expect.objectContaining({ values: ['dataset-one', 'dataset-two', JSON.stringify(config)] }),
        expect.any(Object)
      );
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, dbMessage);
    });

    it('should handle error during query execution and publish error message', async () => {
      const message = unionMessage({});
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockResolvedValue(undefined);
      const processSpy = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset').mockImplementationOnce(() => {
        throw new Error('Error executing query');
      });

      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toContain('Error executing query');

      expect(processSpy).toHaveBeenCalled();
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'Error executing query');
    });
  });

  describe('Self Merge Dataset', () => {
    it('should execute the query and handle the data and end events', async () => {
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id: 'your-tdei-dataset-id',
            proximity: 0.5
          }
        },
        messageId: 'your-message-id'
      };

      const handleStreamDataEventMock = jest.spyOn(backendService.selfMergeDatasetQueryService, 'process_upload_dataset').mockResolvedValueOnce(true);

      await backendService.selfMergeDatasetQueryService.executeSelfMergeDatasetQuery(message);
      await Utility.sleep(1);

      expect(handleStreamDataEventMock).toHaveBeenCalled();
    }, 1000);

    it('should execute the query when proximity is undefined', async () => {
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id: 'your-tdei-dataset-id',
            proximity: undefined
          }
        },
        messageId: 'your-message-id'
      };

      const handleStreamDataEventMock = jest.spyOn(backendService.selfMergeDatasetQueryService, 'process_upload_dataset').mockResolvedValueOnce(true);

      await backendService.selfMergeDatasetQueryService.executeSelfMergeDatasetQuery(message);
      await Utility.sleep(1);

      expect(handleStreamDataEventMock).toHaveBeenCalled();
    }, 1000);

    it('should handle error during query execution and publish error message', async () => {
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id: 'your-tdei-dataset-id'
          }
        },
        messageId: 'your-message-id'
      };
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);

      const handleStreamDataEventMock = jest.spyOn(backendService.selfMergeDatasetQueryService, 'process_upload_dataset').mockImplementationOnce(() => {
        throw new Error('Error executing query');
      });
      await expect(backendService.selfMergeDatasetQueryService.executeSelfMergeDatasetQuery(message)).rejects.toContain('Error executing query');

      expect(handleStreamDataEventMock).toHaveBeenCalled();
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'Error executing query');
    });

    it('should handle error when proximity parameter is of type string', async () => {
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id: 'your-tdei-dataset-id',
            proximity: 'your-proximity'
          }
        },
        messageId: 'your-message-id'
      };
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);

      await expect(backendService.selfMergeDatasetQueryService.executeSelfMergeDatasetQuery(message)).rejects.toContain('Invalid proximity parameter');

      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'Invalid proximity parameter');
    });
  });
});