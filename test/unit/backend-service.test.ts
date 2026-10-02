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
    it('should execute the query and handle the data and end events', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id_one: 'your-tdei-dataset-id',
            tdei_dataset_id_two: 'your-tdei-dataset-id',
            proximity: 0.5
          }
        },
        messageId: 'your-message-id'
      };

      const handleStreamDataEventMock = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset').mockResolvedValueOnce(true);

      // Call the method under test
      await backendService.unionQueryService.executeUnionQuery(message);
      await Utility.sleep(1);

      // Assertions
      expect(handleStreamDataEventMock).toHaveBeenCalled();
    }, 1000);

    it('should execute the query when proximity is undefined', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id_one: 'your-tdei-dataset-id',
            tdei_dataset_id_two: 'your-tdei-dataset-id',
            proximity: undefined
          }
        },
        messageId: 'your-message-id'
      };

      const handleStreamDataEventMock = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset').mockResolvedValueOnce(true);

      // Call the method under test
      await backendService.unionQueryService.executeUnionQuery(message);
      await Utility.sleep(1);

      // Assertions
      expect(handleStreamDataEventMock).toHaveBeenCalled();
    }, 1000);

    it('should handle error during query execution and publish error message', async () => {
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
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);

      const handleStreamDataEventMock = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset').mockImplementationOnce(() => {
        throw new Error('Error executing query');
      });
      // Call the method under test
      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toContain('Error executing query');

      // Assertions
      expect(handleStreamDataEventMock).toHaveBeenCalled();
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'Error executing query');
    });

    it('should handle error when proximity parameter is of type string', async () => {
      // Mock the necessary dependencies
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id_one: 'your-tdei-dataset-id',
            tdei_dataset_id_two: 'your-tdei-dataset-id',
            proximity: 'your-proximity'
          }
        },
        messageId: 'your-message-id'
      };
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);

      // Call the method under test
      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toContain('Invalid proximity parameter');

      // Assertions
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'Invalid proximity parameter');
    });

    it('should pass validated entity_filters to tdei_union_dataset', async () => {
      const entity_filters = {
        edge: {
          filters: [{ highway: 'footway', footway: 'sidewalk' }],
        },
      };
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id_one: 'your-tdei-dataset-id',
            tdei_dataset_id_two: 'your-tdei-dataset-id',
            proximity: 1.0,
            entity_filters,
          },
        },
        messageId: 'your-message-id',
      };

      const handleStreamDataEventMock = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset').mockResolvedValueOnce(true);

      await backendService.unionQueryService.executeUnionQuery(message);

      expect(handleStreamDataEventMock).toHaveBeenCalledWith(
        'your-tdei-dataset-id',
        expect.any(Object),
        message,
        expect.objectContaining({
          text: 'SELECT * FROM content.tdei_union_dataset($1,$2,$3,$4)',
          values: ['your-tdei-dataset-id', 'your-tdei-dataset-id', 1.0, JSON.stringify(entity_filters)],
        })
      );
    });

    it('should pass the full multi-dimension entity_filters sample to tdei_union_dataset', async () => {
      const entity_filters = {
        edge: {
          filters: [
            { highway: 'footway', footway: 'sidewalk' },
            { highway: 'footway', footway: 'crossing' },
          ],
          duplicate_buffer_width: 2,
          duplicate_overlap_percentage: 75,
        },
        node: {
          filters: [
            { barrier: 'kerb' },
          ],
        },
        line: {
          filters: [
            { barrier: 'fence' },
          ],
          duplicate_buffer_width: 1.5,
          duplicate_overlap_percentage: 65,
        },
        polygon: {
          filters: [
            { building: 'yes' },
          ],
          duplicate_overlap_percentage: 80,
        },
        zone: {
          duplicate_overlap_percentage: 75,
        },
        point: {
          filters: [
            { amenity: 'bench' },
            { highway: 'street_lamp' },
          ],
        },
      };
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id_one: 'your-tdei-dataset-id',
            tdei_dataset_id_two: 'your-tdei-dataset-id',
            proximity: 1.0,
            entity_filters,
          },
        },
        messageId: 'your-message-id',
      };

      const handleStreamDataEventMock = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset').mockResolvedValueOnce(true);

      await backendService.unionQueryService.executeUnionQuery(message);

      expect(handleStreamDataEventMock).toHaveBeenCalledWith(
        'your-tdei-dataset-id',
        expect.any(Object),
        message,
        expect.objectContaining({
          text: 'SELECT * FROM content.tdei_union_dataset($1,$2,$3,$4)',
          values: ['your-tdei-dataset-id', 'your-tdei-dataset-id', 1.0, JSON.stringify(entity_filters)],
        })
      );
    });

    it('should reject invalid entity_filters attributes', async () => {
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id_one: 'your-tdei-dataset-id',
            tdei_dataset_id_two: 'your-tdei-dataset-id',
            entity_filters: {
              edge: {
                filters: [{ not_an_osw_attribute: 'x' }],
              },
            },
          },
        },
        messageId: 'your-message-id',
      };
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);
      const handleStreamDataEventMock = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset');

      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toContain(
        "not a valid OpenSidewalks attribute for 'edge'"
      );

      expect(handleStreamDataEventMock).not.toHaveBeenCalled();
      expect(publishMessageSpy).toHaveBeenCalledWith(
        message,
        false,
        expect.stringContaining("not a valid OpenSidewalks attribute for 'edge'")
      );
    });

    it('should reject duplicate_buffer_width on unsupported entity_filters dimensions', async () => {
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id_one: 'your-tdei-dataset-id',
            tdei_dataset_id_two: 'your-tdei-dataset-id',
            entity_filters: {
              polygon: {
                filters: [{ building: 'yes' }],
                duplicate_buffer_width: 2,
                duplicate_overlap_percentage: 80,
              },
            },
          },
        },
        messageId: 'your-message-id',
      };
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);
      const handleStreamDataEventMock = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset');

      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toContain(
        "'duplicate_buffer_width' is not supported for 'polygon'"
      );

      expect(handleStreamDataEventMock).not.toHaveBeenCalled();
      expect(publishMessageSpy).toHaveBeenCalledWith(
        message,
        false,
        expect.stringContaining("'duplicate_buffer_width' is not supported for 'polygon'")
      );
    });

    it('should reject duplicate_overlap_percentage on unsupported entity_filters dimensions', async () => {
      const message: any = {
        data: {
          parameters: {
            tdei_dataset_id_one: 'your-tdei-dataset-id',
            tdei_dataset_id_two: 'your-tdei-dataset-id',
            entity_filters: {
              point: {
                filters: [{ amenity: 'bench' }],
                duplicate_overlap_percentage: 80,
              },
            },
          },
        },
        messageId: 'your-message-id',
      };
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(Utility, 'publishMessage').mockImplementation(publishMessageMock);
      const handleStreamDataEventMock = jest.spyOn(backendService.unionQueryService, 'process_upload_dataset');

      await expect(backendService.unionQueryService.executeUnionQuery(message)).rejects.toContain(
        "'duplicate_overlap_percentage' is not supported for 'point'"
      );

      expect(handleStreamDataEventMock).not.toHaveBeenCalled();
      expect(publishMessageSpy).toHaveBeenCalledWith(
        message,
        false,
        expect.stringContaining("'duplicate_overlap_percentage' is not supported for 'point'")
      );
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