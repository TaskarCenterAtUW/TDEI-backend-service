import { QueueMessage } from 'nodets-ms-core/lib/core/queue';
import { BackendService } from '../../src/service/backend-service';
import dbClient from '../../src/database/data-source';
import { Readable } from 'stream';

describe('BackendService', () => {
  let backendService: BackendService;

  beforeEach(() => {
    backendService = new BackendService();
  });

  describe('backendRequestProcessor', () => {
    // Write tests for the backendRequestProcessor method
  });

  describe('uploadStreamToAzureBlob', () => {
    // Write tests for the uploadStreamToAzureBlob method
  });

  describe('bboxIntersect', () => {

    it('should handle errors during streaming and uploading', async () => {
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
      const queryStreamMock = jest.fn().mockRejectedValueOnce(new Error('Query execution error'));
      const queryStreamSpy = jest.spyOn(dbClient, 'queryStream').mockImplementation(queryStreamMock);
      const publishMessageMock = jest.fn().mockResolvedValueOnce(undefined);
      const publishMessageSpy = jest.spyOn(backendService, 'publishMessage').mockImplementation(publishMessageMock);

      // Call the method under test
      await backendService.bboxIntersect(message);

      // Assertions
      // expect(queryStreamSpy).toHaveBeenCalledWith(expect.anything(), expect.anything());
      expect(publishMessageSpy).toHaveBeenCalledWith(message, false, 'Error executing query');
    });
  });


  describe('zipStream', () => {
    // Write tests for the zipStream method
  });

  describe('publishMessage', () => {
    // Write tests for the publishMessage method
  });
});
