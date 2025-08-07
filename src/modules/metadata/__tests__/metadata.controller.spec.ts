import { Test, TestingModule } from '@nestjs/testing';
import { MetadataController } from '../metadata.controller';
import { MetadataService } from '../metadata.service';
import { SyncResponseDto } from '../dto/sync-response.dto';

describe('MetadataController', () => {
  let controller: MetadataController;
  let service: MetadataService;

  beforeEach(async () => {
    const mockService = {
      syncMetadata: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      controllers: [MetadataController],
      providers: [
        {
          provide: MetadataService,
          useValue: mockService,
        },
      ],
    }).compile();

    controller = module.get<MetadataController>(MetadataController);
    service = module.get<MetadataService>(MetadataService);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });

  it('should return success response when sync completes successfully', async () => {
    const mockResponse: SyncResponseDto = {
      success: true,
      message: 'Metadata sync completed successfully',
      stats: {
        totalTables: 10,
        newTables: 2,
        updatedTables: 1,
        skippedTables: 7,
        processingTimeMs: 5000,
      },
    };

    jest.spyOn(service, 'syncMetadata').mockResolvedValue(mockResponse);

    const result = await controller.syncMetadata();

    expect(result).toEqual(mockResponse);
    expect(service.syncMetadata).toHaveBeenCalled();
  });

  it('should return error response when sync fails', async () => {
    const mockResponse: SyncResponseDto = {
      success: false,
      message: 'Metadata sync failed',
      stats: {
        totalTables: 0,
        newTables: 0,
        updatedTables: 0,
        skippedTables: 0,
        processingTimeMs: 1000,
      },
      errors: ['Connection failed'],
    };

    jest.spyOn(service, 'syncMetadata').mockResolvedValue(mockResponse);

    const result = await controller.syncMetadata();

    expect(result).toEqual(mockResponse);
    expect(result.success).toBe(false);
    expect(result.errors).toBeDefined();
    expect(service.syncMetadata).toHaveBeenCalled();
  });

  it('should handle service exceptions gracefully', async () => {
    const error = new Error('Service error');
    jest.spyOn(service, 'syncMetadata').mockRejectedValue(error);

    await expect(controller.syncMetadata()).rejects.toThrow('Service error');
    expect(service.syncMetadata).toHaveBeenCalled();
  });
}); 