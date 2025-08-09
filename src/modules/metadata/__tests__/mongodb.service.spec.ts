import { Test, TestingModule } from '@nestjs/testing';
import { MongodbService } from '../services/mongodb.service';
import { getModelToken } from '@nestjs/mongoose';
import { Metadata } from '../schemas/metadata.schema';
import { SyncStats } from '../schemas/sync.schema';
import { SnowflakeTable } from '../services/snowflake.service';

describe('MongodbService', () => {
  let service: MongodbService;
  let metadataModel: any;
  let syncStatsModel: any;

  beforeEach(async () => {
    metadataModel = {
      find: jest.fn(),
      bulkWrite: jest.fn(),
      findOne: jest.fn(),
      create: jest.fn(),
      updateOne: jest.fn(),
      countDocuments: jest.fn(),
      findOneAndUpdate: jest.fn(),
    };

    syncStatsModel = {
      findOne: jest.fn(),
      create: jest.fn(),
      find: jest.fn(),
      findById: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MongodbService,
        { provide: getModelToken(Metadata.name), useValue: metadataModel },
        { provide: getModelToken(SyncStats.name), useValue: syncStatsModel },
      ],
    }).compile();
    service = module.get<MongodbService>(MongodbService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('upsertMetadata', () => {
    it('should return zero counts for empty tables array', async () => {
      const result = await service.upsertMetadata([]);
      expect(result).toEqual({
        newTables: 0,
        updatedTables: 0,
        skippedTables: 0
      });
    });

    it('should create new record if not existing', async () => {
      const table: SnowflakeTable = {
        database: 'db',
        schema: 'sch',
        table: 'tbl',
        columns: []
      };

      metadataModel.find.mockResolvedValue([]);
      metadataModel.bulkWrite.mockResolvedValue({
        insertedCount: 1,
        modifiedCount: 0
      });

      const result = await service.upsertMetadata([table]);
      expect(result.newTables).toBe(1);
      expect(result.updatedTables).toBe(0);
      expect(result.skippedTables).toBe(0);
      expect(metadataModel.bulkWrite).toHaveBeenCalledWith(
        expect.arrayContaining([
          expect.objectContaining({
            insertOne: expect.objectContaining({
              document: expect.objectContaining({
                database: 'db',
                schema: 'sch',
                table: 'tbl'
              })
            })
          })
        ]),
        { ordered: false }
      );
    });

    it('should update record if checksum differs', async () => {
      const table: SnowflakeTable = {
        database: 'db',
        schema: 'sch',
        table: 'tbl',
        columns: [{ name: 'col', type: 'string', nullable: true }]
      };

      const existingRecord = {
        database: 'db',
        schema: 'sch',
        table: 'tbl',
        checksum: 'old-checksum'
      };

      metadataModel.find.mockResolvedValue([existingRecord]);
      metadataModel.bulkWrite.mockResolvedValue({
        insertedCount: 0,
        modifiedCount: 1
      });

      const result = await service.upsertMetadata([table]);
      expect(result.newTables).toBe(0);
      expect(result.updatedTables).toBe(1);
      expect(result.skippedTables).toBe(0);
      expect(metadataModel.bulkWrite).toHaveBeenCalledWith(
        expect.arrayContaining([
          expect.objectContaining({
            updateOne: expect.objectContaining({
              filter: {
                database: 'db',
                schema: 'sch',
                table: 'tbl'
              }
            })
          })
        ]),
        { ordered: false }
      );
    });

    it('should skip record if checksum matches', async () => {
      const table: SnowflakeTable = {
        database: 'db',
        schema: 'sch',
        table: 'tbl',
        columns: []
      };

      const checksum = service['generateChecksum'](table);
      const existingRecord = {
        database: 'db',
        schema: 'sch',
        table: 'tbl',
        checksum
      };

      metadataModel.find.mockResolvedValue([existingRecord]);
      metadataModel.bulkWrite.mockResolvedValue({
        insertedCount: 0,
        modifiedCount: 0
      });

      const result = await service.upsertMetadata([table]);
      expect(result.newTables).toBe(0);
      expect(result.updatedTables).toBe(0);
      expect(result.skippedTables).toBe(1);
    });

    it('should handle multiple tables correctly', async () => {
      const tables: SnowflakeTable[] = [
        {
          database: 'db1',
          schema: 'sch1',
          table: 'tbl1',
          columns: []
        },
        {
          database: 'db2',
          schema: 'sch2',
          table: 'tbl2',
          columns: [{ name: 'col', type: 'string', nullable: true }]
        }
      ];

      metadataModel.find.mockResolvedValue([]);
      metadataModel.bulkWrite.mockResolvedValue({
        insertedCount: 2,
        modifiedCount: 0
      });

      const result = await service.upsertMetadata(tables);
      expect(result.newTables).toBe(2);
      expect(result.updatedTables).toBe(0);
      expect(result.skippedTables).toBe(0);
    });

    it('should handle errors gracefully', async () => {
      const table: SnowflakeTable = {
        database: 'db',
        schema: 'sch',
        table: 'tbl',
        columns: []
      };

      metadataModel.find.mockRejectedValue(new Error('Database error'));

      await expect(service.upsertMetadata([table])).rejects.toThrow('Database error');
    });
  });

  describe('generateChecksum', () => {
    it('should generate consistent checksums for same data', () => {
      const table: SnowflakeTable = {
        database: 'db',
        schema: 'sch',
        table: 'tbl',
        columns: [
          { name: 'col1', type: 'string', nullable: true },
          { name: 'col2', type: 'int', nullable: false }
        ]
      };

      const checksum1 = service['generateChecksum'](table);
      const checksum2 = service['generateChecksum'](table);

      expect(checksum1).toBe(checksum2);
      expect(typeof checksum1).toBe('string');
      expect(checksum1.length).toBeGreaterThan(0);
    });

    it('should generate different checksums for different data', () => {
      const table1: SnowflakeTable = {
        database: 'db',
        schema: 'sch',
        table: 'tbl',
        columns: [{ name: 'col1', type: 'string', nullable: true }]
      };

      const table2: SnowflakeTable = {
        database: 'db',
        schema: 'sch',
        table: 'tbl',
        columns: [{ name: 'col2', type: 'int', nullable: false }]
      };

      const checksum1 = service['generateChecksum'](table1);
      const checksum2 = service['generateChecksum'](table2);

      expect(checksum1).not.toBe(checksum2);
    });
  });

  describe('getSyncStats', () => {
    it('should return last sync time when available', async () => {
      const mockLastSync = {
        syncEndTime: new Date('2023-01-01T10:00:00Z')
      };

      syncStatsModel.findOne.mockReturnValue({
        sort: jest.fn().mockReturnValue({
          select: jest.fn().mockResolvedValue(mockLastSync)
        })
      });

      const result = await service.getSyncStats();
      expect(result.lastSyncTime).toEqual(mockLastSync.syncEndTime);
    });

    it('should return null when no sync stats available', async () => {
      syncStatsModel.findOne.mockReturnValue({
        sort: jest.fn().mockReturnValue({
          select: jest.fn().mockResolvedValue(null)
        })
      });

      const result = await service.getSyncStats();
      expect(result.lastSyncTime).toBeNull();
    });
  });

  describe('saveSyncStats', () => {
    it('should save sync stats successfully', async () => {
      const syncResponse = {
        success: true,
        message: 'Sync completed',
        stats: {
          totalTables: 10,
          newTables: 2,
          updatedTables: 3,
          skippedTables: 5,
          processingTimeMs: 1000
        }
      };

      const startTime = new Date('2023-01-01T10:00:00Z');
      const endTime = new Date('2023-01-01T10:01:00Z');

      syncStatsModel.create.mockResolvedValue({});

      await service.saveSyncStats(syncResponse as any, startTime, endTime);

      expect(syncStatsModel.create).toHaveBeenCalledWith({
        syncStartTime: startTime,
        syncEndTime: endTime,
        success: true,
        totalTables: 10,
        newTables: 2,
        updatedTables: 3,
        skippedTables: 5,
        processingTimeMs: 1000,
        errors: [],
        message: 'Sync completed'
      });
    });

    it('should handle errors when saving sync stats', async () => {
      const syncResponse = {
        success: false,
        message: 'Sync failed',
        stats: {
          totalTables: 0,
          newTables: 0,
          updatedTables: 0,
          skippedTables: 0,
          processingTimeMs: 0
        },
        errors: ['Error 1', 'Error 2']
      };

      const startTime = new Date('2023-01-01T10:00:00Z');
      const endTime = new Date('2023-01-01T10:01:00Z');

      syncStatsModel.create.mockRejectedValue(new Error('Save failed'));

      // Should not throw error
      await expect(service.saveSyncStats(syncResponse as any, startTime, endTime)).resolves.toBeUndefined();
    });
  });

  describe('getSyncHistory', () => {
    it('should return sync history', async () => {
      const mockHistory = [
        { id: '1', syncEndTime: new Date('2023-01-01T10:00:00Z') },
        { id: '2', syncEndTime: new Date('2023-01-01T09:00:00Z') }
      ];

      syncStatsModel.find.mockReturnValue({
        sort: jest.fn().mockReturnValue({
          limit: jest.fn().mockReturnValue({
            exec: jest.fn().mockResolvedValue(mockHistory)
          })
        })
      });

      const result = await service.getSyncHistory(5);
      expect(result).toEqual(mockHistory);
    });

    it('should handle errors when getting sync history', async () => {
      syncStatsModel.find.mockReturnValue({
        sort: jest.fn().mockReturnValue({
          limit: jest.fn().mockReturnValue({
            exec: jest.fn().mockRejectedValue(new Error('Query failed'))
          })
        })
      });

      const result = await service.getSyncHistory(5);
      expect(result).toEqual([]);
    });
  });

  describe('getSyncStatsById', () => {
    it('should return sync stats by ID', async () => {
      const mockStats = {
        id: '1',
        syncEndTime: new Date('2023-01-01T10:00:00Z'),
        success: true
      };

      syncStatsModel.findById.mockReturnValue({
        exec: jest.fn().mockResolvedValue(mockStats)
      });

      const result = await service.getSyncStatsById('1');
      expect(result).toEqual(mockStats);
    });

    it('should return null when sync stats not found', async () => {
      syncStatsModel.findById.mockReturnValue({
        exec: jest.fn().mockResolvedValue(null)
      });

      const result = await service.getSyncStatsById('1');
      expect(result).toBeNull();
    });

    it('should handle errors when getting sync stats by ID', async () => {
      syncStatsModel.findById.mockReturnValue({
        exec: jest.fn().mockRejectedValue(new Error('Query failed'))
      });

      const result = await service.getSyncStatsById('1');
      expect(result).toBeNull();
    });
  });

  describe('Metrics Logging', () => {
    it('should log metrics correctly', () => {
      service['operationCount'] = 5;
      service['totalOperationDuration'] = 1000;
      service['errorCount'] = 1;

      const logSpy = jest.spyOn(service['logger'], 'log');
      service['logMetrics']('Test Metrics', { additionalData: 'test' });

      expect(logSpy).toHaveBeenCalledWith(
        expect.stringContaining('[METRICS]')
      );
    });
  });
});