import { Test, TestingModule } from '@nestjs/testing';
import { MongodbService } from '../services/mongodb.service';
import { getModelToken } from '@nestjs/mongoose';
import { Metadata } from '../schemas/metadata.schema';

describe('MongodbService', () => {
  let service: MongodbService;
  let model: any;

  beforeEach(async () => {
    model = {
      findOne: jest.fn(),
      create: jest.fn(),
      updateOne: jest.fn(),
      countDocuments: jest.fn(),
      find: jest.fn(),
      findOneAndUpdate: jest.fn(),
    };
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MongodbService,
        { provide: getModelToken(Metadata.name), useValue: model },
      ],
    }).compile();
    service = module.get<MongodbService>(MongodbService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  it('should create new record if not existing', async () => {
    model.findOne.mockResolvedValue(null);
    model.create.mockResolvedValue({});
    const result = await service.upsertMetadata([
      { database: 'db', schema: 'sch', table: 'tbl', columns: [] }
    ] as any);
    expect(result.newTables).toBe(1);
    expect(result.updatedTables).toBe(0);
    expect(result.skippedTables).toBe(0);
  });

  it('should update record if checksum differs', async () => {
    model.findOne.mockResolvedValue({ checksum: 'old' });
    model.updateOne.mockResolvedValue({});
    const result = await service.upsertMetadata([
      { database: 'db', schema: 'sch', table: 'tbl', columns: [{ name: 'col', type: 'string', nullable: true }] }
    ] as any);
    expect(result.newTables).toBe(0);
    expect(result.updatedTables).toBe(1);
    expect(result.skippedTables).toBe(0);
  });

  it('should skip record if checksum matches', async () => {
    const table = { database: 'db', schema: 'sch', table: 'tbl', columns: [] };
    const checksum = service['generateChecksum'](table as any);
    model.findOne.mockResolvedValue({ checksum });
    const result = await service.upsertMetadata([table as any]);
    expect(result.newTables).toBe(0);
    expect(result.updatedTables).toBe(0);
    expect(result.skippedTables).toBe(1);
  });

  it('should handle errors gracefully', async () => {
    model.findOne.mockRejectedValue(new Error('fail'));
    const result = await service.upsertMetadata([
      { database: 'db', schema: 'sch', table: 'tbl', columns: [] }
    ] as any);
    expect(result.newTables + result.updatedTables + result.skippedTables).toBe(0);
  });

  it('should update (not insert) when table is changed', async () => {
    // Simulate an existing record with a different checksum
    model.findOne.mockResolvedValue({ checksum: 'old' });
    model.create.mockClear();
    model.updateOne.mockClear();
    model.updateOne.mockResolvedValue({});
    const table = { database: 'db', schema: 'sch', table: 'tbl', columns: [{ name: 'col', type: 'string', nullable: true }] };
    await service.upsertMetadata([table as any]);
    expect(model.updateOne).toHaveBeenCalled();
    expect(model.create).not.toHaveBeenCalled();
  });

  it('should insert (not update) when table is new', async () => {
    // Simulate no existing record
    model.findOne.mockResolvedValue(null);
    model.create.mockClear();
    model.updateOne.mockClear();
    model.create.mockResolvedValue({});
    const table = { database: 'db', schema: 'sch', table: 'newtbl', columns: [{ name: 'col', type: 'string', nullable: true }] };
    await service.upsertMetadata([table as any]);
    expect(model.create).toHaveBeenCalled();
    expect(model.updateOne).not.toHaveBeenCalled();
  });
});