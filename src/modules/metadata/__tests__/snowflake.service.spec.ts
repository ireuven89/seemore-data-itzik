jest.mock('snowflake-sdk', () => ({
  createConnection: jest.fn(),
}));
import { Test, TestingModule } from '@nestjs/testing';
import { SnowflakeService } from '../services/snowflake.service';
import * as snowflake from 'snowflake-sdk';

describe('SnowflakeService', () => {
  let service: SnowflakeService;
  let mockConnection: any;

  beforeEach(async () => {
    mockConnection = {
      connect: jest.fn(cb => cb(null)),
      destroy: jest.fn(cb => cb()),
      execute: jest.fn(({ complete }) => complete(null, {}, [{ foo: 'bar' }])),
    };
    (snowflake.createConnection as jest.Mock).mockReturnValue(mockConnection);
    const module: TestingModule = await Test.createTestingModule({
      providers: [SnowflakeService],
    }).compile();
    service = module.get<SnowflakeService>(SnowflakeService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  it('should connect and disconnect without error', async () => {
    service['connection'] = mockConnection;
    await expect(service['connect']()).resolves.toBeUndefined();
    await expect(service['disconnect']()).resolves.toBeUndefined();
  });

  it('should execute a query and return rows', async () => {
    service['connection'] = mockConnection;
    await service['connect']();
    const result = await (service as any).executeQuery('SELECT 1');
    expect(result).toEqual([{ foo: 'bar' }]);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });
});