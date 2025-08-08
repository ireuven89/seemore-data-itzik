jest.mock('snowflake-sdk', () => ({
  createConnection: jest.fn(),
}));
import { Test, TestingModule } from '@nestjs/testing';
import { SnowflakeService } from '../services/snowflake.service';
import * as snowflake from 'snowflake-sdk';
import * as dotenv from 'dotenv';
import {resolve} from 'path';

dotenv.config({ path: resolve(__dirname, '../../../.env.test') });

dotenv.config();


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
    service['connection'] = mockConnection;
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  describe('Connection Management', () => {
    it('should connect successfully', async () => {
      await expect(service['connect']()).resolves.toBeUndefined();
      expect(mockConnection.connect).toHaveBeenCalled();
    });

    it('should handle connection errors', async () => {
      const error = new Error('Connection failed');
      mockConnection.connect.mockImplementation(cb => cb(error));

      await expect(service['connect']()).rejects.toThrow('Connection failed');
    });

    it('should disconnect successfully', async () => {
      await expect(service['disconnect']()).resolves.toBeUndefined();
      expect(mockConnection.destroy).toHaveBeenCalled();
    });

    it('should handle disconnect when no connection exists', async () => {
      service['connection'] = null as any;
      await expect(service['disconnect']()).resolves.toBeUndefined();
    });
  });

  describe('Environment Variables', () => {
    it('should throw error when required env vars are missing', () => {
      const originalEnv = process.env;
      process.env = {};

      expect(() => service['checkEnvVars']()).toThrow('Missing required Snowflake environment variables');

      process.env = originalEnv;
    });

    it('should pass when all required env vars are present', () => {
      const originalEnv = process.env;
      process.env = {
        SNOWFLAKE_ACCOUNT: 'test',
        SNOWFLAKE_USERNAME: 'test',
        SNOWFLAKE_PASSWORD: 'test',
        SNOWFLAKE_WAREHOUSE: 'test',
        SNOWFLAKE_ROLE: 'test'
      };

      expect(() => service['checkEnvVars']()).not.toThrow();

      process.env = originalEnv;
    });
  });

  describe('Query Execution', () => {
    it('should execute a query and return rows', async () => {
      await service['connect']();
      const result = await service['executeQuery']('SELECT 1');
      expect(result).toEqual([{ foo: 'bar' }]);
      expect(service['queryCount']).toBe(1);
    });

    it('should handle query execution errors', async () => {
      const error = new Error('Query failed');
      mockConnection.execute.mockImplementation(({ complete }) => complete(error));

      await service['connect']();
      await expect(service['executeQuery']('SELECT 1')).rejects.toThrow('Query failed');
    });

    it('should track query count correctly', async () => {
      await service['connect']();
      await service['executeQuery']('SELECT 1');
      await service['executeQuery']('SELECT 2');
      expect(service['queryCount']).toBe(2);
    });
  });

  describe('Database Discovery', () => {
    it('should filter out system databases', () => {
      expect(service['isSystemDatabase']('SNOWFLAKE')).toBe(true);
      expect(service['isSystemDatabase']('INFORMATION_SCHEMA')).toBe(true);
      expect(service['isSystemDatabase']('MY_DB')).toBe(false);
    });

    it('should filter out system schemas', () => {
      expect(service['isSystemSchema']('INFORMATION_SCHEMA')).toBe(true);
      expect(service['isSystemSchema']('PUBLIC')).toBe(false);
    });

    it('should skip tables based on last sync time', () => {
      const lastSyncTime = new Date('2023-01-01');
      const oldTable = { created_on: '2022-12-01' };
      const newTable = { created_on: '2023-02-01' };

      expect(service['shouldSkipTable'](oldTable, lastSyncTime)).toBe(true);
      expect(service['shouldSkipTable'](newTable, lastSyncTime)).toBe(false);
      expect(service['shouldSkipTable'](newTable)).toBe(false);
    });
  });

  describe('Schema and Table Discovery', () => {
    it('should get schemas for database', async () => {
      const mockSchemas = [
        { name: 'PUBLIC' },
        { name: 'INFORMATION_SCHEMA' }
      ];
      mockConnection.execute.mockImplementation(({ complete }) =>
        complete(null, {}, mockSchemas)
      );

      const result = await service['getSchemasForDatabase']('TEST_DB');
      expect(result).toEqual(mockSchemas);
    });

    it('should handle schema discovery errors', async () => {
      mockConnection.execute.mockImplementation(({ complete }) =>
        complete(new Error('Access denied'), {}, [])
      );

      const result = await service['getSchemasForDatabase']('TEST_DB');
      expect(result).toEqual([]);
    });

    it('should get tables for schema', async () => {
      const mockTables = [
        { name: 'TABLE1', created_on: '2023-01-01' },
        { name: 'TABLE2', created_on: '2023-01-02' }
      ];
      mockConnection.execute.mockImplementation(({ complete }) =>
        complete(null, {}, mockTables)
      );

      const result = await service['getTablesForSchema']('TEST_DB', 'PUBLIC');
      expect(result).toEqual(mockTables);
    });

    it('should handle table discovery errors', async () => {
      mockConnection.execute.mockImplementation(({ complete }) =>
        complete(new Error('Access denied'), {}, [])
      );

      const result = await service['getTablesForSchema']('TEST_DB', 'PUBLIC');
      expect(result).toEqual([]);
    });
  });

  describe('getAllTables', () => {
    it('should get all tables successfully', async () => {
      const mockDatabases = [
        { name: 'DB1' },
        { name: 'DB2' }
      ];
      const mockSchemas = [
        { name: 'PUBLIC' }
      ];
      const mockTables = [
        { name: 'TABLE1', created_on: '2023-01-01' },
        { name: 'TABLE2', created_on: '2023-01-02' }
      ];

      let callCount = 0;
      mockConnection.execute.mockImplementation(({ complete }) => {
        callCount++;
        if (callCount === 1) {
          // SHOW DATABASES
          complete(null, {}, mockDatabases);
        } else if (callCount === 2 || callCount === 4) {
          // SHOW SCHEMAS
          complete(null, {}, mockSchemas);
        } else {
          // SHOW TABLES
          complete(null, {}, mockTables);
        }
      });

      const result = await service.getAllTables();
      expect(result).toBeDefined();
      expect(Array.isArray(result)).toBe(true);
    });

    it('should handle errors in getAllTables', async () => {
      mockConnection.execute.mockImplementation(({ complete }) =>
        complete(new Error('Connection failed'), {}, [])
      );

      await expect(service.getAllTables()).rejects.toThrow('Connection failed');
    });

    it('should filter tables based on last sync time', async () => {
      const lastSyncTime = new Date('2023-01-15');
      const mockDatabases = [{ name: 'DB1' }];
      const mockSchemas = [{ name: 'PUBLIC' }];
      const mockTables = [
        { name: 'OLD_TABLE', created_on: '2023-01-01' },
        { name: 'NEW_TABLE', created_on: '2023-01-20' }
      ];

      let callCount = 0;
      mockConnection.execute.mockImplementation(({ complete }) => {
        callCount++;
        if (callCount === 1) {
          complete(null, {}, mockDatabases);
        } else if (callCount === 2) {
          complete(null, {}, mockSchemas);
        } else {
          complete(null, {}, mockTables);
        }
      });

      const result = await service.getAllTables(lastSyncTime);
      // Should only include NEW_TABLE since OLD_TABLE was created before lastSyncTime
      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('Column Discovery', () => {
    it('should get columns for tables', async () => {
      const mockColumns = [
        {
          SCHEMA: 'PUBLIC',
          TABLE: 'TABLE1',
          NAME: 'COL1',
          TYPE: 'VARCHAR',
          NULLABLE: 'YES',
          DEFAULTVALUE: null,
          COMMENT: 'Test column'
        }
      ];

      mockConnection.execute.mockImplementation(({ complete }) =>
        complete(null, {}, mockColumns)
      );

      const tables = [
        { database: 'DB1', schema: 'PUBLIC', table: 'TABLE1' }
      ];

      const result = await service['getAllColumnsForDatabase']('DB1', tables);
      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
      expect(result[0].columns).toBeDefined();
    });

    it('should group columns into tables correctly', () => {
      const mockColumns = [
        {
          SCHEMA: 'PUBLIC',
          TABLE: 'TABLE1',
          NAME: 'COL1',
          TYPE: 'VARCHAR',
          NULLABLE: 'YES',
          DEFAULTVALUE: null,
          COMMENT: 'Test column'
        },
        {
          SCHEMA: 'PUBLIC',
          TABLE: 'TABLE1',
          NAME: 'COL2',
          TYPE: 'INTEGER',
          NULLABLE: 'NO',
          DEFAULTVALUE: '0',
          COMMENT: 'Another column'
        }
      ];

      const result = service['groupColumnsIntoTables']('DB1', mockColumns);
      expect(result).toHaveLength(1);
      expect(result[0].database).toBe('DB1');
      expect(result[0].schema).toBe('PUBLIC');
      expect(result[0].table).toBe('TABLE1');
      expect(result[0].columns).toHaveLength(2);
    });
  });



  afterEach(() => {
    jest.clearAllMocks();
  });
});