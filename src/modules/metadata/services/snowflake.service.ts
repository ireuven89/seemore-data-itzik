import { Injectable, Logger } from '@nestjs/common';
import * as snowflake from 'snowflake-sdk';
import { DatabaseConfig } from '../../../config/database.config';

export interface SnowflakeTable {
  database: string;
  schema: string;
  table: string;
  columns: Array<{
    name: string;
    type: string;
    nullable: boolean;
    defaultValue?: string;
    comment?: string;
  }>;
}

@Injectable()
export class SnowflakeService {
  private readonly logger = new Logger(SnowflakeService.name);
  private connection: snowflake.Connection;
  private queryCount = 0;

  constructor() {
  }

  private createConnection(): snowflake.Connection {
    return snowflake.createConnection(DatabaseConfig.getSnowflakeConfig());
  }

  private checkEnvVars() {
    const requiredVars = [
      'SNOWFLAKE_ACCOUNT',
      'SNOWFLAKE_USERNAME',
      'SNOWFLAKE_PASSWORD',
      'SNOWFLAKE_WAREHOUSE',
    ];
    const missing = requiredVars.filter((v) => !process.env[v]);
    if (missing.length > 0) {
      throw new Error(`Missing required Snowflake environment variables: ${missing.join(', ')}`);
    }
  }

  async connect(): Promise<void> {
    this.checkEnvVars();
    return new Promise((resolve, reject) => {
      this.connection.connect((err) => {
        if (err) {
          this.logger.error('Failed to connect to Snowflake', err);
          reject(err);
        } else {
          this.logger.log('Successfully connected to Snowflake');
          resolve();
        }
      });
    });
  }

  async disconnect(): Promise<void> {

    return new Promise((resolve) => {
      if (this.connection) {
        this.connection.destroy(() => {
          this.logger.log(`Disconnected from Snowflake. Total queries executed: ${this.queryCount}`);
          resolve();
        });
      } else {
        this.logger.warn('No Snowflake connection to disconnect.');
        resolve();
      }
    });
  }

  async getAllTables(lastSyncTime?: Date): Promise<SnowflakeTable[]> {
    try {
      this.connection = this.createConnection();
      await this.connect();
      this.queryCount = 0;
      const allTables = await this.getAllTablesOptimized(lastSyncTime);
      await this.disconnect();
      return allTables;
    } catch (error) {
      this.logger.error('Error fetching Snowflake metadata', error);
      await this.disconnect();
      throw error;
    }
  }

  private async getAllTablesOptimized(lastSyncTime?: Date): Promise<SnowflakeTable[]> {
    this.logger.log('Starting optimized metadata extraction...');
    const allTablesMetadata = await this.getAllTablesInAccount(lastSyncTime);
    if (allTablesMetadata.length === 0) {
      this.logger.warn('No tables found in account');
      return [];
    }
    const tablesByDatabase = this.groupTablesByDatabase(allTablesMetadata);
    const allTables: SnowflakeTable[] = [];
    for (const [database, tables] of Object.entries(tablesByDatabase)) {
      this.logger.log(`Processing ${tables.length} tables in database: ${database}`);
      const tablesWithColumns = await this.getAllColumnsForDatabase(database, tables);
      allTables.push(...tablesWithColumns);
    }
    this.logger.log(`Optimization complete. Retrieved ${allTables.length} tables with ${this.queryCount} queries`);
    return allTables;
  }

  private async getAllTablesInAccount(lastSyncTime?: Date): Promise<Array<{database: string, schema: string, table: string}>> {
    let query = `
      SELECT 
        TABLE_CATALOG as database,
        TABLE_SCHEMA as schema,
        TABLE_NAME as table,
        LAST_ALTERED
      FROM INFORMATION_SCHEMA.TABLES 
      WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
      AND TABLE_CATALOG != 'SNOWFLAKE'
    `;
    if (lastSyncTime) {
      const iso = lastSyncTime.toISOString().replace('T', ' ').replace('Z', '');
      query += ` AND LAST_ALTERED > '${iso}'`;
    }
    query += ` ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME`;
    try {
      const rows = await this.executeQuery(query);
      return rows.map(row => ({
        database: row.DATABASE,
        schema: row.SCHEMA,
        table: row.TABLE
      }));
    } catch (error) {
      this.logger.error('Failed to get tables from INFORMATION_SCHEMA, falling back to SHOW commands', error);
      return await this.getAllTablesWithShowCommands();
    }
  }

  private async getAllTablesWithShowCommands(): Promise<Array<{database: string, schema: string, table: string}>> {
    this.logger.warn('Using SHOW commands as fallback (less efficient)');
    const databases = await this.getDatabases();
    const allTables: Array<{database: string, schema: string, table: string}> = [];
    for (const database of databases) {
      const schemas = await this.getSchemas(database);
      for (const schema of schemas) {
        const tables = await this.getTables(database, schema);
        for (const table of tables) {
          allTables.push({ database, schema, table });
        }
      }
    }
    return allTables;
  }

  private async getAllColumnsForDatabase(database: string, tables: Array<{database: string, schema: string, table: string}>): Promise<SnowflakeTable[]> {
    const tableNames = tables.map(t => `'${t.table}'`).join(',');
    const schemaNames = [...new Set(tables.map(t => t.schema))].map(s => `'${s}'`).join(',');
    const query = `
      SELECT 
        TABLE_SCHEMA as schema,
        TABLE_NAME as table,
        COLUMN_NAME as name,
        DATA_TYPE as type,
        IS_NULLABLE as nullable,
        COLUMN_DEFAULT as defaultValue,
        COMMENT as comment,
        ORDINAL_POSITION as position
      FROM "${database}".INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA IN (${schemaNames})
      AND TABLE_NAME IN (${tableNames})
      ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    `;
    try {
      const rows = await this.executeQuery(query);
      return this.groupColumnsIntoTables(database, rows);
    } catch (error) {
      this.logger.error(`Failed to get columns for database ${database}, falling back to individual queries`, error);
      return await this.getTablesWithIndividualColumnQueries(database, tables);
    }
  }

  private groupColumnsIntoTables(database: string, columnRows: any[]): SnowflakeTable[] {
    const tablesMap = new Map<string, SnowflakeTable>();
    for (const row of columnRows) {
      const key = `${database}.${row.SCHEMA}.${row.TABLE}`;
      if (!tablesMap.has(key)) {
        tablesMap.set(key, {
          database,
          schema: row.SCHEMA,
          table: row.TABLE,
          columns: []
        });
      }
      tablesMap.get(key)!.columns.push({
        name: row.NAME,
        type: row.TYPE,
        nullable: row.NULLABLE === 'YES',
        defaultValue: row.DEFAULTVALUE,
        comment: row.COMMENT
      });
    }
    return Array.from(tablesMap.values());
  }

  private groupTablesByDatabase(tables: Array<{database: string, schema: string, table: string}>): Record<string, Array<{database: string, schema: string, table: string}>> {
    const grouped: Record<string, Array<{database: string, schema: string, table: string}>> = {};
    for (const table of tables) {
      if (!grouped[table.database]) {
        grouped[table.database] = [];
      }
      grouped[table.database].push(table);
    }
    return grouped;
  }

  private async getTablesWithIndividualColumnQueries(database: string, tables: Array<{database: string, schema: string, table: string}>): Promise<SnowflakeTable[]> {
    const result: SnowflakeTable[] = [];
    for (const tableInfo of tables) {
      const columns = await this.getTableColumns(tableInfo.database, tableInfo.schema, tableInfo.table);
      result.push({
        database: tableInfo.database,
        schema: tableInfo.schema,
        table: tableInfo.table,
        columns
      });
    }
    return result;
  }

  private async executeQuery(query: string): Promise<any[]> {
    this.queryCount++;
    if (this.queryCount % 10 == 0 ){
      this.logger.debug(`Executing query ${this.queryCount}: ${query.substring(0, 100)}...`);
    }
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText: query,
        complete: (err, stmt, rows) => {
          if (err) {
            this.logger.error(`Query ${this.queryCount} failed: ${query}`, err);
            reject(err);
          } else {
            this.logger.debug(`Query ${this.queryCount} returned ${rows?.length || 0} rows`);
            resolve(rows || []);
          }
        }
      });
    });
  }

  private async getDatabases(): Promise<string[]> {
    const query = `SHOW DATABASES`;
    const rows = await this.executeQuery(query);
    return rows.map(row => row.name).filter(name => 
      !['INFORMATION_SCHEMA', 'SNOWFLAKE'].includes(name.toUpperCase())
    );
  }

  private async getSchemas(database: string): Promise<string[]> {
    const query = `SHOW SCHEMAS IN DATABASE "${database}"`;
    const rows = await this.executeQuery(query);
    return rows.map(row => row.name).filter(name => 
      name.toUpperCase() !== 'INFORMATION_SCHEMA'
    );
  }

  private async getTables(database: string, schema: string): Promise<string[]> {
    const query = `SHOW TABLES IN SCHEMA "${database}"."${schema}"`;
    try {
      const rows = await this.executeQuery(query);
      return rows.map(row => row.name);
    } catch (error) {
      this.logger.warn(`Could not fetch tables for ${database}.${schema}`, error.message);
      return [];
    }
  }

  private async getTableColumns(database: string, schema: string, table: string): Promise<any[]> {
    const query = `
      SELECT 
        COLUMN_NAME as name,
        DATA_TYPE as type,
        IS_NULLABLE as nullable,
        COLUMN_DEFAULT as defaultValue,
        COMMENT as comment
      FROM "${database}".INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${schema}' 
      AND TABLE_NAME = '${table}'
      ORDER BY ORDINAL_POSITION
    `;
    try {
      const rows = await this.executeQuery(query);
      return rows.map(row => ({
        name: row.NAME,
        type: row.TYPE,
        nullable: row.NULLABLE === 'YES',
        defaultValue: row.DEFAULTVALUE,
        comment: row.COMMENT
      }));
    } catch (error) {
      this.logger.warn(`Could not fetch columns for ${database}.${schema}.${table}`, error.message);
      return [];
    }
  }
}