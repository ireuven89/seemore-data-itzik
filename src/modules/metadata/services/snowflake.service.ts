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

  private createConnection(): snowflake.Connection {
    return snowflake.createConnection(DatabaseConfig.getSnowflakeConfig());
  }

  private checkEnvVars() {
    const requiredVars = [
      'SNOWFLAKE_ACCOUNT',
      'SNOWFLAKE_USERNAME',
      'SNOWFLAKE_PASSWORD',
      'SNOWFLAKE_WAREHOUSE',
      'SNOWFLAKE_ROLE'
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
    const tables: Array<{database: string, schema: string, table: string}> = [];

    try {
      const databases = await this.executeQuery(`SHOW DATABASES`);

      for (const db of databases) {
        if (this.isSystemDatabase(db.name)) continue;

        const schemas = await this.getSchemasForDatabase(db.name);

        for (const schema of schemas) {
          if (this.isSystemSchema(schema.name)) continue;

          const schemaTables = await this.getTablesForSchema(db.name, schema.name);

          for (const table of schemaTables) {
            //if existing table change detection is needed - for modified tables - remove or comment this line
            if (this.shouldSkipTable(table, lastSyncTime)) continue;

            tables.push({
              database: db.name,
              schema: schema.name,
              table: table.name || table.NAME
            });
          }
        }
      }

      this.logger.log(`Found ${tables.length} tables across all databases`);
      return tables;

    } catch (error) {
      this.logger.error('Failed to get tables using SHOW commands', error);
      throw error
    }
  }


  private isSystemDatabase(dbName: string): boolean {
    return ['SNOWFLAKE', 'INFORMATION_SCHEMA'].includes(dbName?.toUpperCase());
  }

  private isSystemSchema(schemaName: string): boolean {
    return schemaName?.toUpperCase() === 'INFORMATION_SCHEMA';
  }

  private shouldSkipTable(table: any, lastSyncTime?: Date): boolean {
    if (!lastSyncTime) return false;

    const createdTime = new Date(table.created_on || table.CREATED_ON || Date.now());

    // Only skip tables created before last sync
    return createdTime <= lastSyncTime;
  }

  private async getSchemasForDatabase(databaseName: string): Promise<any[]> {
    try {
      return await this.executeQuery(`SHOW SCHEMAS IN DATABASE "${databaseName}"`);
    } catch (err) {
      this.logger.warn(`Could not get schemas from ${databaseName}:`, err.message);
      return [];
    }
  }

  private async getTablesForSchema(databaseName: string, schemaName: string): Promise<any[]> {
    try {
      return await this.executeQuery(`SHOW TABLES IN SCHEMA "${databaseName}"."${schemaName}"`);
    } catch (err) {
      this.logger.warn(`Could not get tables from ${databaseName}.${schemaName}:`, err.message);
      return [];
    }
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
      return await this.getTablesWithIndividualColumnQueries(tables);
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

  private async getTablesWithIndividualColumnQueries(tables: Array<{database: string, schema: string, table: string}>): Promise<SnowflakeTable[]> {
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