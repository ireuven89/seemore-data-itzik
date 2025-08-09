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
  private totalQueryDuration = 0;
  private errorCount = 0;
  private retryCount = 0;

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
          this.logMetrics('Snowflake Service Metrics');
          this.logger.log(`Disconnected from Snowflake. Total queries executed: ${this.queryCount}, Total duration: ${this.totalQueryDuration}ms, Total errors: ${this.errorCount}, Total retries: ${this.retryCount}`);
          resolve();
        });
      } else {
        this.logger.warn('No Snowflake connection to disconnect.');
        resolve();
      }
    });
  }

  async getAllTables(lastSyncTime?: Date): Promise<SnowflakeTable[]> {
    const operationStartTime = Date.now();
    this.logger.log(`🚀 Starting getAllTables operation${lastSyncTime ? ` (incremental sync since ${lastSyncTime})` : ' (full sync)'}`);
    
    try {
      this.connection = this.createConnection();
      await this.connect();
      this.queryCount = 0;
      this.totalQueryDuration = 0;
      this.errorCount = 0;
      this.retryCount = 0;
      
      // Use the optimized approach for better performance
      const allTables = await this.getAllTablesOptimized(lastSyncTime);
      
      await this.disconnect();
      
      const totalOperationTime = Date.now() - operationStartTime;
      this.logger.log(`🎯 getAllTables completed in ${totalOperationTime}ms: ${allTables.length} tables found, ${this.queryCount} queries executed, ${this.retryCount} retries, ${this.errorCount} errors`);
      
      return allTables;
    } catch (error) {
      const totalOperationTime = Date.now() - operationStartTime;
      this.logger.error(`💥 getAllTables failed after ${totalOperationTime}ms: ${error.message}`, error);
      await this.disconnect();
      throw error;
    }
  }

  /**
   * Optimized approach using set-based queries for better performance
   */
  private async getAllTablesOptimized(lastSyncTime?: Date): Promise<SnowflakeTable[]> {
    this.logger.log('Starting optimized metadata extraction...');

    try {
      // First, get all databases to build the query
      const databases = await this.executeQueryWithRetry(`SHOW DATABASES`);
      this.logger.log(`Found ${databases.length} total databases`);
      
      const userDatabases = databases.filter(db => !this.isSystemDatabase(db.name));
      this.logger.log(`Found ${userDatabases.length} user databases: ${userDatabases.map(db => db.name).join(', ')}`);
      
      if (userDatabases.length === 0) {
        this.logger.warn('No user databases found');
        return [];
      }
      
      // Build a UNION query for all databases
      const databaseQueries = userDatabases.map(db => `
        SELECT 
          '${db.name}' as database_name,
          TABLE_SCHEMA as schema_name,
          TABLE_NAME as table_name,
          COLUMN_NAME as column_name,
          DATA_TYPE as data_type,
          IS_NULLABLE as is_nullable,
          COLUMN_DEFAULT as column_default,
          COMMENT as column_comment,
          ORDINAL_POSITION as ordinal_position
        FROM "${db.name}".INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA'
          AND TABLE_SCHEMA NOT LIKE 'SNOWFLAKE%'
      `);
      
      const query = databaseQueries.join(' UNION ALL ') + `
        ORDER BY database_name, schema_name, table_name, ordinal_position
      `;

      this.logger.debug(`Executing set-based query for ${userDatabases.length} databases`);
      const rows = await this.executeQueryWithRetry(query);
      this.logger.log(`Retrieved ${rows.length} column records from ${userDatabases.length} databases`);
      
      if (rows.length === 0) {
        this.logger.warn('No column records found in set-based query');
        return [];
      }
      
      // Group columns into tables in memory
      const tablesMap = new Map<string, SnowflakeTable>();
      
      for (const row of rows) {
        // Handle both uppercase and lowercase field names
        const databaseName = row.DATABASE_NAME || row.database_name;
        const schemaName = row.SCHEMA_NAME || row.schema_name;
        const tableName = row.TABLE_NAME || row.table_name;
        const columnName = row.COLUMN_NAME || row.column_name;
        const dataType = row.DATA_TYPE || row.data_type;
        const isNullable = row.IS_NULLABLE || row.is_nullable;
        const columnDefault = row.COLUMN_DEFAULT || row.column_default;
        const columnComment = row.COLUMN_COMMENT || row.column_comment;
        
        if (!databaseName || !schemaName || !tableName || !columnName) {
          this.logger.warn(`Skipping row with missing required fields: ${JSON.stringify(row)}`);
          continue;
        }
        
        const key = `${databaseName}.${schemaName}.${tableName}`;
        
        if (!tablesMap.has(key)) {
          tablesMap.set(key, {
            database: databaseName,
            schema: schemaName,
            table: tableName,
            columns: []
          });
        }
        
        tablesMap.get(key)!.columns.push({
          name: columnName,
          type: dataType,
          nullable: isNullable === 'YES',
          defaultValue: columnDefault,
          comment: columnComment
        });
      }
      
      const allTables = Array.from(tablesMap.values());
      this.logger.log(`Grouped into ${allTables.length} tables`);
      
      // Apply lastSyncTime filtering if provided
      if (lastSyncTime) {
        const filteredTables = await this.filterTablesByLastSyncTimeOptimized(allTables, lastSyncTime);
        this.logger.log(`Filtered to ${filteredTables.length} tables after lastSyncTime: ${lastSyncTime}`);
        return filteredTables;
      }
      
      return allTables;
      
    } catch (error) {
      this.logger.error('Failed to execute set-based query, falling back to simple approach', error);
      return await this.getAllTablesSimple(lastSyncTime);
    }
  }

  /**
   * Simple, reliable approach as fallback
   */
  private async getAllTablesSimple(lastSyncTime?: Date): Promise<SnowflakeTable[]> {
    this.logger.log('Starting simple metadata extraction (fallback)...');
    
    const allTables: SnowflakeTable[] = [];
    
    try {
      // Get all databases
      const databases = await this.executeQueryWithRetry(`SHOW DATABASES`);
      this.logger.log(`Found ${databases.length} databases`);
      
      for (const db of databases) {
        if (this.isSystemDatabase(db.name)) {
          this.logger.debug(`Skipping system database: ${db.name}`);
          continue;
        }
        
        this.logger.log(`Processing database: ${db.name}`);
        
        // Get schemas for this database
        const schemas = await this.executeQueryWithRetry(`SHOW SCHEMAS IN DATABASE "${db.name}"`);
        this.logger.log(`Found ${schemas.length} schemas in database ${db.name}`);
        
        for (const schema of schemas) {
          if (this.isSystemSchema(schema.name)) {
            this.logger.debug(`Skipping system schema: ${db.name}.${schema.name}`);
            continue;
          }
          
          this.logger.log(`Processing schema: ${db.name}.${schema.name}`);
          
          // Get tables for this schema
          const tables = await this.executeQueryWithRetry(`SHOW TABLES IN SCHEMA "${db.name}"."${schema.name}"`);
          this.logger.log(`Found ${tables.length} tables in schema ${db.name}.${schema.name}`);
          
          for (const table of tables) {
            const tableName = table.name || table.NAME;
            if (!tableName) {
              this.logger.warn(`Skipping table with no name in ${db.name}.${schema.name}`);
              continue;
            }
            
            this.logger.log(`Processing table: ${db.name}.${schema.name}.${tableName}`);
            
            // Get columns for this table
            const columns = await this.getTableColumnsSimple(db.name, schema.name, tableName);
            
            allTables.push({
              database: db.name,
              schema: schema.name,
              table: tableName,
              columns
            });
          }
        }
      }
      
      this.logger.log(`Total tables found: ${allTables.length}`);
      return allTables;
      
    } catch (error) {
      this.logger.error('Error in simple metadata extraction', error);
      throw error;
    }
  }

  /**
   * Simple method to get columns for a single table
   */
  private async getTableColumnsSimple(database: string, schema: string, table: string): Promise<any[]> {
    try {
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
      
      const rows = await this.executeQueryWithRetry(query);
      
      return rows.map(row => ({
        name: row.NAME || row.name,
        type: row.TYPE || row.type,
        nullable: (row.NULLABLE || row.nullable) === 'YES',
        defaultValue: row.DEFAULTVALUE || row.defaultValue,
        comment: row.COMMENT || row.comment
      }));
      
    } catch (error) {
      this.logger.warn(`Could not fetch columns for ${database}.${schema}.${table}: ${error.message}`);
      return [];
    }
  }

  /**
   * Execute query with enhanced retry mechanism and exponential backoff
   */
  private async executeQueryWithRetry(query: string, maxRetries: number = 3): Promise<any[]> {
    let lastError: Error;
    const queryStartTime = Date.now();
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        this.logger.debug(`Executing query (attempt ${attempt}/${maxRetries}): ${query.substring(0, 100)}...`);
        const result = await this.executeQuery(query);
        const queryDuration = Date.now() - queryStartTime;
        this.logger.log(`Query completed in ${queryDuration}ms (attempt ${attempt}/${maxRetries})`);
        return result;
      } catch (error) {
        lastError = error;
        const queryDuration = Date.now() - queryStartTime;
        this.logger.warn(`Query attempt ${attempt} failed after ${queryDuration}ms: ${error.message}`);
        
        if (attempt < maxRetries && this.isRetryableError(error)) {
          this.retryCount++;
          // Exponential backoff: 1s, 2s, 4s, etc.
          const delay = Math.pow(2, attempt - 1) * 1000;
          this.logger.log(`Retrying in ${delay}ms... (retry #${this.retryCount})`);
          await new Promise(resolve => setTimeout(resolve, delay));
        } else {
          // Either max retries reached or non-retryable error
          break;
        }
      }
    }
    
    const totalQueryDuration = Date.now() - queryStartTime;
    this.logger.error(`Query failed after ${maxRetries} attempts (total time: ${totalQueryDuration}ms): ${query.substring(0, 200)}...`);
    throw lastError!;
  }

  /**
   * Check if an error is retryable
   */
  private isRetryableError(error: any): boolean {
    const retryableErrors = [
      'timeout',
      'connection',
      'network',
      'temporary',
      'rate limit',
      'service unavailable',
      'connection lost',
      'query timeout',
      'warehouse suspended',
      'session expired'
    ];
    
    const errorMessage = error.message?.toLowerCase() || '';
    const errorCode = error.code?.toString() || '';
    
    // Check for retryable error patterns
    const isRetryable = retryableErrors.some(keyword => 
      errorMessage.includes(keyword)
    );
    
    // Check for specific Snowflake error codes that are retryable
    const retryableErrorCodes = [
      '100072', // Query timeout
      '100073', // Connection timeout
      '100074', // Network timeout
      '100075', // Service unavailable
      '100076', // Rate limit exceeded
      '100077', // Warehouse suspended
      '100078', // Session expired
    ];
    
    const isRetryableCode = retryableErrorCodes.includes(errorCode);
    
    this.logger.debug(`Error ${errorCode}: ${errorMessage} - Retryable: ${isRetryable || isRetryableCode}`);
    
    return isRetryable || isRetryableCode;
  }

  private async executeQuery(query: string): Promise<any[]> {
    this.queryCount++;
    const startTime = Date.now();
    
    if (this.queryCount % 10 === 0) {
      this.logger.debug(`Executing query ${this.queryCount}: ${query.substring(0, 100)}...`);
    }
    
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText: query,
        complete: (err, stmt, rows) => {
          const duration = Date.now() - startTime;
          this.totalQueryDuration += duration;
          
          if (err) {
            this.logger.error(`Query ${this.queryCount} failed after ${duration}ms: ${query.substring(0, 200)}...`, err);
            this.errorCount++;
            reject(err);
          } else {
            const rowCount = rows?.length || 0;
            this.logger.log(`Query ${this.queryCount} completed in ${duration}ms, returned ${rowCount} rows`);
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

  /**
   * Log metrics summary
   */
  private logMetrics(context: string, additionalData?: any): void {
    const metrics = {
      context,
      queryCount: this.queryCount,
      totalQueryDuration: `${this.totalQueryDuration}ms`,
      averageQueryDuration: this.queryCount > 0 ? `${Math.round(this.totalQueryDuration / this.queryCount)}ms` : '0ms',
      retryCount: this.retryCount,
      errorCount: this.errorCount,
      timestamp: new Date().toISOString(),
      ...additionalData
    };
    
    this.logger.log(`[METRICS] ${JSON.stringify(metrics)}`);
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

  /**
   * Optimized filtering approach that doesn't require individual queries
   */
  private async filterTablesByLastSyncTimeOptimized(tables: SnowflakeTable[], lastSyncTime: Date): Promise<SnowflakeTable[]> {
    if (!lastSyncTime) return tables;
    
    this.logger.log(`Filtering ${tables.length} tables based on lastSyncTime: ${lastSyncTime}`);
    
    // Group tables by database for efficient querying
    const tablesByDatabase = new Map<string, Array<{schema: string, table: string}>>();
    
    for (const table of tables) {
      if (!tablesByDatabase.has(table.database)) {
        tablesByDatabase.set(table.database, []);
      }
      tablesByDatabase.get(table.database)!.push({
        schema: table.schema,
        table: table.table
      });
    }
    
    const tableDateMap = new Map<string, Date>();
    
    // Query each database separately for table creation dates (single query per database)
    for (const [database, tableList] of tablesByDatabase) {
      try {
        if (tableList.length === 0) continue;
        
        // Build a single query for all tables in this database
        const tableConditions = tableList.map(t => `(TABLE_SCHEMA = '${t.schema}' AND TABLE_NAME = '${t.table}')`).join(' OR ');
        
        const query = `
          SELECT 
            TABLE_SCHEMA as schema,
            TABLE_NAME as table,
            CREATED as created_on
          FROM "${database}".INFORMATION_SCHEMA.TABLES
          WHERE ${tableConditions}
        `;
        
        const tableDates = await this.executeQueryWithRetry(query);
        
        for (const row of tableDates) {
          const key = `${database}.${row.SCHEMA || row.schema}.${row.TABLE || row.table}`;
          const createdDate = row.CREATED_ON || row.created_on;
          if (createdDate) {
            tableDateMap.set(key, new Date(createdDate));
          }
        }
      } catch (error) {
        this.logger.warn(`Failed to get creation dates for database ${database}`, error);
        // Continue with other databases
      }
    }
    
    const filteredTables = tables.filter(table => {
      const key = `${table.database}.${table.schema}.${table.table}`;
      const createdDate = tableDateMap.get(key);
      
      if (!createdDate) {
        // If we can't find creation date, include the table (safer approach)
        this.logger.debug(`No creation date found for ${key}, including table`);
        return true;
      }
      
      const shouldInclude = createdDate > lastSyncTime;
      if (!shouldInclude) {
        this.logger.debug(`Excluding ${key} - created ${createdDate} (before ${lastSyncTime})`);
      }
      return shouldInclude;
    });
    
    this.logger.log(`Filtered from ${tables.length} to ${filteredTables.length} tables`);
    return filteredTables;
  }
}