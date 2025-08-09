import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Metadata, MetadataDocument } from '../schemas/metadata.schema';
import { SyncStats, SyncDocument } from '../schemas/sync.schema';
import { SnowflakeTable } from './snowflake.service';
import { SyncResponseDto } from '../dto/sync-response.dto';
import * as crypto from 'crypto';

@Injectable()
export class MongodbService {
  private readonly logger = new Logger(MongodbService.name);
  private operationCount = 0;
  private totalOperationDuration = 0;
  private errorCount = 0;

  constructor(
    @InjectModel(Metadata.name) private metadataModel: Model<MetadataDocument>,
    @InjectModel(SyncStats.name) private syncStatsModel: Model<SyncDocument>,
  ) {}

  async upsertMetadata(tables: SnowflakeTable[]): Promise<{
    newTables: number;
    updatedTables: number;
    skippedTables: number;
  }> {
    const startTime = Date.now();
    this.operationCount++;
    
    if (tables.length === 0) {
      this.logMetrics('MongoDB Upsert - Empty Tables');
      return { newTables: 0, updatedTables: 0, skippedTables: 0 };
    }

    this.logger.log(`Processing ${tables.length} tables with bulk operations`);

    try {
      // Prepare bulk operations
      const bulkOps: any[] = [];
      const checksums = new Map<string, string>();

      // Generate checksums for all tables
      for (const table of tables) {
        const checksum = this.generateChecksum(table);
        const key = `${table.database}.${table.schema}.${table.table}`;
        checksums.set(key, checksum);
      }

      // Get existing records for comparison
      const existingRecords = await this.metadataModel.find({
        $or: tables.map(table => ({
          database: table.database,
          schema: table.schema,
          table: table.table
        }))
      });

      const existingMap = new Map<string, any>();
      existingRecords.forEach(record => {
        const key = `${record.database}.${record.schema}.${record.table}`;
        existingMap.set(key, record);
      });

      // Prepare bulk operations
      for (const table of tables) {
        const key = `${table.database}.${table.schema}.${table.table}`;
        const checksum = checksums.get(key)!;
        const existingRecord = existingMap.get(key);

        if (!existingRecord) {
          // New record - insert
          bulkOps.push({
            insertOne: {
              document: {
                ...table,
                checksum,
                lastSynced: new Date()
              }
            }
          });
        } else if (existingRecord.checksum !== checksum) {
          // Updated record - update
          bulkOps.push({
            updateOne: {
              filter: {
                database: table.database,
                schema: table.schema,
                table: table.table
              },
              update: {
                $set: {
                  columns: table.columns,
                  checksum,
                  lastSynced: new Date()
                }
              }
            }
          });
        }
        // Skip unchanged records (no operation needed)
      }

      // Execute bulk operations
      let newTables = 0;
      let updatedTables = 0;
      let skippedTables = 0;

      if (bulkOps.length > 0) {
        this.logger.log(`Executing ${bulkOps.length} bulk operations`);
        const result = await this.metadataModel.bulkWrite(bulkOps, { ordered: false });
        
        newTables = result.insertedCount || 0;
        updatedTables = result.modifiedCount || 0;
        skippedTables = tables.length - newTables - updatedTables;
        
        this.logger.log(`Bulk operations completed: ${newTables} inserted, ${updatedTables} updated, ${skippedTables} skipped`);
      } else {
        skippedTables = tables.length;
        this.logger.log(`All ${tables.length} tables were unchanged - no operations needed`);
      }

      const duration = Date.now() - startTime;
      this.totalOperationDuration += duration;
      
      this.logMetrics('MongoDB Upsert', {
        tablesProcessed: tables.length,
        newTables,
        updatedTables,
        skippedTables,
        bulkOpsCount: bulkOps.length,
        duration: `${duration}ms`
      });

      return { newTables, updatedTables, skippedTables };
      
    } catch (error) {
      const duration = Date.now() - startTime;
      this.errorCount++;
      this.logger.error(`MongoDB upsert failed after ${duration}ms`, error);
      throw error;
    }
  }

  /**
   * Log metrics summary
   */
  private logMetrics(context: string, additionalData?: any): void {
    const metrics = {
      context,
      operationCount: this.operationCount,
      totalOperationDuration: `${this.totalOperationDuration}ms`,
      averageOperationDuration: this.operationCount > 0 ? `${Math.round(this.totalOperationDuration / this.operationCount)}ms` : '0ms',
      errorCount: this.errorCount,
      timestamp: new Date().toISOString(),
      ...additionalData
    };
    
    this.logger.log(`[METRICS] ${JSON.stringify(metrics)}`);
  }


  private generateChecksum(table: SnowflakeTable): string {
    const data = JSON.stringify({
      columns: table.columns.sort((a, b) => a.name.localeCompare(b.name))
    });
    return crypto.createHash('sha256').update(data).digest('hex');
  }

  async getSyncStats(): Promise<{
    lastSyncTime: Date | null;
  }> {
    const lastSync = await this.syncStatsModel.findOne({"success": true})
      .sort({ syncEndTime: -1 })
      .select('syncEndTime');

    return {
      lastSyncTime: lastSync?.syncEndTime || null
    };
  }

  async saveSyncStats(syncResponse: SyncResponseDto, startTime: Date, endTime: Date): Promise<void> {
    try {
      await this.syncStatsModel.create({
        syncStartTime: startTime,
        syncEndTime: endTime,
        success: syncResponse.success,
        totalTables: syncResponse.stats.totalTables,
        newTables: syncResponse.stats.newTables,
        updatedTables: syncResponse.stats.updatedTables,
        skippedTables: syncResponse.stats.skippedTables,
        processingTimeMs: syncResponse.stats.processingTimeMs,
        errors: syncResponse.errors || [],
        message: syncResponse.message
      });
      this.logger.log('Sync stats saved to MongoDB');
    } catch (error) {
      this.logger.error('Failed to save sync stats', error);
    }
  }

  async getSyncHistory(limit: number = 10): Promise<SyncStats[]> {
    try {
      return await this.syncStatsModel
        .find()
        .sort({ syncEndTime: -1 })
        .limit(limit)
        .exec();
    } catch (error) {
      this.logger.error('Failed to retrieve sync history', error);
      return [];
    }
  }

  async getSyncStatsById(id: string): Promise<SyncStats | null> {
    try {
      return await this.syncStatsModel.findById(id).exec();
    } catch (error) {
      this.logger.error('Failed to retrieve sync stats by ID', error);
      return null;
    }
  }
}