import { Injectable, Logger } from '@nestjs/common';
import { SnowflakeService } from './services/snowflake.service';
import { MongodbService } from './services/mongodb.service';
import { SyncResponseDto } from './dto/sync-response.dto';

@Injectable()
export class MetadataService {
  private readonly logger = new Logger(MetadataService.name);

  constructor(
    private readonly snowflakeService: SnowflakeService,
    private readonly mongodbService: MongodbService,
  ) {}

  async syncMetadata(): Promise<SyncResponseDto> {
    const startTime = new Date();
    const errors: string[] = [];

    try {
      this.logger.log('Starting metadata sync process');
      
      // Get last sync time from MongoDB
      const stats = await this.mongodbService.getSyncStats();
      const lastSyncTime: Date | undefined = stats.lastSyncTime ?? undefined;
      this.logger.log(`Last sync time: ${lastSyncTime}`);
      
      // Fetch only changed/new tables since last sync
      const snowflakeStartTime = Date.now();
      const tables = await this.snowflakeService.getAllTables(lastSyncTime);
      const snowflakeDuration = Date.now() - snowflakeStartTime;
      this.logger.log(`Fetched ${tables.length} tables from Snowflake in ${snowflakeDuration}ms`);
      
      // Process tables in MongoDB
      const mongoStartTime = Date.now();
      const upsertResults = await this.mongodbService.upsertMetadata(tables);
      const mongoDuration = Date.now() - mongoStartTime;
      
      const endTime = new Date();
      const processingTimeMs = endTime.getTime() - startTime.getTime();
      
      // Log comprehensive metrics
      this.logComprehensiveMetrics({
        totalTables: tables.length,
        newTables: upsertResults.newTables,
        updatedTables: upsertResults.updatedTables,
        skippedTables: upsertResults.skippedTables,
        snowflakeDuration,
        mongoDuration,
        totalProcessingTime: processingTimeMs,
        lastSyncTime: lastSyncTime?.toISOString()
      });
      
      this.logger.log(`Sync completed in ${processingTimeMs}ms`);
      this.logger.log(`Stats: ${upsertResults.newTables} new, ${upsertResults.updatedTables} updated, ${upsertResults.skippedTables} skipped`);
      
      const syncResponse: SyncResponseDto = {
        success: true,
        message: 'Metadata sync completed successfully',
        stats: {
          totalTables: tables.length,
          ...upsertResults,
          processingTimeMs
        }
      };

      // Save sync stats to MongoDB
      await this.mongodbService.saveSyncStats(syncResponse, startTime, endTime);
      
      return syncResponse;
    } catch (error) {
      const endTime = new Date();
      const processingTimeMs = endTime.getTime() - startTime.getTime();
      const errorMessage = error.message || 'Unknown error occurred';
      this.logger.error('Metadata sync failed', error);
      errors.push(errorMessage);
      
      // Log error metrics
      this.logComprehensiveMetrics({
        totalTables: 0,
        newTables: 0,
        updatedTables: 0,
        skippedTables: 0,
        snowflakeDuration: 0,
        mongoDuration: 0,
        totalProcessingTime: processingTimeMs,
        error: errorMessage
      });
      
      const syncResponse: SyncResponseDto = {
        success: false,
        message: 'Metadata sync failed',
        stats: {
          totalTables: 0,
          newTables: 0,
          updatedTables: 0,
          skippedTables: 0,
          processingTimeMs
        },
        errors
      };

      // Save sync stats even for failed syncs
      await this.mongodbService.saveSyncStats(syncResponse, startTime, endTime);
      
      return syncResponse;
    }
  }

  async getSyncHistory(limit: number = 10): Promise<any[]> {
    return await this.mongodbService.getSyncHistory(limit);
  }

  async getSyncStatsById(id: string): Promise<any> {
    return await this.mongodbService.getSyncStatsById(id);
  }

  /**
   * Log comprehensive metrics for the sync operation
   */
  private logComprehensiveMetrics(metrics: {
    totalTables: number;
    newTables: number;
    updatedTables: number;
    skippedTables: number;
    snowflakeDuration: number;
    mongoDuration: number;
    totalProcessingTime: number;
    lastSyncTime?: string;
    error?: string;
  }): void {
    const comprehensiveMetrics = {
      context: 'Metadata Sync Complete',
      tables: {
        total: metrics.totalTables,
        new: metrics.newTables,
        updated: metrics.updatedTables,
        skipped: metrics.skippedTables
      },
      performance: {
        snowflakeDuration: `${metrics.snowflakeDuration}ms`,
        mongoDuration: `${metrics.mongoDuration}ms`,
        totalProcessingTime: `${metrics.totalProcessingTime}ms`,
        efficiency: metrics.totalProcessingTime > 0 ? `${Math.round((metrics.snowflakeDuration + metrics.mongoDuration) / metrics.totalProcessingTime * 100)}%` : '0%'
      },
      timing: {
        lastSyncTime: metrics.lastSyncTime || 'N/A',
        timestamp: new Date().toISOString()
      },
      ...(metrics.error && { error: metrics.error })
    };
    
    this.logger.log(`[COMPREHENSIVE_METRICS] ${JSON.stringify(comprehensiveMetrics)}`);
  }
}