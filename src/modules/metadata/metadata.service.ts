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
    const startTime = Date.now();
    const errors: string[] = [];

    try {
      this.logger.log('Starting metadata sync process');
      // Get last sync time from MongoDB
      const stats = await this.mongodbService.getMetadataStats();
      const lastSyncTime: Date | undefined = stats.lastSyncTime ?? undefined;
      this.logger.log(`Last sync time: ${lastSyncTime}`);
      // Fetch only changed/new tables since last sync
      const tables = await this.snowflakeService.getAllTables(lastSyncTime);
      this.logger.log(`Fetched ${tables.length} tables from Snowflake`);
      const upsertResults = await this.mongodbService.upsertMetadata(tables);
      const processingTimeMs = Date.now() - startTime;
      this.logger.log(`Sync completed in ${processingTimeMs}ms`);
      this.logger.log(`Stats: ${upsertResults.newTables} new, ${upsertResults.updatedTables} updated, ${upsertResults.skippedTables} skipped`);
      return {
        success: true,
        message: 'Metadata sync completed successfully',
        stats: {
          totalTables: tables.length,
          ...upsertResults,
          processingTimeMs
        }
      };
    } catch (error) {
      const processingTimeMs = Date.now() - startTime;
      const errorMessage = error.message || 'Unknown error occurred';
      this.logger.error('Metadata sync failed', error);
      errors.push(errorMessage);
      return {
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
    }
  }
}