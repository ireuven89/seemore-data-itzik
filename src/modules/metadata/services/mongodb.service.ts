import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Metadata, MetadataDocument } from '../schemas/metadata.schema';
import { SnowflakeTable } from './snowflake.service';
import * as crypto from 'crypto';

@Injectable()
export class MongodbService {
  private readonly logger = new Logger(MongodbService.name);

  constructor(
    @InjectModel(Metadata.name) private metadataModel: Model<MetadataDocument>,
  ) {}

  async upsertMetadata(tables: SnowflakeTable[]): Promise<{
    newTables: number;
    updatedTables: number;
    skippedTables: number;
  }> {
    let newTables = 0;
    let updatedTables = 0;
    let skippedTables = 0;

    for (const table of tables) {
      try {
        const checksum = this.generateChecksum(table);
        const filter = {
          database: table.database,
          schema: table.schema,
          table: table.table
        };

        const existingRecord = await this.metadataModel.findOne(filter);

        if (!existingRecord) {
          await this.metadataModel.create({
            ...table,
            checksum,
            lastSynced: new Date()
          });
          newTables++;
          this.logger.debug(`Created new record for ${table.database}.${table.schema}.${table.table}`);
        } else if (existingRecord.checksum !== checksum) {
          await this.metadataModel.updateOne(filter, {
            columns: table.columns,
            checksum,
            lastSynced: new Date()
          });
          updatedTables++;
          this.logger.debug(`Updated record for ${table.database}.${table.schema}.${table.table}`);
        } else {
          skippedTables++;
          this.logger.debug(`Skipped unchanged record for ${table.database}.${table.schema}.${table.table}`);
        }
      } catch (error) {
        this.logger.error(`Error processing table ${table.database}.${table.schema}.${table.table}`, error);
      }
    }

    return { newTables, updatedTables, skippedTables };
  }

  private generateChecksum(table: SnowflakeTable): string {
    const data = JSON.stringify({
      columns: table.columns.sort((a, b) => a.name.localeCompare(b.name))
    });
    return crypto.createHash('sha256').update(data).digest('hex');
  }

  /**
   * can be replaced with a different collection - of sync metadata
   */
  async getMetadataStats(): Promise<{
    totalTables: number;
    lastSyncTime: Date | null;
  }> {
    const totalTables = await this.metadataModel.countDocuments();
    const lastSync = await this.metadataModel.findOne()
      .sort({ lastSynced: -1 })
      .select('lastSynced');

    return {
      totalTables,
      lastSyncTime: lastSync?.lastSynced || null
    };
  }
}