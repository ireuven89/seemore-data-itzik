import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { MetadataController } from './metadata.controller';
import { MetadataService } from './metadata.service';
import { SnowflakeService } from './services/snowflake.service';
import { MongodbService } from './services/mongodb.service';
import { Metadata, MetadataSchema } from './schemas/metadata.schema';
import { SyncStats, SyncStatsSchema } from './schemas/sync.schema';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: Metadata.name, schema: MetadataSchema },
      { name: SyncStats.name, schema: SyncStatsSchema }
    ])
  ],
  controllers: [MetadataController],
  providers: [MetadataService, SnowflakeService, MongodbService],
  exports: [MetadataService],
})
export class MetadataModule {}