import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { MetadataModule } from './modules/metadata/metadata.module';
import { DatabaseConfig } from './config/database.config';

@Module({
  imports: [
    MongooseModule.forRoot(DatabaseConfig.getMongoUri()),
    MetadataModule,
  ],
})
export class AppModule {}
