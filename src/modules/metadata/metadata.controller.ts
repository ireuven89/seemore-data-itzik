import { Controller, Post, Get, Param, Query, HttpStatus, HttpCode, Logger } from '@nestjs/common';
import { MetadataService } from './metadata.service';
import { SyncResponseDto } from './dto/sync-response.dto';

@Controller('metadata')
export class MetadataController {
  private readonly logger = new Logger(MetadataController.name);

  constructor(private readonly metadataService: MetadataService) {}

  @Post('sync')
  @HttpCode(HttpStatus.OK)
  async syncMetadata(): Promise<SyncResponseDto> {
    this.logger.log('Metadata sync endpoint called');
    return await this.metadataService.syncMetadata();
  }

  @Get('sync/history')
  @HttpCode(HttpStatus.OK)
  async getSyncHistory(@Query('limit') limit?: string): Promise<any[]> {
    this.logger.log('Sync history endpoint called');
    const limitNumber = limit ? parseInt(limit, 10) : 10;
    return await this.metadataService.getSyncHistory(limitNumber);
  }

  @Get('sync/stats/:id')
  @HttpCode(HttpStatus.OK)
  async getSyncStatsById(@Param('id') id: string): Promise<any> {
    this.logger.log(`Sync stats endpoint called for ID: ${id}`);
    return await this.metadataService.getSyncStatsById(id);
  }
}