import { Controller, Post, HttpStatus, HttpCode, Logger } from '@nestjs/common';
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
}