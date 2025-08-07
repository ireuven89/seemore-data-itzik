export class SyncResponseDto {
  success: boolean;
  message: string;
  stats: {
    totalTables: number;
    newTables: number;
    updatedTables: number;
    skippedTables: number;
    processingTimeMs: number;
  };
  errors?: string[];
}