import { Document } from 'mongoose';
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

export type SyncDocument = SyncStats & Document;

@Schema({
  collection: 'sync_stats',
  timestamps: true
})
export class SyncStats {
  @Prop({ required: true })
  syncStartTime: Date;

  @Prop({ required: true })
  syncEndTime: Date;

  @Prop({ type: Boolean, required: true })
  success: boolean;

  @Prop({ type: Number, default: 0 })
  totalTables: number;

  @Prop({ type: Number, default: 0 })
  newTables: number;

  @Prop({ type: Number, default: 0 })
  updatedTables: number;

  @Prop({ type: Number, default: 0 })
  skippedTables: number;

  @Prop({ type: Number, required: true })
  processingTimeMs: number;

  @Prop([{ type: String }])
  errors: string[];

  @Prop({ type: String })
  message: string;
}

export const SyncStatsSchema = SchemaFactory.createForClass(SyncStats);

// Index for efficient querying
SyncStatsSchema.index({ syncEndTime: -1 });
SyncStatsSchema.index({ success: 1, syncEndTime: -1 });