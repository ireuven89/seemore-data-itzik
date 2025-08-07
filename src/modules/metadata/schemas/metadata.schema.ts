import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type MetadataDocument = Metadata & Document;

@Schema({ 
  collection: 'metadata',
  timestamps: true
})
export class Metadata {
  @Prop({ required: true })
  database: string;

  @Prop({ required: true })
  schema: string;

  @Prop({ required: true })
  table: string;

  @Prop([
    {
      name: { type: String, required: true },
      type: { type: String, required: true },
      nullable: { type: Boolean, required: true },
      defaultValue: { type: String, default: null },
      comment: { type: String, default: null }
    }
  ])
  columns: Array<{
    name: string;
    type: string;
    nullable: boolean;
    defaultValue?: string;
    comment?: string;
  }>;

  @Prop({ type: Date, default: Date.now })
  lastSynced: Date;

  @Prop({ type: String })
  checksum: string;
}

export const MetadataSchema = SchemaFactory.createForClass(Metadata);
MetadataSchema.index({ database: 1, schema: 1, table: 1 }, { unique: true });