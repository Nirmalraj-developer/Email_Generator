import parquet from 'parquetjs-lite';
import fs from 'fs';
import path from 'path';

export const validSchema = new parquet.ParquetSchema({
  trackingId: { type: 'UTF8' },
  personId: { type: 'UTF8' },
  companyId: { type: 'UTF8', optional: true },
  pattern: { type: 'UTF8' },
  email: { type: 'UTF8' },
  score: { type: 'INT32' },
  deliverable: { type: 'BOOLEAN' },
  isRisky: { type: 'BOOLEAN' },
  isB2C: { type: 'BOOLEAN' },
  createdAt: { type: 'INT64' }
});

export const webhookTriggerSchema = new parquet.ParquetSchema({
  trackingId: { type: 'UTF8' },
  personId: { type: 'UTF8' },
  companyId: { type: 'UTF8', optional: true },
  pattern: { type: 'UTF8' },
  email: { type: 'UTF8' },
  patternIndex: { type: 'INT32' },
  status: { type: 'UTF8' },
  createdAt: { type: 'INT64' }
});

export const invalidSchema = new parquet.ParquetSchema({
  trackingId: { type: 'UTF8' },
  personId: { type: 'UTF8' },
  companyId: { type: 'UTF8', optional: true },
  pattern: { type: 'UTF8' },
  email: { type: 'UTF8' },
  score: { type: 'INT32' },
  deliverable: { type: 'BOOLEAN' },
  reason: { type: 'UTF8', optional: true },
  createdAt: { type: 'INT64' }
});

/**
 * Atomic, serialized Chunked Parquet Writer (.tmp -> .parquet on 1M rows or safe stop)
 */
export class ChunkedParquetWriter {
  constructor({ outputDir, filePrefix, schema, maxCapacity = 1000000, serverId = 'v6' }) {
    this.outputDir = outputDir;
    this.filePrefix = filePrefix;
    this.schema = schema;
    this.maxCapacity = maxCapacity;
    this.serverId = serverId;

    this.writer = null;
    this.tmpPath = null;
    this.finalPath = null;
    this.partIndex = 1;
    this.count = 0;
    this.totalWritten = 0;
    this._lock = Promise.resolve();

    if (!fs.existsSync(this.outputDir)) {
      fs.mkdirSync(this.outputDir, { recursive: true });
    }
  }

  async _open() {
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const name = `${this.filePrefix}_${this.serverId}_part_${this.partIndex}_${ts}`;
    this.tmpPath = path.join(this.outputDir, `.${name}.tmp`);
    this.finalPath = path.join(this.outputDir, `${name}.parquet`);
    this.writer = await parquet.ParquetWriter.openFile(this.schema, this.tmpPath);
    this.count = 0;
  }

  async seal() {
    if (!this.writer) return;
    const w = this.writer;
    const tmp = this.tmpPath;
    const fin = this.finalPath;
    const rows = this.count;
    this.writer = null;
    this.count = 0;

    await w.close();
    if (rows === 0) {
      if (tmp && fs.existsSync(tmp)) fs.unlinkSync(tmp);
      return;
    }
    if (tmp && fs.existsSync(tmp)) {
      fs.renameSync(tmp, fin);
    }
    this.partIndex++;
  }

  append(row) {
    const op = async () => {
      if (!this.writer) await this._open();
      await this.writer.appendRow(row);
      this.count++;
      this.totalWritten++;
      if (this.count >= this.maxCapacity) {
        await this.seal();
      }
    };
    this._lock = this._lock.then(op, op);
    return this._lock;
  }

  async close() {
    await this._lock;
    await this.seal();
  }
}
