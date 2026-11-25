import parquet from 'parquetjs-lite';
import fs from 'fs';
import path from 'path';

/**
 * Parquet Schema for Valid Emails
 */
export const validEmailSchema = new parquet.ParquetSchema({
  trackingId: { type: 'UTF8' },
  personId: { type: 'UTF8' },
  firstName: { type: 'UTF8' },
  lastName: { type: 'UTF8' },
  companyId: { type: 'UTF8', optional: true },
  companyName: { type: 'UTF8', optional: true },
  website: { type: 'UTF8' },
  domain: { type: 'UTF8' },
  pattern: { type: 'UTF8' },
  email: { type: 'UTF8' },
  source: { type: 'UTF8' },
  score: { type: 'INT32' },
  appName: { type: 'UTF8', optional: true },
  isDeliverable: { type: 'BOOLEAN' },
  isRisky: { type: 'BOOLEAN' },
  isUndeliverable: { type: 'BOOLEAN' },
  isB2C: { type: 'BOOLEAN' },
  createdAt: { type: 'INT64' }
});

/**
 * Parquet Schema for Invalid Emails
 */
export const invalidEmailSchema = new parquet.ParquetSchema({
  trackingId: { type: 'UTF8' },
  personId: { type: 'UTF8' },
  firstName: { type: 'UTF8' },
  lastName: { type: 'UTF8' },
  companyId: { type: 'UTF8', optional: true },
  companyName: { type: 'UTF8', optional: true },
  website: { type: 'UTF8' },
  domain: { type: 'UTF8' },
  email: { type: 'UTF8', optional: true },
  pattern: { type: 'UTF8', optional: true },
  status: { type: 'UTF8' }, // 'all_patterns_cached_invalid' | 'n2b_validated_invalid'
  score: { type: 'INT32', optional: true },
  inCache: { type: 'BOOLEAN', optional: true },
  reason: { type: 'UTF8', optional: true },
  createdAt: { type: 'INT64' }
});

/**
 * Thread-safe, rotating chunked Parquet Writer
 * Automatically creates new files when row count reaches maxCapacity.
 */
export class ChunkedParquetWriter {
  /**
   * @param {object} options
   * @param {string} options.outputDir   - Base directory for files (e.g. data/valid)
   * @param {string} options.filePrefix  - Prefix for file name (e.g. 'valid_emails')
   * @param {parquet.ParquetSchema} options.schema - Parquet schema
   * @param {number} [options.minCapacity=10000]   - Minimum buffer rows before flushing
   * @param {number} [options.maxCapacity=100000]  - Max rows per parquet file before rotating
   * @param {string} [options.serverId='worker-1']- Server ID to prevent file naming collisions
   */
  constructor(options = {}) {
    this.outputDir = options.outputDir;
    this.filePrefix = options.filePrefix;
    this.schema = options.schema;
    this.minCapacity = options.minCapacity || 10000;
    this.maxCapacity = options.maxCapacity || 100000;
    this.serverId = options.serverId || `srv_${process.pid}`;

    this.currentWriter = null;
    this.currentFilePath = null;
    this.currentFileIndex = 1;
    this.currentCount = 0;
    this.isRotating = false;

    if (!fs.existsSync(this.outputDir)) {
      fs.mkdirSync(this.outputDir, { recursive: true });
    }
  }

  async _initNewFile() {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const fileName = `${this.filePrefix}_${this.serverId}_part_${this.currentFileIndex}_${timestamp}.parquet`;
    this.currentFilePath = path.join(this.outputDir, fileName);
    this.currentWriter = await parquet.ParquetWriter.openFile(this.schema, this.currentFilePath);
    this.currentCount = 0;
  }

  /**
   * Append a single row to the current Parquet file. Rotates if maxCapacity reached.
   *
   * @param {object} row
   */
  async append(row) {
    while (this.isRotating) {
      await new Promise(r => setTimeout(r, 10));
    }

    if (!this.currentWriter) {
      await this._initNewFile();
    }

    await this.currentWriter.appendRow(row);
    this.currentCount++;

    if (this.currentCount >= this.maxCapacity) {
      this.isRotating = true;
      try {
        await this.currentWriter.close();
        this.currentWriter = null;
        this.currentFileIndex++;
        await this._initNewFile();
      } finally {
        this.isRotating = false;
      }
    }
  }

  /**
   * Append a batch of rows efficiently.
   *
   * @param {Array<object>} rows
   */
  async appendBatch(rows = []) {
    for (const row of rows) {
      await this.append(row);
    }
  }

  /**
   * Closes and flushes current open Parquet file.
   */
  async close() {
    if (this.currentWriter) {
      await this.currentWriter.close();
      this.currentWriter = null;
    }
  }

  /**
   * Returns stats about current written rows and active file.
   */
  getStats() {
    return {
      currentCount: this.currentCount,
      currentFileIndex: this.currentFileIndex,
      currentFilePath: this.currentFilePath
    };
  }
}
