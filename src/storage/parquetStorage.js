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
  website: { type: 'UTF8', optional: true },
  domain: { type: 'UTF8', optional: true },
  pattern: { type: 'UTF8' },
  email: { type: 'UTF8' },
  source: { type: 'UTF8' },
  score: { type: 'INT32' },
  appName: { type: 'UTF8', optional: true },
  deliverable: { type: 'BOOLEAN', optional: true },
  isDeliverable: { type: 'BOOLEAN' },
  isRisky: { type: 'BOOLEAN' },
  isUndeliverable: { type: 'BOOLEAN' },
  isB2C: { type: 'BOOLEAN' },
  createdAt: { type: 'INT64' }
});

/**
 * Parquet Schema for Webhook Triggers (When submitted to Verify V6)
 */
export const webhookTriggerSchema = new parquet.ParquetSchema({
  trackingId: { type: 'UTF8' },
  personId: { type: 'UTF8' },
  firstName: { type: 'UTF8' },
  lastName: { type: 'UTF8' },
  companyId: { type: 'UTF8', optional: true },
  companyName: { type: 'UTF8', optional: true },
  website: { type: 'UTF8', optional: true },
  domain: { type: 'UTF8', optional: true },
  pattern: { type: 'UTF8' },
  email: { type: 'UTF8' },
  status: { type: 'UTF8' }, // 'triggered_waiting_callback'
  createdAt: { type: 'INT64' }
});

/**
 * Parquet Schema for Invalid / Bounced Emails
 */
export const invalidEmailSchema = new parquet.ParquetSchema({
  trackingId: { type: 'UTF8' },
  personId: { type: 'UTF8' },
  firstName: { type: 'UTF8' },
  lastName: { type: 'UTF8' },
  companyId: { type: 'UTF8', optional: true },
  companyName: { type: 'UTF8', optional: true },
  website: { type: 'UTF8', optional: true },
  domain: { type: 'UTF8', optional: true },
  email: { type: 'UTF8', optional: true },
  pattern: { type: 'UTF8', optional: true },
  deliverable: { type: 'BOOLEAN', optional: true },
  isDeliverable: { type: 'BOOLEAN', optional: true },
  isRisky: { type: 'BOOLEAN', optional: true },
  isUndeliverable: { type: 'BOOLEAN', optional: true },
  isB2C: { type: 'BOOLEAN', optional: true },
  status: { type: 'UTF8' }, // 'undeliverable_via_webhook' | 'all_patterns_cached_invalid' | 'secondary_valid_ignored'
  score: { type: 'INT32', optional: true },
  reason: { type: 'UTF8', optional: true },
  createdAt: { type: 'INT64' }
});

/**
 * Production Thread-safe Rotating Parquet Writer
 * 
 * DESIGN FEATURES:
 * 1. Writes active data to a `.tmp` file first.
 * 2. Auto-seals & atomically renames to `.parquet` every `flushIntervalMs` OR every `maxCapacity` rows.
 * 3. Any `.parquet` file in the folder is 100% COMPLETE, SEALED, and readable at all times (even while the server is running).
 * 4. Never leaves 0-byte or unsealed `.parquet` files on disk.
 */
export class ChunkedParquetWriter {
  /**
   * @param {object} options
   * @param {string} options.outputDir          - Base directory (e.g. data/valid)
   * @param {string} options.filePrefix         - Prefix for file name (e.g. 'valid_emails')
   * @param {parquet.ParquetSchema} options.schema - Parquet schema
   * @param {number} [options.maxCapacity=1000000] - Rows per chunk before auto-sealing (Default: 1,000,000 for 150M production scale)
   * @param {number} [options.flushIntervalMs=300000] - Auto-seal interval if idle (Default: 5m)
   * @param {string} [options.serverId='worker-1'] - Server ID for collision-free names
   */
  constructor(options = {}) {
    this.outputDir = options.outputDir;
    this.filePrefix = options.filePrefix;
    this.schema = options.schema;
    this.maxCapacity = options.maxCapacity || 1000000;
    this.flushIntervalMs = options.flushIntervalMs || 300000;
    this.serverId = options.serverId || `srv_${process.pid}`;

    this.currentWriter = null;
    this.currentTmpPath = null;
    this.currentFinalPath = null;
    this.currentFileIndex = 1;
    this.currentCount = 0;
    this.isRotating = false;
    this._writeLock = Promise.resolve();

    if (!fs.existsSync(this.outputDir)) {
      fs.mkdirSync(this.outputDir, { recursive: true });
    }

    // Only seal on timer if idle for a prolonged period AND exceeds minCapacity
    // During active processing, let the file fill up to maxCapacity (1,000,000 records)
    this.minCapacity = options.minCapacity || 100000;
    this.flushTimer = setInterval(() => {
      if (this.currentCount >= this.minCapacity && !this.isRotating) {
        this.sealCurrentFile().catch(() => {});
      }
    }, this.flushIntervalMs);
    this.flushTimer.unref(); // Won't keep process alive on exit
  }

  async _initNewFile() {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const baseName = `${this.filePrefix}_${this.serverId}_part_${this.currentFileIndex}_${timestamp}`;
    this.currentTmpPath = path.join(this.outputDir, `.${baseName}.tmp`);
    this.currentFinalPath = path.join(this.outputDir, `${baseName}.parquet`);
    this.currentWriter = await parquet.ParquetWriter.openFile(this.schema, this.currentTmpPath);
    this.currentCount = 0;
  }

  /**
   * Seals current file and atomically renames it to .parquet
   */
  async sealCurrentFile() {
    if (this.isRotating || !this.currentWriter || this.currentCount === 0) {
      if (this.currentWriter && this.currentCount === 0) {
        // Discard 0-row empty files cleanly
        await this.currentWriter.close();
        if (fs.existsSync(this.currentTmpPath)) {
          fs.unlinkSync(this.currentTmpPath);
        }
        this.currentWriter = null;
      }
      return;
    }

    this.isRotating = true;
    try {
      await this.currentWriter.close();
      this.currentWriter = null;

      // Atomically expose completed Parquet file to external tools/SCP
      if (fs.existsSync(this.currentTmpPath)) {
        fs.renameSync(this.currentTmpPath, this.currentFinalPath);
      }
      this.currentFileIndex++;
    } finally {
      this.isRotating = false;
    }
  }

  /**
   * Append a single row to the current Parquet file (thread-safe async serialized).
   *
   * @param {object} row
   */
  async append(row) {
    const op = async () => {
      while (this.isRotating) {
        await new Promise(r => setTimeout(r, 10));
      }

      if (!this.currentWriter) {
        await this._initNewFile();
      }

      await this.currentWriter.appendRow(row);
      this.currentCount++;

      if (this.currentCount >= this.maxCapacity) {
        await this.sealCurrentFile();
      }
    };

    this._writeLock = this._writeLock.then(op, op);
    return this._writeLock;
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
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
    }
    await this._writeLock;
    await this.sealCurrentFile();
  }

  getStats() {
    return {
      outputDir: this.outputDir,
      currentFilePath: this.currentFinalPath,
      rowsInCurrentChunk: this.currentCount,
      chunksCompleted: this.currentFileIndex - 1
    };
  }
}
