import fs from 'fs';
import path from 'path';

/**
 * Chunked Streaming JSONL Writer (1 JSON record per line).
 * - Rotates cleanly at `maxCapacity` (default: 1,000,000 records per file).
 * - Crash-safe: every flushed line is immediately valid JSON with no footer required.
 */
export class ChunkedJsonWriter {
  constructor({ outputDir, filePrefix, maxCapacity = 1000000, serverId = 'v6' }) {
    this.outputDir = outputDir;
    this.filePrefix = filePrefix;
    this.maxCapacity = maxCapacity;
    this.serverId = serverId;

    this.stream = null;
    this.currentPath = null;
    this.partIndex = 1;
    this.count = 0;
    this.totalWritten = 0;

    if (!fs.existsSync(this.outputDir)) {
      fs.mkdirSync(this.outputDir, { recursive: true });
    }
  }

  _open() {
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const name = `${this.filePrefix}_${this.serverId}_part_${this.partIndex}_${ts}.jsonl`;
    this.currentPath = path.join(this.outputDir, name);
    this.stream = fs.createWriteStream(this.currentPath, { flags: 'a', encoding: 'utf8' });
    this.count = 0;
  }

  async _rotate() {
    if (!this.stream) return;
    const s = this.stream;
    this.stream = null;
    this.count = 0;
    this.partIndex++;
    await new Promise(resolve => s.end(resolve));
  }

  async append(row) {
    if (!this.stream) {
      this._open();
    }
    const line = JSON.stringify(row) + '\n';
    const ok = this.stream.write(line);
    this.count++;
    this.totalWritten++;

    if (!ok) {
      await new Promise(resolve => this.stream.once('drain', resolve));
    }
    if (this.count >= this.maxCapacity) {
      await this._rotate();
    }
  }

  async close() {
    if (!this.stream) return;
    const s = this.stream;
    this.stream = null;
    await new Promise(resolve => s.end(resolve));
  }
}
