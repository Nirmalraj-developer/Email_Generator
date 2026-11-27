import fs from 'fs';
import path from 'path';
import { config } from './config.js';

export class ErrorLogger {
  constructor(filePath = config.errorLogFile) {
    this.filePath = filePath;
    const dir = path.dirname(this.filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    this.stream = fs.createWriteStream(this.filePath, { flags: 'a', encoding: 'utf8' });
    this.errorCount = 0;
  }

  log(tag, message, meta = {}) {
    this.errorCount++;
    const line = JSON.stringify({
      ts: new Date().toISOString(),
      tag,
      message: String(message || ''),
      ...meta
    });
    this.stream.write(line + '\n');
  }

  close() {
    return new Promise((resolve) => {
      this.stream.end(resolve);
    });
  }
}
