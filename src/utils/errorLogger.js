/**
 * @file errorLogger.js
 * @description Dedicated Error Logger that ONLY logs errors to a clean, rotating log file
 *              (logs/error.log) and isolates them from regular console output.
 */

import fs from 'fs';
import path from 'path';

export class ErrorLogger {
  constructor(logDir = './logs') {
    this.logDir = logDir;
    this.logFilePath = path.join(this.logDir, 'error.log');
    
    if (!fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }
  }

  /**
   * Appends an error event to logs/error.log
   *
   * @param {string} context - Where the error happened (e.g. 'CACHE_API', 'WEBHOOK_V6', 'PARQUET_WRITER')
   * @param {Error|string} err - The error object or error message
   * @param {object} [metadata] - Optional context info (personId, email, trackingId)
   */
  logError(context, err, metadata = {}) {
    const timestamp = new Date().toISOString();
    const errorMessage = typeof err === 'string' ? err : (err?.message || 'Unknown error');
    const stack = err?.stack ? `\n  Stack: ${err.stack.split('\n').slice(1, 4).join('\n  ')}` : '';
    
    const metaStr = Object.keys(metadata).length > 0 ? ` | Meta: ${JSON.stringify(metadata)}` : '';
    const line = `[${timestamp}] [ERROR] [${context}] ${errorMessage}${metaStr}${stack}\n`;

    try {
      fs.appendFileSync(this.logFilePath, line, 'utf8');
    } catch (writeErr) {
      console.error('[ErrorLogger Failed to write]:', writeErr.message);
    }
  }
}

export const errorLogger = new ErrorLogger();
