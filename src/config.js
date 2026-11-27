import dotenv from 'dotenv';
dotenv.config();

export const config = {
  serverId: process.env.SERVER_ID || `v6_runner_${process.pid}`,
  inputDir: process.env.INPUT_DIR || '/root/no2bounce-webhook/validation/output',
  excludedFiles: (process.env.EXCLUDED_FILES || 'all_verifications_unique.jsonl')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean),

  v6: {
    url: process.env.WEBHOOK_VALIDATION_URL || 'https://verifyv6.no2bounce.com/api/v2/single-webhook-validation',
    apiToken: process.env.WEBHOOK_API_TOKEN || '',
    targetRps: parseInt(process.env.TARGET_RPS || '3000', 10),
    maxConcurrency: parseInt(process.env.MAX_CONCURRENCY || '1500', 10),
    timeoutMs: parseInt(process.env.API_TIMEOUT_MS || '15000', 10),
    maxRetries: parseInt(process.env.MAX_RETRIES || '3', 10)
  },

  webhook: {
    port: parseInt(process.env.WEBHOOK_PORT || '3000', 10),
    path: process.env.WEBHOOK_PATH || '/api/webhook/validation',
    callbackBaseUrl: (process.env.WEBHOOK_CALLBACK_BASE_URL || 'http://127.0.0.1:3000').replace(/\/+$/, '')
  },

  parquet: {
    baseDir: process.env.PARQUET_BASE_DIR || './data',
    maxCapacity: parseInt(process.env.PARQUET_MAX_CAPACITY || '1000000', 10)
  },

  checkpointFile: process.env.CHECKPOINT_FILE || './data/processed_ids.log',
  errorLogFile: process.env.ERROR_LOG_FILE || './logs/error.log',
  statsIntervalMs: parseInt(process.env.STATS_INTERVAL_MS || '1000', 10)
};
