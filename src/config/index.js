import dotenv from 'dotenv';
dotenv.config();

export const config = {
  env: process.env.NODE_ENV || 'production',

  // CLUSTER NODE ROLE: 'master' or 'worker'
  nodeRole: process.env.NODE_ROLE || 'master',
  serverId: process.env.SERVER_ID || `srv_${process.env.NODE_ROLE || 'node'}_${process.pid}`,

  // SOURCE MONGODB: People Collection (Direct website read) - Master Only
  sourceMongo: {
    uri: process.env.SOURCE_MONGODB_URI || 'mongodb://localhost:27017',
    peopleDb: process.env.SOURCE_PEOPLE_DB || 'dg_data',
    peopleCollection: process.env.SOURCE_PEOPLE_COLLECTION || 'es_cont_email',
    checkpointCollection: process.env.DEST_CHECKPOINT_COLLECTION || 'sync_checkpoints',
    checkpointDb: process.env.DEST_MONGODB_DB_NAME || 'email_generator_db',
    batchSize: parseInt(process.env.SOURCE_BATCH_SIZE || '500', 10),
    batchDelayMs: parseInt(process.env.SOURCE_BATCH_DELAY_MS || '100', 10)
  },

  // PARTITIONED PARQUET DIRECTORY STRUCTURE
  parquet: {
    baseDir: process.env.PARQUET_BASE_DIR || './data',
    validDir: process.env.PARQUET_VALID_DIR || './data/valid',
    waitingWebhookDir: process.env.PARQUET_WAITING_WEBHOOK_DIR || './data/invalid/waiting_webhook_v6',
    permanentInvalidCacheDir: process.env.PARQUET_PERM_INVALID_DIR || './data/invalid/permanent_invalid_cache',
    process2CompletedDir: process.env.PARQUET_P2_COMPLETED_DIR || './data/invalid/process2_completed',
    failedPermanentDir: process.env.PARQUET_FAILED_PERM_DIR || './data/invalid/failed_permanent',

    minCapacity: parseInt(process.env.PARQUET_MIN_CAPACITY || '10000', 10),
    maxCapacity: parseInt(process.env.PARQUET_MAX_CAPACITY || '100000', 10) // 100K rows per file
  },

  // SHARED REDIS CONFIGURATION (Connects all 3 servers)
  redis: {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: parseInt(process.env.REDIS_PORT || '6379', 10),
    password: process.env.REDIS_PASSWORD || undefined,
    db: parseInt(process.env.REDIS_DB || '0', 10),
    maxRetriesPerRequest: null
  },

  // CACHE VALIDATION ENDPOINT
  cacheValidation: {
    url: process.env.CACHE_VALIDATION_URL || 'https://cachevalidation.no2bounce.com/api/cache/check',
    token: process.env.CACHE_VALIDATION_TOKEN || ''
  },

  // VERIFY V6 WEBHOOK VALIDATION SERVICE
  webhookValidation: {
    url: process.env.WEBHOOK_VALIDATION_URL || 'https://verifyv6.no2bounce.com/api/v2/single-webhook-validation',
    apiToken: process.env.WEBHOOK_API_TOKEN || process.env.CACHE_VALIDATION_TOKEN || '',
    
    // ONLY ONE SINGLE SERVER RUNS THE WEBHOOK LISTENER
    // Set IS_WEBHOOK_RECEIVER=true on that single designated server (e.g. Master)
    // All other servers have IS_WEBHOOK_RECEIVER=false
    isWebhookReceiver: process.env.IS_WEBHOOK_RECEIVER === 'true' || (process.env.NODE_ROLE === 'master' && process.env.IS_WEBHOOK_RECEIVER !== 'false'),
    
    // The public/accessible callback URL of that ONE single server (e.g. http://10.0.0.1:3000)
    // Every worker server will pass THIS URL in their requests to verifyv6
    callbackBaseUrl: process.env.WEBHOOK_CALLBACK_BASE_URL || 'http://127.0.0.1:3000',
    serverPort: parseInt(process.env.WEBHOOK_SERVER_PORT || '3000', 10)
  },

  // WORKER TUNING FOR 150M in 5 DAYS
  queue: {
    name: 'email-verification-cluster-queue',
    workerConcurrency: parseInt(process.env.WORKER_CONCURRENCY || '60', 10),
    enableWorker: process.env.ENABLE_LOCAL_WORKER !== 'false' && parseInt(process.env.WORKER_CONCURRENCY || '60', 10) > 0,
    maxRetries: parseInt(process.env.MAX_JOB_RETRIES || '2', 10),
    retryBackoffMs: parseInt(process.env.RETRY_BACKOFF_MS || '1000', 10)
  }
};
