/**
 * @file appController.js
 * @description Cluster-Aware App Controller supporting:
 *              - Master Mode: Reads MongoDB batches, enqueues to Redis BullMQ, AND runs local worker
 *              - Worker Mode: Listens to Redis BullMQ only and processes jobs (zero MongoDB reading)
 *              - Safe Stop & Checkpointing
 */

import { SourceMongoReader } from '../services/sourceMongoReader.js';
import { PipelineManager } from '../queue/pipelineManager.js';
import { MongoClient } from 'mongodb';
import { config } from '../config/index.js';

export class AppController {
  constructor() {
    this.role = config.nodeRole; // 'master' or 'worker'
    this.pipeline = new PipelineManager({ serverId: config.serverId });
    this.sourceReader = this.role === 'master' ? new SourceMongoReader() : null;
    this.checkpointClient = null;
    this.checkpointCol = null;
    this.isRunning = false;
    this.stopRequested = false;
  }

  async init() {
    console.log('====================================================');
    console.log(`🚀 Email Generator & Verification Cluster Node`);
    console.log(`🏷️  Server ID:   ${config.serverId}`);
    console.log(`👑  Node Role:   ${this.role.toUpperCase()}`);
    console.log(`💾  Storage:     Chunked Parquet Files [${config.parquet.baseDir}]`);
    console.log(`⚡  Mode:        ${config.queue.enableWorker ? `${config.queue.workerConcurrency} concurrent workers` : 'DISPATCHER ONLY (Zero verification work, enqueues to workers)'}`);
    console.log('====================================================');

    await this.pipeline.initialize();

    if (this.role === 'master') {
      await this.sourceReader.connect();
      
      // Connect checkpoint collection
      this.checkpointClient = new MongoClient(config.sourceMongo.uri);
      await this.checkpointClient.connect();
      this.checkpointCol = this.checkpointClient
        .db(config.sourceMongo.checkpointDb)
        .collection(config.sourceMongo.checkpointCollection);
    }

    this._setupGracefulShutdown();
  }

  _setupGracefulShutdown() {
    const handleShutdown = async (signal) => {
      if (this.stopRequested) return;
      this.stopRequested = true;
      console.log(`\n🛑 [${this.role.toUpperCase()}] Received ${signal}. Initiating safe stop...`);
      await this.stop();
      process.exit(0);
    };

    process.on('SIGINT', () => handleShutdown('SIGINT'));
    process.on('SIGTERM', () => handleShutdown('SIGTERM'));
  }

  /**
   * Run Master Loop: Read Mongo -> Enqueue to Redis -> Work simultaneously
   */
  async runMaster({ resumeFromCheckpoint = true, maxRecords = Infinity } = {}) {
    this.isRunning = true;
    let lastId = null;
    let totalIngested = 0;

    const CHECKPOINT_KEY = 'mongo_stream_parquet_checkpoint';

    if (resumeFromCheckpoint) {
      const saved = await this.checkpointCol.findOne({ key: CHECKPOINT_KEY });
      if (saved?.lastId) {
        lastId = saved.lastId;
        totalIngested = saved.totalIngested || 0;
        console.log(`[Master:PatchContinue] Resuming from cursor. Ingested so far: ${totalIngested}, Last ID:`, lastId);
      }
    }

    console.log('[Master] Streaming continuously from MongoDB to keep cluster queue stocked...');
    let chunkCounter = 0;
    let consecutiveErrors = 0;
    const MAX_STREAM_RETRIES = 10;
    const QUEUE_HIGH_WATERMARK = 100000;

    while (this.isRunning && !this.stopRequested && totalIngested < maxRecords) {
      try {
        const remaining = maxRecords - totalIngested;
        await this.sourceReader.streamContacts(lastId, {
          chunkSize: 2000,
          maxRecords: remaining,
          shouldStop: () => !this.isRunning || this.stopRequested,
          onChunk: async ({ records, lastId: chunkLastId, rawScanned }) => {
            consecutiveErrors = 0;

            if (records.length > 0) {
              await this.pipeline.enqueueContacts(records);
              totalIngested += records.length;
            }

            if (chunkLastId) {
              lastId = chunkLastId;
            }

            chunkCounter++;
            if (records.length > 0 || chunkCounter % 5 === 0) {
              console.log(`[Master:Stream] Pushed +${records.length}/${rawScanned} valid-website contacts | Total Queued: ${totalIngested.toLocaleString()} | Last ID: ${lastId}`);
            }

            // Save progress checkpoint every 5 chunks (~10,000 contacts) without blocking every chunk
            if (lastId && chunkCounter % 5 === 0) {
              this.checkpointCol.updateOne(
                { key: CHECKPOINT_KEY },
                { $set: { key: CHECKPOINT_KEY, lastId, totalIngested, updatedAt: new Date() } },
                { upsert: true }
              ).catch(() => {});
            }

            // Backpressure: keep up to 100,000 jobs stocked in Redis, pause only if buffer exceeds 100K
            if (chunkCounter % 5 === 0) {
              let qStatus = await this.pipeline.getStatus();
              while (qStatus.queueStatus.waiting > QUEUE_HIGH_WATERMARK && this.isRunning && !this.stopRequested) {
                console.log(`[Master:StockFull] Queue stocked (${qStatus.queueStatus.waiting.toLocaleString()} waiting). Pausing 2s...`);
                await new Promise(r => setTimeout(r, 2000));
                qStatus = await this.pipeline.getStatus();
              }
            }
          }
        });

        // Stream completed cleanly (reached end of collection or maxRecords)
        if (!this.stopRequested) {
          console.log('[Master] Reached end of matching people collection or limit.');
        }
        break;
      } catch (err) {
        consecutiveErrors++;
        console.warn(`[Master] Transient stream error (attempt ${consecutiveErrors}/${MAX_STREAM_RETRIES}): ${err.message}`);
        if (consecutiveErrors >= MAX_STREAM_RETRIES) {
          console.error('[Master] Fatal error streaming MongoDB after retries:', err.message);
          break;
        }
        await new Promise(r => setTimeout(r, 2000));
      }
    }

    console.log(`[Master] Ingestion finished (${totalIngested} contacts sent to cluster). Working down active queue...`);

    // Master continues working its own share until queue is fully drained
    let status = await this.pipeline.getStatus();
    while ((status.queueStatus.waiting > 0 || status.queueStatus.active > 0) && !this.stopRequested) {
      const totalValid = (status.metrics.validFromCache || 0) + (status.metrics.webhookValidReceived || 0);
      const totalInvalid = (status.metrics.allCachedInvalid || 0) + (status.metrics.webhookInvalidReceived || 0);
      console.log(`[Master:Progress] Queue: Waiting=${status.queueStatus.waiting}, Active=${status.queueStatus.active} | Node Stored: Valid=${totalValid}, Invalid=${totalInvalid}`);
      await new Promise(r => setTimeout(r, 3000));
      status = await this.pipeline.getStatus();
    }

    console.log('✅ [Master] Cluster jobs completed.');
    this.printSummary(status);
  }

  /**
   * Run Worker Loop: Listen to Redis BullMQ only and write Parquet locally
   */
  async runWorker() {
    this.isRunning = true;
    console.log('[Worker] Connected and listening to Redis BullMQ cluster queue...');

    while (this.isRunning && !this.stopRequested) {
      try {
        const status = await this.pipeline.getStatus();
        const totalValid = (status.metrics.validFromCache || 0) + (status.metrics.webhookValidReceived || 0);
        const totalInvalid = (status.metrics.allCachedInvalid || 0) + (status.metrics.webhookInvalidReceived || 0);
        console.log(`[Worker:${config.serverId}] Active=${status.queueStatus.active}, QueueWaiting=${status.queueStatus.waiting} | Valid=${totalValid}, Invalid=${totalInvalid}`);
      } catch (err) {
        // Suppress transient Redis disconnect log during reconnection
      }
      await new Promise(r => setTimeout(r, 5000));
    }
  }

  async run() {
    const maxRecords = process.env.MAX_CONTACTS_LIMIT ? parseInt(process.env.MAX_CONTACTS_LIMIT, 10) : Infinity;
    const resetCheckpoint = process.env.RESET_CHECKPOINT === 'true';

    if (this.role === 'master') {
      await this.runMaster({ resumeFromCheckpoint: !resetCheckpoint, maxRecords });
    } else {
      await this.runWorker();
    }
  }

  printSummary(status) {
    console.log('\n================ NODE EXECUTION SUMMARY ================');
    console.log(`Server ID:                           ${config.serverId}`);
    console.log(`Role:                                ${this.role.toUpperCase()}`);
    console.log(`Contacts Evaluated (This Node):     ${status.metrics.contactsReceived}`);
    console.log(`Candidate Patterns Tested:           ${status.metrics.candidatesGenerated}`);
    console.log(`Valid Written to Parquet (Cache):    ${status.metrics.validFromCache || 0}`);
    console.log(`Valid Written to Parquet (Webhook):  ${status.metrics.webhookValidReceived || 0}`);
    console.log(`Invalid (All 10 Cached Low Score):   ${status.metrics.allCachedInvalid || 0}`);
    console.log(`Invalid (Webhook Validated):         ${status.metrics.webhookInvalidReceived || 0}`);
    console.log(`Active Valid Parquet File:           ${status.validParquet?.currentFilePath || 'N/A'}`);
    console.log(`Active Invalid Parquet File:         ${status.invalidParquet?.currentFilePath || 'N/A'}`);
    console.log('========================================================\n');
  }

  async stop() {
    this.isRunning = false;
    if (this.sourceReader) {
      await this.sourceReader.disconnect();
    }
    if (this.checkpointClient) {
      await this.checkpointClient.close();
    }
    await this.pipeline.safeStop();
  }
}
