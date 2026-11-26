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

    console.log('[Master] Ingesting from MongoDB and distributing to cluster workers...');
    let batchCounter = 0;
    const MAX_READ_RETRIES = 5;

    while (this.isRunning && !this.stopRequested && totalIngested < maxRecords) {
      try {
        // High-Throughput Backpressure check: Check queue backlog every 5 batches (Threshold: 100,000 for 251GB RAM cluster)
        if (batchCounter % 5 === 0) {
          const qStatus = await this.pipeline.getStatus();
          if (qStatus.queueStatus.waiting > 100000) {
            console.log(`[Master:Backpressure] Queue buffer full (${qStatus.queueStatus.waiting.toLocaleString()} waiting). Pausing ingestion for 2s...`);
            await new Promise(r => setTimeout(r, 2000));
            continue;
          }
        }
        batchCounter++;

        // Batch read with transient error retry
        let batch = null;
        let readAttempts = 0;
        const remaining = maxRecords - totalIngested;
        const currentBatchLimit = Math.min(this.sourceReader.batchSize, remaining);

        while (readAttempts < MAX_READ_RETRIES && !batch) {
          try {
            batch = await this.sourceReader.fetchBatch(lastId, currentBatchLimit);
          } catch (readErr) {
            readAttempts++;
            console.warn(`[Master] Transient MongoDB read error (attempt ${readAttempts}/${MAX_READ_RETRIES}): ${readErr.message}`);
            if (readAttempts >= MAX_READ_RETRIES) throw readErr;
            await new Promise(r => setTimeout(r, 2000));
          }
        }

        if (!batch.records || batch.records.length === 0) {
          console.log('[Master] Reached end of matching people collection.');
          break;
        }

        // Push to cluster queue
        await this.pipeline.enqueueContacts(batch.records);

        lastId = batch.nextLastId;
        totalIngested += batch.records.length;

        // Display live progress in terminal
        console.log(`[Master:Stream] Ingested ${batch.records.length} contacts | Total Queued: ${totalIngested.toLocaleString()} | Last ID: ${lastId}`);

        // Save progress checkpoint
        await this.checkpointCol.updateOne(
          { key: CHECKPOINT_KEY },
          { $set: { key: CHECKPOINT_KEY, lastId, totalIngested, updatedAt: new Date() } },
          { upsert: true }
        );

        // Throttle pacing if specified
        if (config.sourceMongo.batchDelayMs > 0) {
          await new Promise(r => setTimeout(r, config.sourceMongo.batchDelayMs));
        }

        if (!lastId) break;
      } catch (err) {
        console.error('[Master] Fatal error reading MongoDB batch after retries:', err.message);
        break;
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
