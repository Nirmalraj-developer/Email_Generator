/**
 * @file pipelineManager.js
 * @description Cluster-ready Pipeline Manager with Integrated Webhook Receiver:
 *              - Works on Master, Worker, or Standalone node
 *              - Runs local WebhookReceiverServer when enableReceiver is true
 *              - Caches in-flight trackingId context in Redis so ANY server receiving the webhook
 *                can accurately resolve personId, name, domain, etc.
 *              - Enforces Option A (First Valid Wins) per personId
 *              - Writes results to Partitioned Subdirectories
 */

import { Queue, Worker } from 'bullmq';
import IORedis from 'ioredis';
import { EventEmitter } from 'events';
import crypto from 'crypto';
import { config } from '../config/index.js';
import { generateCandidateEmails, extractDomain } from '../services/patternGenerator.js';
import { CacheValidationClient } from '../services/cacheValidationClient.js';
import { WebhookValidationClient } from '../services/webhookValidationClient.js';
import { WebhookReceiverServer } from '../services/webhookServer.js';
import { ChunkedParquetWriter, validEmailSchema, invalidEmailSchema } from '../storage/parquetStorage.js';
import { errorLogger } from '../utils/errorLogger.js';

class InMemoryQueue extends EventEmitter {
  constructor(name) {
    super();
    this.name = name;
    this.jobs = [];
    this.active = false;
    this.processedCount = 0;
    this.failedCount = 0;
    this.completedCount = 0;
  }

  async add(name, data, opts = {}) {
    const job = {
      id: `mem_job_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`,
      name,
      data,
      opts,
      attemptsMade: 0
    };
    this.jobs.push(job);
    return job;
  }

  async addBulk(jobList) {
    const added = [];
    for (const item of jobList) {
      const job = await this.add(item.name, item.data, item.opts);
      added.push(job);
    }
    return added;
  }

  async getJobCounts() {
    return {
      waiting: this.jobs.length,
      active: this.active ? 1 : 0,
      completed: this.completedCount,
      failed: this.failedCount
    };
  }

  async close() {
    this.jobs = [];
  }
}

export class PipelineManager {
  /**
   * @param {object} [options]
   * @param {string} [options.queueName]
   * @param {string} [options.serverId]
   */
  constructor(options = {}) {
    this.queueName = options.queueName || config.queue.name;
    this.serverId = options.serverId || config.serverId;
    this.cacheClient = options.cacheClient || new CacheValidationClient();
    this.webhookClient = options.webhookClient || new WebhookValidationClient();
    
    // 1. Valid emails Parquet Writer (data/valid/)
    this.validParquetWriter = new ChunkedParquetWriter({
      outputDir: config.parquet.validDir,
      filePrefix: 'valid_emails',
      schema: validEmailSchema,
      minCapacity: config.parquet.minCapacity,
      maxCapacity: config.parquet.maxCapacity,
      serverId: this.serverId
    });

    // 2. Waiting for Webhook V6 Parquet Writer (data/invalid/waiting_webhook_v6/)
    this.waitingWebhookParquetWriter = new ChunkedParquetWriter({
      outputDir: config.parquet.waitingWebhookDir,
      filePrefix: 'waiting_webhook_v6',
      schema: invalidEmailSchema,
      minCapacity: config.parquet.minCapacity,
      maxCapacity: config.parquet.maxCapacity,
      serverId: this.serverId
    });

    // 3. Permanent Invalid Cache Parquet Writer -> INPUT FOR PROCESS 2 (data/invalid/permanent_invalid_cache/)
    this.permanentInvalidCacheParquetWriter = new ChunkedParquetWriter({
      outputDir: config.parquet.permanentInvalidCacheDir,
      filePrefix: 'permanent_invalid_cache',
      schema: invalidEmailSchema,
      minCapacity: config.parquet.minCapacity,
      maxCapacity: config.parquet.maxCapacity,
      serverId: this.serverId
    });

    // 4. Failed Permanent Parquet Writer (data/invalid/failed_permanent/)
    this.failedPermanentParquetWriter = new ChunkedParquetWriter({
      outputDir: config.parquet.failedPermanentDir,
      filePrefix: 'failed_permanent',
      schema: invalidEmailSchema,
      minCapacity: config.parquet.minCapacity,
      maxCapacity: config.parquet.maxCapacity,
      serverId: this.serverId
    });

    // In-flight tracking map (local fallback)
    this.localPendingTrackers = new Map();

    // Local in-memory set to ensure Option A across local execution
    this.resolvedPersonIds = new Set();

    // Webhook HTTP Receiver Server runs ONLY if this server is the designated single receiver
    this.webhookReceiver = config.webhookValidation.isWebhookReceiver ? new WebhookReceiverServer({
      port: config.webhookValidation.serverPort,
      onValidationResult: this._handleWebhookCallback.bind(this)
    }) : null;

    this.useMemoryQueue = false;
    this.redisClient = null;
    this.queue = null;
    this.worker = null;
    this.isStopping = false;

    this.metrics = {
      contactsReceived: 0,
      candidatesGenerated: 0,
      cacheChecks: 0,
      webhookSubmitted: 0,
      webhookValidReceived: 0,
      webhookInvalidReceived: 0,
      validFromCache: 0,
      allCachedInvalid: 0,
      failedJobs: 0
    };
  }

  /**
   * Initializes Redis BullMQ queue, worker, and webhook listener.
   */
  async initialize() {
    // Start webhook receiver if enabled on this node
    if (this.webhookReceiver) {
      await this.webhookReceiver.start();
    }

    try {
      const redisOptions = {
        host: config.redis.host,
        port: config.redis.port,
        password: config.redis.password || undefined,
        db: config.redis.db || 0,
        maxRetriesPerRequest: null,
        enableReadyCheck: false,
        keepAlive: 10000,
        connectTimeout: 10000,
        retryStrategy: (times) => Math.min(times * 100, 3000)
      };

      this.redisClient = new IORedis(redisOptions);
      this.redisClient.on('error', () => {});

      const pong = await this.redisClient.ping();
      if (pong !== 'PONG') throw new Error('Ping failed');

      this.queue = new Queue(this.queueName, {
        connection: this.redisClient,
        defaultJobOptions: {
          attempts: config.queue.maxRetries,
          backoff: {
            type: 'exponential',
            delay: config.queue.retryBackoffMs
          },
          removeOnComplete: 2000,
          removeOnFail: 5000
        }
      });

      console.log(`[PipelineManager] Connected to BullMQ Cluster with Redis at ${config.redis.host}:${config.redis.port}`);
    } catch (err) {
      if (this.redisClient) {
        this.redisClient.disconnect(false);
        this.redisClient = null;
      }
      console.warn(`[PipelineManager] Redis not reachable (${err.message}). Activating In-Memory Fallback Queue.`);
      this.useMemoryQueue = true;
      this.queue = new InMemoryQueue(this.queueName);
    }

    this._startWorker();
  }

  _startWorker() {
    if (!config.queue.enableWorker) {
      console.log('[PipelineManager] Local worker disabled (Dispatcher-Only Mode). Node will only read MongoDB & push jobs.');
      return;
    }

    if (this.useMemoryQueue) {
      this._startMemoryWorker();
      return;
    }

    const workerConnection = new IORedis({
      host: config.redis.host,
      port: config.redis.port,
      password: config.redis.password || undefined,
      db: config.redis.db || 0,
      maxRetriesPerRequest: null,
      enableReadyCheck: false,
      keepAlive: 10000,
      connectTimeout: 10000,
      retryStrategy: (times) => Math.min(times * 100, 3000)
    });
    workerConnection.on('error', () => {});

    const workerOptions = {
      connection: workerConnection,
      concurrency: config.queue.workerConcurrency,
      lockDuration: 30000,
      stalledInterval: 15000,
      maxStalledCount: 2
    };

    this.worker = new Worker(this.queueName, async (job) => {
      return await this._processContactJob(job.data);
    }, workerOptions);

    this.worker.on('failed', (job, err) => {
      this.metrics.failedJobs++;
      errorLogger.logError('WORKER_JOB_FAILED', err, { jobId: job?.id, personId: job?.data?.id });
    });
  }

  _startMemoryWorker() {
    const loop = async () => {
      while (!this.isStopping) {
        if (this.queue.jobs.length > 0) {
          const job = this.queue.jobs.shift();
          try {
            this.queue.active = true;
            await this._processContactJob(job.data);
            this.queue.completedCount++;
          } catch (err) {
            this.queue.failedCount++;
            this.metrics.failedJobs++;
            errorLogger.logError('MEMORY_WORKER_FAILED', err);
          } finally {
            this.queue.active = false;
          }
        } else {
          await new Promise(r => setTimeout(r, 200));
        }
      }
    };
    loop();
  }

  _generateTrackingId(personId) {
    const timestamp = Date.now().toString(36);
    const randomHex = crypto.randomBytes(4).toString('hex');
    return `trk_${personId || 'p'}_${timestamp}_${randomHex}`;
  }

  /**
   * Option A: Distributed Atomic Claim for First Valid Email per person
   */
  async _claimFirstValidForPerson(personId) {
    if (!personId) return true;

    if (this.resolvedPersonIds.has(personId)) {
      return false;
    }

    if (this.redisClient && !this.useMemoryQueue) {
      try {
        const acquired = await this.redisClient.set(`person_valid_claimed:${personId}`, '1', 'EX', 86400 * 7, 'NX');
        if (acquired === 'OK') {
          this.resolvedPersonIds.add(personId);
          return true;
        }
        this.resolvedPersonIds.add(personId);
        return false;
      } catch (err) {
        // Fallback to local memory lock
      }
    }

    this.resolvedPersonIds.add(personId);
    return true;
  }

  /**
   * Saves in-flight tracking context (in Redis for cluster-wide resolution, or local map)
   */
  async _saveInFlightTracker(trackingId, context) {
    this.localPendingTrackers.set(trackingId, context);
    if (this.redisClient && !this.useMemoryQueue) {
      try {
        await this.redisClient.set(`tracker:${trackingId}`, JSON.stringify(context), 'EX', 86400 * 3); // 3-day expiry
      } catch (err) {
        // fallback to local
      }
    }
  }

  /**
   * Resolves in-flight tracking context from Redis or local memory
   */
  async _getInFlightTracker(trackingId) {
    if (this.redisClient && !this.useMemoryQueue) {
      try {
        const data = await this.redisClient.get(`tracker:${trackingId}`);
        if (data) return JSON.parse(data);
      } catch (err) {}
    }
    return this.localPendingTrackers.get(trackingId) || {};
  }

  /**
   * Main Contact Evaluation Pipeline:
   */
  async _processContactJob(contact) {
    this.metrics.contactsReceived++;
    const { id, firstName, lastName, website, companyName, companyId } = contact;
    const domain = extractDomain(website);
    const localTrackingId = this._generateTrackingId(id);

    const candidates = generateCandidateEmails(firstName, lastName, website);
    this.metrics.candidatesGenerated += candidates.length;

    let validFound = null;
    const cacheEvaluationResults = [];
    const missingCacheCandidates = [];

    // STEP 1: Evaluate candidates against Cache API sequentially
    for (const candidate of candidates) {
      if (this.isStopping) break;

      this.metrics.cacheChecks++;
      const cacheResult = await this.cacheClient.checkCache(candidate.email);
      cacheEvaluationResults.push({ candidate, cacheResult });

      const score = Number(cacheResult.score ?? cacheResult.cacheResponse?.score ?? 0);
      const isDeliverable = Boolean(cacheResult.deliverable ?? cacheResult.cacheResponse?.isDeliverable);
      const inCache = Boolean(cacheResult.foundInCache);

      // SCENARIO 1: Valid found in cache ("isDeliverable": true and "score": 99 / >= 90)
      if (inCache && isDeliverable && (score >= 90 || score === 99)) {
        validFound = {
          trackingId: localTrackingId,
          personId: id,
          firstName,
          lastName,
          companyId: companyId || '',
          companyName: companyName || '',
          website,
          domain: candidate.domain,
          pattern: candidate.pattern,
          email: candidate.email,
          source: 'cache',
          score,
          appName: cacheResult.cacheResponse?.appName || 'molset',
          isDeliverable: true,
          isRisky: Boolean(cacheResult.cacheResponse?.isRisky),
          isUndeliverable: false,
          isB2C: Boolean(cacheResult.cacheResponse?.isB2C),
          createdAt: Date.now()
        };
        break; // Stop testing more patterns
      }

      // If inCache: false -> Collect candidate for Verify V6 Webhook validation
      if (!inCache) {
        missingCacheCandidates.push(candidate);
      }
    }

    // ACTION FOR SCENARIO 1: Valid found in cache -> Append to data/valid/
    if (validFound) {
      const isFirst = await this._claimFirstValidForPerson(id);
      if (isFirst) {
        await this.validParquetWriter.append(validFound);
        this.metrics.validFromCache++;
      }
      return { success: true, scenario: 'VALID_IN_CACHE', email: validFound.email };
    }

    // SCENARIO 2: All 10 patterns found in cache with low score / undeliverable
    // -> Append directly to data/invalid/permanent_invalid_cache/ (INPUT FOR PROCESS 2)
    const allFoundInCacheLowScore = (missingCacheCandidates.length === 0) && (cacheEvaluationResults.length === candidates.length);
    if (allFoundInCacheLowScore) {
      await this.permanentInvalidCacheParquetWriter.append({
        trackingId: localTrackingId,
        personId: id,
        firstName,
        lastName,
        companyId: companyId || '',
        companyName: companyName || '',
        website,
        domain,
        email: '',
        pattern: '',
        status: 'all_patterns_cached_invalid',
        score: 0,
        inCache: true,
        reason: 'all_10_patterns_found_in_cache_with_low_score_or_undeliverable',
        createdAt: Date.now()
      });
      this.metrics.allCachedInvalid++;
      return { success: true, scenario: 'ALL_PATTERNS_CACHED_INVALID' };
    }

    // SCENARIO 3: Missing in cache -> Submit to Verify V6 Webhook API
    const waitingWebhookRecords = [];

    for (const missingCandidate of missingCacheCandidates) {
      if (this.isStopping) break;

      this.metrics.webhookSubmitted++;
      const res = await this.webhookClient.submitForValidation(missingCandidate.email);
      const returnedTrackingId = res.trackingId || localTrackingId;

      // Persist in-flight tracker so ANY server receiving the webhook can resolve context
      await this._saveInFlightTracker(returnedTrackingId, {
        personId: id,
        firstName,
        lastName,
        companyId: companyId || '',
        companyName: companyName || '',
        website,
        domain: missingCandidate.domain,
        pattern: missingCandidate.pattern,
        email: missingCandidate.email,
        submittedAt: Date.now()
      });

      waitingWebhookRecords.push({
        trackingId: returnedTrackingId,
        personId: id,
        firstName,
        lastName,
        companyId: companyId || '',
        companyName: companyName || '',
        website,
        domain: missingCandidate.domain,
        email: missingCandidate.email,
        pattern: missingCandidate.pattern,
        status: 'waiting_webhook_v6',
        score: 0,
        inCache: false,
        reason: res.message || 'Request accepted for real-time validation',
        createdAt: Date.now()
      });
    }

    if (waitingWebhookRecords.length > 0) {
      await this.waitingWebhookParquetWriter.appendBatch(waitingWebhookRecords);
    }

    return { success: true, scenario: 'SENT_TO_WEBHOOK_VALIDATION', count: waitingWebhookRecords.length };
  }

  /**
   * Asynchronous Webhook Callback Handler:
   * Triggers when verifyv6 sends the callback back to POST /api/webhook/validation
   */
  async _handleWebhookCallback(payload) {
    const trackingId = payload.trackingId || payload.wh_trackingId;
    const email = payload.email;
    const isDeliverable = Boolean(payload.isDeliverable ?? payload.deliverable ?? payload.status === 'valid');
    const score = Number(payload.score ?? (isDeliverable ? 99 : 0));

    // Resolve context across cluster (from Redis or local)
    const context = await this._getInFlightTracker(trackingId);
    const personId = context.personId || '';

    if (isDeliverable && score >= 90) {
      // Option A: First Valid Wins per personId across the cluster
      const isFirstValid = await this._claimFirstValidForPerson(personId);

      if (isFirstValid) {
        await this.validParquetWriter.append({
          trackingId: trackingId || this._generateTrackingId(personId),
          personId: personId,
          firstName: context.firstName || '',
          lastName: context.lastName || '',
          companyId: context.companyId || '',
          companyName: context.companyName || '',
          website: context.website || '',
          domain: context.domain || '',
          pattern: context.pattern || '',
          email: email || context.email,
          source: 'verifyv6_webhook',
          score,
          appName: 'verifyv6',
          isDeliverable: true,
          isRisky: Boolean(payload.isRisky),
          isUndeliverable: false,
          isB2C: Boolean(payload.isB2C),
          createdAt: Date.now()
        });
        this.metrics.webhookValidReceived++;
      } else {
        await this.failedPermanentParquetWriter.append({
          trackingId: trackingId || '',
          personId: personId,
          firstName: context.firstName || '',
          lastName: context.lastName || '',
          companyId: context.companyId || '',
          companyName: context.companyName || '',
          website: context.website || '',
          domain: context.domain || '',
          email: email || context.email || '',
          pattern: context.pattern || '',
          status: 'secondary_valid_ignored',
          score,
          inCache: false,
          reason: 'person_already_has_primary_valid_email',
          createdAt: Date.now()
        });
      }
    } else {
      await this.failedPermanentParquetWriter.append({
        trackingId: trackingId || '',
        personId: personId,
        firstName: context.firstName || '',
        lastName: context.lastName || '',
        companyId: context.companyId || '',
        companyName: context.companyName || '',
        website: context.website || '',
        domain: context.domain || '',
        email: email || context.email || '',
        pattern: context.pattern || '',
        status: 'webhook_validated_undeliverable',
        score,
        inCache: false,
        reason: payload.reason || payload.message || 'undeliverable_via_webhook',
        createdAt: Date.now()
      });
      this.metrics.webhookInvalidReceived++;
    }

    if (trackingId) {
      this.localPendingTrackers.delete(trackingId);
    }
  }

  async enqueueContacts(contacts = []) {
    const jobs = contacts.map(c => ({
      name: 'verify_contact_patterns',
      data: c,
      opts: {
        jobId: `contact_${c.id}`
      }
    }));

    await this.queue.addBulk(jobs);
  }

  async safeStop() {
    console.log(`[PipelineManager:${this.serverId}] Initiating Safe Stop & Flushing all Partitioned Parquet Writers...`);
    this.isStopping = true;

    if (this.webhookReceiver) {
      await this.webhookReceiver.stop();
    }

    if (this.worker && typeof this.worker.close === 'function') {
      await this.worker.close();
    }
    if (this.queue && typeof this.queue.close === 'function') {
      await this.queue.close();
    }
    if (this.redisClient) {
      await this.redisClient.quit().catch(() => {});
    }

    // Flush all 4 partitioned Parquet files
    await this.validParquetWriter.close();
    await this.waitingWebhookParquetWriter.close();
    await this.permanentInvalidCacheParquetWriter.close();
    await this.failedPermanentParquetWriter.close();
    console.log(`[PipelineManager:${this.serverId}] All Partitioned Parquet files flushed and closed cleanly.`);
  }

  async getStatus() {
    const counts = await this.queue.getJobCounts();
    return {
      serverId: this.serverId,
      isMemoryQueue: this.useMemoryQueue,
      queueStatus: counts,
      metrics: this.metrics,
      validParquet: this.validParquetWriter.getStats(),
      waitingWebhookParquet: this.waitingWebhookParquetWriter.getStats(),
      permanentInvalidCacheParquet: this.permanentInvalidCacheParquetWriter.getStats(),
      failedPermanentParquet: this.failedPermanentParquetWriter.getStats()
    };
  }
}
