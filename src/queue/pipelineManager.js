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
import { ChunkedParquetWriter, validEmailSchema, webhookTriggerSchema, invalidEmailSchema } from '../storage/parquetStorage.js';
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

    // 2. Webhook Triggers Parquet Writer (data/webhook_triggers/) - Log every V6 submission with trackingId
    this.webhookTriggersParquetWriter = new ChunkedParquetWriter({
      outputDir: config.parquet.webhookTriggersDir,
      filePrefix: 'webhook_triggers',
      schema: webhookTriggerSchema,
      minCapacity: config.parquet.minCapacity,
      maxCapacity: config.parquet.maxCapacity,
      serverId: this.serverId
    });

    // 3. Invalid Parquet Writer (data/invalid/) - All dead leads, bounces, low scores
    this.invalidParquetWriter = new ChunkedParquetWriter({
      outputDir: config.parquet.invalidDir,
      filePrefix: 'invalid_emails',
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

    if (options.useMemoryQueue) {
      this.useMemoryQueue = true;
      this.queue = new InMemoryQueue(this.queueName);
    }
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

    if (this.useMemoryQueue) {
      if (!this.queue) {
        this.queue = new InMemoryQueue(this.queueName);
      }
      this._startWorker();
      return;
    }

    try {
      console.log(`[PipelineManager] Connecting to Redis cluster at ${config.redis.host}:${config.redis.port}...`);

      const redisOptions = {
        host: config.redis.host,
        port: config.redis.port,
        password: config.redis.password || undefined,
        db: config.redis.db || 0,
        maxRetriesPerRequest: null,
        enableReadyCheck: false,
        keepAlive: 10000,
        connectTimeout: 5000,
        retryStrategy: (times) => Math.min(times * 100, 3000)
      };

      this.redisClient = new IORedis(redisOptions);
      this.redisClient.on('error', () => {});

      const pongPromise = this.redisClient.ping();
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error(`Redis connection timed out after 5000ms connecting to ${config.redis.host}:${config.redis.port}`)), 5000)
      );
      const pong = await Promise.race([pongPromise, timeoutPromise]);
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

      if (config.nodeRole === 'worker') {
        console.error(`\n❌ [FATAL] Worker node cannot connect to Redis at ${config.redis.host}:${config.redis.port} (${err.message})`);
        console.error(`   Please check that Server 1 Redis is running (redis-cli ping) and port ${config.redis.port} is reachable.\n`);
        throw err;
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
      if (job.name === 'verify_next_pattern') {
        return await this._processNextPatternJob(job.data);
      }
      return await this._processContactJob(job.data);
    }, workerOptions);

    console.log(`[Worker:${this.serverId}] BullMQ Worker active with ${config.queue.workerConcurrency} concurrent slots. Listening for cluster jobs...`);

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
            if (job.name === 'verify_next_pattern') {
              await this._processNextPatternJob(job.data);
            } else {
              await this._processContactJob(job.data);
            }
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

  _addResolvedPersonId(personId) {
    if (this.resolvedPersonIds.size > 100000) {
      const it = this.resolvedPersonIds.values();
      for (let i = 0; i < 20000; i++) {
        this.resolvedPersonIds.delete(it.next().value);
      }
    }
    this.resolvedPersonIds.add(personId);
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
          this._addResolvedPersonId(personId);
          return true;
        }
        this._addResolvedPersonId(personId);
        return false;
      } catch (err) {
        // Fallback to local memory lock
      }
    }

    this._addResolvedPersonId(personId);
    return true;
  }

  /**
   * Saves in-flight tracking context (in Redis for cluster-wide resolution, or local map)
   */
  async _saveInFlightTracker(trackingId, context) {
    if (this.localPendingTrackers.size > 100000) {
      const it = this.localPendingTrackers.keys();
      for (let i = 0; i < 20000; i++) {
        this.localPendingTrackers.delete(it.next().value);
      }
    }
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
   * Waterfall State Machine: Save or update person's candidate patterns progression
   */
  async _savePersonWaterfallState(personId, state) {
    if (this.redisClient && !this.useMemoryQueue) {
      try {
        await this.redisClient.set(`waterfall:${personId}`, JSON.stringify(state), 'EX', 86400); // 24-hr TTL
        return;
      } catch (err) {}
    }
    if (!this.localWaterfallStates) this.localWaterfallStates = new Map();
    this.localWaterfallStates.set(personId, state);
  }

  /**
   * Waterfall State Machine: Fetch person's progression state
   */
  async _getPersonWaterfallState(personId) {
    if (this.redisClient && !this.useMemoryQueue) {
      try {
        const raw = await this.redisClient.get(`waterfall:${personId}`);
        if (raw) return JSON.parse(raw);
      } catch (err) {}
    }
    return (this.localWaterfallStates && this.localWaterfallStates.get(personId)) || null;
  }

  /**
   * Waterfall State Machine: Delete person state once finalized
   */
  async _deletePersonWaterfallState(personId) {
    if (this.redisClient && !this.useMemoryQueue) {
      try {
        await this.redisClient.del(`waterfall:${personId}`);
      } catch (err) {}
    }
    if (this.localWaterfallStates) {
      this.localWaterfallStates.delete(personId);
    }
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

    // STEP 1: Fast Parallel Cache Evaluation for all candidate patterns
    this.metrics.cacheChecks += candidates.length;
    const cacheResults = await Promise.all(
      candidates.map(candidate => this.cacheClient.checkCache(candidate.email))
    );

    for (let i = 0; i < candidates.length; i++) {
      const candidate = candidates[i];
      const cacheResult = cacheResults[i];
      cacheEvaluationResults.push({ candidate, cacheResult });

      const score = Number(cacheResult.score ?? cacheResult.cacheResponse?.score ?? 0);
      const isDeliverable = Boolean(cacheResult.deliverable ?? cacheResult.cacheResponse?.isDeliverable);
      const inCache = Boolean(cacheResult.foundInCache);

      // SCENARIO 1: Valid found in cache ("isDeliverable": true and "score": 99 / >= 90)
      if (inCache && isDeliverable && (score >= 90 || score === 99) && !validFound) {
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
      }

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

    // SCENARIO 2: All 10 patterns found in cache with low score / undeliverable -> data/invalid/
    const allFoundInCacheLowScore = (missingCacheCandidates.length === 0) && (cacheEvaluationResults.length === candidates.length);
    if (allFoundInCacheLowScore) {
      await this.invalidParquetWriter.append({
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
        deliverable: false,
        isDeliverable: false,
        isRisky: false,
        isUndeliverable: true,
        isB2C: false,
        status: 'all_patterns_cached_invalid',
        score: 0,
        reason: 'all_10_patterns_found_in_cache_with_low_score_or_undeliverable',
        createdAt: Date.now()
      });
      this.metrics.allCachedInvalid++;
      return { success: true, scenario: 'ALL_PATTERNS_CACHED_INVALID' };
    }

    // SCENARIO 3: Missing in cache -> Waterfall progression: Trigger ONLY Pattern 1
    // Store full waterfall candidate list in Redis for cluster-wide escalation
    await this._savePersonWaterfallState(id, {
      personId: id,
      firstName,
      lastName,
      companyId: companyId || '',
      companyName: companyName || '',
      website,
      domain,
      missingCandidates: missingCacheCandidates,
      currentIndex: 0
    });

    const firstCandidate = missingCacheCandidates[0];
    this.metrics.webhookSubmitted++;
    const res = await this.webhookClient.submitForValidation(firstCandidate.email);
    const returnedTrackingId = res.trackingId || localTrackingId;

    // Persist in-flight tracker so ANY server receiving the webhook can resolve context
    await this._saveInFlightTracker(returnedTrackingId, {
      personId: id,
      firstName,
      lastName,
      companyId: companyId || '',
      companyName: companyName || '',
      website,
      domain: firstCandidate.domain,
      pattern: firstCandidate.pattern,
      email: firstCandidate.email,
      patternIndex: 0,
      submittedAt: Date.now()
    });

    await this.webhookTriggersParquetWriter.append({
      trackingId: returnedTrackingId,
      personId: id,
      firstName,
      lastName,
      companyId: companyId || '',
      companyName: companyName || '',
      website,
      domain: firstCandidate.domain,
      pattern: firstCandidate.pattern,
      email: firstCandidate.email,
      status: 'triggered_waiting_callback',
      createdAt: Date.now()
    });

    return { success: true, scenario: 'WATERFALL_PATTERN_1_TRIGGERED', email: firstCandidate.email };
  }

  /**
   * Distributed Waterfall Escalator:
   * Processed by ANY worker (Server 2, 3, or 4) when the previous pattern was undeliverable
   */
  async _processNextPatternJob(jobData) {
    const { personId, nextIndex } = jobData;
    const waterfallState = await this._getPersonWaterfallState(personId);
    if (!waterfallState || !waterfallState.missingCandidates) {
      return { success: false, reason: 'NO_WATERFALL_STATE' };
    }

    // Check if person was already claimed as valid while in-flight
    if (this.resolvedPersonIds.has(personId)) {
      await this._deletePersonWaterfallState(personId);
      return { success: true, scenario: 'ALREADY_RESOLVED_VALID' };
    }

    if (nextIndex >= waterfallState.missingCandidates.length) {
      // All candidate patterns have been tested and returned invalid
      await this._deletePersonWaterfallState(personId);
      return { success: true, scenario: 'ALL_PATTERNS_EXHAUSTED' };
    }

    const nextCandidate = waterfallState.missingCandidates[nextIndex];
    waterfallState.currentIndex = nextIndex;
    await this._savePersonWaterfallState(personId, waterfallState);

    this.metrics.webhookSubmitted++;
    const res = await this.webhookClient.submitForValidation(nextCandidate.email);
    const trackingId = res.trackingId || this._generateTrackingId(personId);

    await this._saveInFlightTracker(trackingId, {
      personId: personId,
      firstName: waterfallState.firstName,
      lastName: waterfallState.lastName,
      companyId: waterfallState.companyId,
      companyName: waterfallState.companyName,
      website: waterfallState.website,
      domain: nextCandidate.domain,
      pattern: nextCandidate.pattern,
      email: nextCandidate.email,
      patternIndex: nextIndex,
      submittedAt: Date.now()
    });

    await this.webhookTriggersParquetWriter.append({
      trackingId,
      personId,
      firstName: waterfallState.firstName,
      lastName: waterfallState.lastName,
      companyId: waterfallState.companyId,
      companyName: waterfallState.companyName,
      website: waterfallState.website,
      domain: nextCandidate.domain,
      pattern: nextCandidate.pattern,
      email: nextCandidate.email,
      status: 'triggered_waiting_callback',
      createdAt: Date.now()
    });

    return { success: true, scenario: 'NEXT_PATTERN_TRIGGERED', patternIndex: nextIndex, email: nextCandidate.email };
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

      // Successfully found a deliverable valid email! Delete waterfall state so no more patterns are triggered
      await this._deletePersonWaterfallState(personId);

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
          deliverable: true,
          isDeliverable: true,
          isRisky: Boolean(payload.isRisky),
          isUndeliverable: false,
          isB2C: Boolean(payload.isB2C),
          createdAt: Date.now()
        });
        this.metrics.webhookValidReceived++;
      } else {
        await this.invalidParquetWriter.append({
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
          deliverable: true,
          isDeliverable: true,
          isRisky: Boolean(payload.isRisky),
          isUndeliverable: false,
          isB2C: Boolean(payload.isB2C),
          status: 'secondary_valid_ignored',
          score,
          reason: 'person_already_has_primary_valid_email',
          createdAt: Date.now()
        });
      }
    } else {
      await this.invalidParquetWriter.append({
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
        deliverable: false,
        isDeliverable: false,
        isRisky: Boolean(payload.isRisky),
        isUndeliverable: Boolean(payload.isUndeliverable ?? true),
        isB2C: Boolean(payload.isB2C),
        status: payload.status || 'undeliverable',
        score,
        reason: payload.reason || payload.message || 'undeliverable',
        createdAt: Date.now()
      });
      this.metrics.webhookInvalidReceived++;

      // WATERFALL ESCALATION:
      // Since this pattern failed, escalate to the next pattern if available!
      const currentPatternIndex = typeof context.patternIndex === 'number' ? context.patternIndex : 0;
      const nextIndex = currentPatternIndex + 1;

      if (personId && !this.resolvedPersonIds.has(personId)) {
        const waterfallState = await this._getPersonWaterfallState(personId);
        if (waterfallState && waterfallState.missingCandidates && nextIndex < waterfallState.missingCandidates.length) {
          if (this.useMemoryQueue) {
            await this._processNextPatternJob({ personId, nextIndex });
          } else {
            // Push escalation job to Redis queue so ANY worker node can test Pattern #N
            try {
              await this.queue.add('verify_next_pattern', {
                personId,
                nextIndex
              }, {
                jobId: `escalate_${personId}_${nextIndex}`,
                priority: 1 // High priority so in-flight people resolve promptly
              });
            } catch (enqueueErr) {
              // Local fallback
              this._processNextPatternJob({ personId, nextIndex }).catch(() => {});
            }
          }
        } else if (waterfallState) {
          // All patterns exhausted
          await this._deletePersonWaterfallState(personId);
        }
      }
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
      // Force shutdown within 1.5 seconds so worker doesn't hang waiting for stalled jobs
      await Promise.race([
        this.worker.close(),
        new Promise(r => setTimeout(r, 1500))
      ]);
    }
    if (this.queue && typeof this.queue.close === 'function') {
      await Promise.race([
        this.queue.close(),
        new Promise(r => setTimeout(r, 1000))
      ]);
    }
    if (this.redisClient) {
      this.redisClient.disconnect(false);
    }

    // Flush all 3 clean Parquet files concurrently
    await Promise.all([
      this.validParquetWriter.close(),
      this.webhookTriggersParquetWriter.close(),
      this.invalidParquetWriter.close()
    ]);
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
      webhookTriggersParquet: this.webhookTriggersParquetWriter.getStats(),
      invalidParquet: this.invalidParquetWriter.getStats()
    };
  }
}
