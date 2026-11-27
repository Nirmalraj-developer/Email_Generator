import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { config } from './config.js';
import { ErrorLogger } from './logger.js';
import { ChunkedJsonWriter } from './jsonWriter.js';
import { loadCheckpointSet, streamPendingContacts } from './sliceReader.js';
import { WebhookServer } from './webhookServer.js';
import { V6Client } from './v6Client.js';

export class V6WaterfallRunner {
  constructor(customConfig = {}) {
    this.cfg = {
      ...config,
      ...customConfig,
      v6: { ...config.v6, ...(customConfig.v6 || {}) },
      webhook: { ...config.webhook, ...(customConfig.webhook || {}) },
      output: { ...config.output, ...(customConfig.output || {}) }
    };

    this.errorLogger = new ErrorLogger(this.cfg.errorLogFile);

    this.validWriter = new ChunkedJsonWriter({
      outputDir: path.join(this.cfg.output.baseDir, 'valid'),
      filePrefix: 'valid_emails',
      maxCapacity: this.cfg.output.maxCapacity,
      serverId: this.cfg.serverId
    });

    this.triggerWriter = new ChunkedJsonWriter({
      outputDir: path.join(this.cfg.output.baseDir, 'webhook_triggers'),
      filePrefix: 'webhook_triggers',
      maxCapacity: this.cfg.output.maxCapacity,
      serverId: this.cfg.serverId
    });

    this.invalidWriter = new ChunkedJsonWriter({
      outputDir: path.join(this.cfg.output.baseDir, 'invalid'),
      filePrefix: 'invalid_emails',
      maxCapacity: this.cfg.output.maxCapacity,
      serverId: this.cfg.serverId
    });

    const callbackUrl = `${this.cfg.webhook.callbackBaseUrl}${this.cfg.webhook.path}`;
    this.v6Client = new V6Client({
      url: this.cfg.v6.url,
      apiToken: this.cfg.v6.apiToken,
      callbackUrl,
      targetRps: this.cfg.v6.targetRps,
      timeoutMs: this.cfg.v6.timeoutMs,
      maxRetries: this.cfg.v6.maxRetries,
      onRetry: (email, attempt, msg) => {
        this.errorLogger.log('V6_RETRY', msg, { email, attempt });
      },
      onError: (tag, msg, meta) => {
        this.errorLogger.log(tag, msg, meta);
      }
    });

    this.webhookServer = new WebhookServer({
      port: this.cfg.webhook.port,
      path: this.cfg.webhook.path,
      onCallback: (payload) => this._handleCallback(payload),
      onError: (tag, msg) => this.errorLogger.log(tag, msg)
    });

    this.processedSet = loadCheckpointSet(this.cfg.checkpointFile);
    const cpDir = path.dirname(this.cfg.checkpointFile);
    if (!fs.existsSync(cpDir)) fs.mkdirSync(cpDir, { recursive: true });
    this.checkpointStream = fs.createWriteStream(this.cfg.checkpointFile, { flags: 'a', encoding: 'utf8' });

    // Waterfall & in-flight state
    this.waterfallStates = new Map(); // personId -> { companyId, pendingPatterns, currentIndex }
    this.inFlightByTracking = new Map(); // trackingId -> { personId, companyId, pattern, email, patternIndex }
    this.inFlightByEmail = new Map(); // emailLower -> trackingId
    this.resolvedValid = new Set(); // Option A: first valid wins per person

    // Priority escalation queue (Pattern 2..10 retries jump ahead of new contacts)
    this.escalationQueue = [];
    this.activeSends = 0;
    this.isStopping = false;
    this.statsTimer = null;

    this.stats = {
      loadedContacts: 0,
      skippedCompleted: 0,
      validCount: 0,
      invalidCount: 0,
      triggersWritten: 0
    };
  }

  _genTrackingId(personId, idx) {
    return `trk_${personId}_${idx}_${crypto.randomBytes(3).toString('hex')}`;
  }

  _markPersonDone(personId) {
    if (!this.processedSet.has(personId)) {
      this.processedSet.add(personId);
      this.checkpointStream.write(personId + '\n');
    }
    this.waterfallStates.delete(personId);
  }

  async _dispatchPattern(personId, patternIndex) {
    const state = this.waterfallStates.get(personId);
    if (!state || this.resolvedValid.has(personId)) {
      this.waterfallStates.delete(personId);
      return;
    }

    if (patternIndex >= state.pendingPatterns.length) {
      this._markPersonDone(personId);
      return;
    }

    const candidate = state.pendingPatterns[patternIndex];
    state.currentIndex = patternIndex;
    const localTrackingId = this._genTrackingId(personId, patternIndex);

    this.activeSends++;
    try {
      const res = await this.v6Client.submit(candidate.email, localTrackingId);
      if (!res.success) {
        // Record failed submission and escalate to next pattern if available
        await this.invalidWriter.append({
          trackingId: localTrackingId,
          personId,
          companyId: state.companyId || '',
          pattern: candidate.pattern,
          email: candidate.email,
          score: 0,
          deliverable: false,
          reason: `submit_failed: ${res.error}`,
          createdAt: Date.now()
        });
        this.stats.invalidCount++;

        const nextIdx = patternIndex + 1;
        if (nextIdx < state.pendingPatterns.length && !this.isStopping) {
          this.escalationQueue.push({ personId, patternIndex: nextIdx });
        } else {
          this._markPersonDone(personId);
        }
        return;
      }

      const trackingId = res.trackingId || localTrackingId;
      const tracker = {
        personId,
        companyId: state.companyId || '',
        pattern: candidate.pattern,
        email: candidate.email,
        patternIndex
      };
      this.inFlightByTracking.set(trackingId, tracker);
      this.inFlightByEmail.set(candidate.email.toLowerCase(), trackingId);

      await this.triggerWriter.append({
        trackingId,
        personId,
        companyId: state.companyId || '',
        pattern: candidate.pattern,
        email: candidate.email,
        patternIndex,
        status: 'triggered_waiting_callback',
        createdAt: Date.now()
      });
      this.stats.triggersWritten++;
    } finally {
      this.activeSends--;
    }
  }

  async _handleCallback(payload) {
    const trackingId = payload.trackingId || payload.tracking_id || payload.id || '';
    const email = String(payload.email || '').trim();
    const emailKey = email.toLowerCase();

    const resolvedTrackingId = (trackingId && this.inFlightByTracking.has(trackingId))
      ? trackingId
      : this.inFlightByEmail.get(emailKey);

    const tracker = resolvedTrackingId ? this.inFlightByTracking.get(resolvedTrackingId) : null;
    if (resolvedTrackingId) {
      this.inFlightByTracking.delete(resolvedTrackingId);
    }
    if (emailKey) {
      this.inFlightByEmail.delete(emailKey);
    }

    const personId = tracker?.personId || String(payload.personId || payload.person_id || 'unknown');
    const companyId = tracker?.companyId || '';
    const pattern = tracker?.pattern || '';
    const patternIndex = tracker?.patternIndex ?? 0;

    const score = Number(payload.score ?? 0);
    const deliverable = Boolean(payload.deliverable ?? payload.isDeliverable ?? (score >= 90));

    if (deliverable && score >= 90) {
      if (!this.resolvedValid.has(personId)) {
        this.resolvedValid.add(personId);
        await this.validWriter.append({
          trackingId: resolvedTrackingId || trackingId || '',
          personId,
          companyId,
          pattern,
          email: email || tracker?.email || '',
          score,
          deliverable: true,
          isRisky: Boolean(payload.isRisky),
          isB2C: Boolean(payload.isB2C),
          createdAt: Date.now()
        });
        this.stats.validCount++;
      }
      this._markPersonDone(personId);
      return;
    }

    // Invalid callback -> write to invalid parquet and escalate to Pattern N+1
    await this.invalidWriter.append({
      trackingId: resolvedTrackingId || trackingId || '',
      personId,
      companyId,
      pattern,
      email: email || tracker?.email || '',
      score,
      deliverable: false,
      reason: String(payload.reason || payload.message || 'undeliverable'),
      createdAt: Date.now()
    });
    this.stats.invalidCount++;

    const state = this.waterfallStates.get(personId);
    const nextIdx = patternIndex + 1;
    if (state && !this.resolvedValid.has(personId) && nextIdx < state.pendingPatterns.length && !this.isStopping) {
      this.escalationQueue.push({ personId, patternIndex: nextIdx });
    } else if (personId !== 'unknown') {
      this._markPersonDone(personId);
    }
  }

  _startStatsLogger() {
    let prevSent = 0;
    let prevIn = 0;
    let prevTime = Date.now();

    this.statsTimer = setInterval(() => {
      const now = Date.now();
      const elapsedSec = Math.max(0.1, (now - prevTime) / 1000);
      const sentRate = Math.round((this.v6Client.sentCount - prevSent) / elapsedSec);
      const inRate = Math.round((this.webhookServer.receivedCount - prevIn) / elapsedSec);

      prevSent = this.v6Client.sentCount;
      prevIn = this.webhookServer.receivedCount;
      prevTime = now;

      console.log(
        `[${new Date().toLocaleTimeString()}] ` +
        `Send/s: ${sentRate.toLocaleString()} | ` +
        `WebhookIn/s: ${inRate.toLocaleString()} | ` +
        `Active: ${this.activeSends} | ` +
        `InFlight: ${this.inFlightByTracking.size.toLocaleString()} | ` +
        `EscalationsQ: ${this.escalationQueue.length} | ` +
        `Valid: ${this.stats.validCount.toLocaleString()} | ` +
        `Invalid: ${this.stats.invalidCount.toLocaleString()} | ` +
        `Skipped: ${this.stats.skippedCompleted.toLocaleString()} | ` +
        `Retries: ${this.v6Client.retryCount} | ` +
        `Err: ${this.errorLogger.errorCount}`
      );
    }, this.cfg.statsIntervalMs);
  }

  async start({ waitForInFlightMs = 120000 } = {}) {
    await this.webhookServer.start();
    console.log(`[WebhookServer] Listening on 0.0.0.0:${this.cfg.webhook.port}${this.cfg.webhook.path}`);
    console.log(`[Runner] Reading slices from: ${this.cfg.inputDir} (Target: ${this.cfg.v6.targetRps} req/s, MaxConcurrency: ${this.cfg.v6.maxConcurrency})`);

    this._startStatsLogger();

    const inFlightPromises = new Set();
    const launchTask = (fn) => {
      const p = fn().finally(() => inFlightPromises.delete(p));
      inFlightPromises.add(p);
    };

    const drainEscalations = async () => {
      while (this.escalationQueue.length > 0 && !this.isStopping) {
        if (inFlightPromises.size >= this.cfg.v6.maxConcurrency) {
          await new Promise(r => setTimeout(r, 5));
          continue;
        }
        const job = this.escalationQueue.shift();
        launchTask(() => this._dispatchPattern(job.personId, job.patternIndex));
      }
    };

    const contactStream = streamPendingContacts({
      inputDir: this.cfg.inputDir,
      excludedFiles: this.cfg.excludedFiles,
      processedSet: this.processedSet,
      shouldStop: () => this.isStopping,
      onSkipped: () => { this.stats.skippedCompleted++; }
    });

    for await (const contact of contactStream) {
      if (this.isStopping) break;
      if (this.stats.loadedContacts >= this.cfg.maxContacts) break;

      await drainEscalations();

      while (inFlightPromises.size >= this.cfg.v6.maxConcurrency && !this.isStopping) {
        await new Promise(r => setTimeout(r, 5));
        await drainEscalations();
      }
      if (this.isStopping) break;

      this.stats.loadedContacts++;
      this.waterfallStates.set(contact.personId, {
        companyId: contact.companyId,
        pendingPatterns: contact.pendingPatterns,
        currentIndex: 0
      });

      launchTask(() => this._dispatchPattern(contact.personId, 0));
    }

    // Wait for remaining escalations & in-flight callbacks after slices finish
    const deadline = Date.now() + waitForInFlightMs;
    while (!this.isStopping && (inFlightPromises.size > 0 || this.escalationQueue.length > 0 || this.inFlightByTracking.size > 0)) {
      await drainEscalations();
      if (Date.now() > deadline && this.escalationQueue.length === 0 && inFlightPromises.size === 0) break;
      await new Promise(r => setTimeout(r, 50));
    }
  }

  async safeStop() {
    if (this.isStopping) return;
    this.isStopping = true;
    console.log('\n🛑 [SafeStop] Stopping ingestion and flushing JSONL files...');

    if (this.statsTimer) clearInterval(this.statsTimer);

    // Wait briefly for in-flight HTTP requests to settle
    const waitStart = Date.now();
    while (this.activeSends > 0 && Date.now() - waitStart < 3000) {
      await new Promise(r => setTimeout(r, 50));
    }

    await this.webhookServer.stop();
    await Promise.all([
      this.validWriter.close(),
      this.triggerWriter.close(),
      this.invalidWriter.close()
    ]);

    await new Promise(resolve => this.checkpointStream.end(resolve));
    await this.errorLogger.close();
    console.log(
      `✅ [SafeStop] All JSONL files flushed and closed cleanly. ` +
      `Valid=${this.stats.validCount}, Invalid=${this.stats.invalidCount}, Triggers=${this.stats.triggersWritten}`
    );
  }
}
