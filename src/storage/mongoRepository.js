/**
 * @file mongoRepository.js
 * @description Dedicated MongoDB repository for storing:
 *              - Valid emails in 'valid_emails' (with trackingId)
 *              - Invalid emails in 'invalid_emails' (with trackingId and clear distinct statuses:
 *                  - 'all_patterns_cached_invalid': all 10 patterns were found in cache with low score/undeliverable
 *                  - 'n2b_validated_invalid': missing emails were sent to N2B validation and confirmed invalid
 *                  - 'pending_n2b_batch': sent or queued for N2B batch
 *              - Checkpoints in 'sync_checkpoints'
 */

import { MongoClient } from 'mongodb';
import { config } from '../config/index.js';

export class MongoRepository {
  constructor() {
    this.uri = config.destinationMongo.uri;
    this.dbName = config.destinationMongo.dbName;
    this.validColName = config.destinationMongo.validCollection;
    this.invalidColName = config.destinationMongo.invalidCollection;
    this.checkpointColName = config.destinationMongo.checkpointCollection;
    
    this.client = null;
    this.db = null;
  }

  /**
   * Connects to Destination MongoDB and sets up unique indexes for fast lookups.
   */
  async connect() {
    this.client = new MongoClient(this.uri);
    await this.client.connect();
    this.db = this.client.db(this.dbName);
    // Note: No indexing is performed in MongoDB per requirement
  }

  /**
   * Closes MongoDB connection.
   */
  async disconnect() {
    if (this.client) {
      await this.client.close();
      this.client = null;
    }
  }

  /**
   * Saves valid email result to 'valid_emails' collection.
   *
   * @param {object} record
   * @returns {Promise<{ success: boolean, result: object }>}
   */
  async saveValidEmail(record) {
    const doc = {
      trackingId: record.trackingId,
      personId: record.personId,
      firstName: record.firstName,
      lastName: record.lastName,
      companyId: record.companyId || null,
      companyName: record.companyName || null,
      website: record.website,
      domain: record.domain,
      pattern: record.pattern,
      email: record.email,
      
      // Source & score
      source: record.source || 'cache', // 'cache' | 'n2b_validation'
      inCache: record.cacheResponse?.inCache ?? true,
      score: record.cacheResponse?.score ?? record.score ?? 99,
      app: record.cacheResponse?.app ?? null,
      appName: record.cacheResponse?.appName ?? null,
      isDeliverable: record.cacheResponse?.isDeliverable ?? true,
      isRisky: record.cacheResponse?.isRisky ?? false,
      isUndeliverable: record.cacheResponse?.isUndeliverable ?? false,
      isB2C: record.cacheResponse?.isB2C ?? false,
      cacheUpdatedAt: record.cacheResponse?.updatedAt ? new Date(record.cacheResponse.updatedAt) : null,
      
      // Full raw response payload
      rawResponse: record.cacheResponse || record.rawResponse || null,
      
      createdAt: new Date(),
      updatedAt: new Date()
    };

    const collection = this.db.collection(this.validColName);
    const result = await collection.updateOne(
      { email: doc.email },
      { $set: doc },
      { upsert: true }
    );

    return { success: true, result };
  }

  /**
   * Saves individual invalid email record to 'invalid_emails' collection.
   * Keeps it as a separate record with trackingId and distinct status.
   *
   * @param {object} record
   * @returns {Promise<{ success: boolean, result: object }>}
   */
  async saveInvalidEmail(record) {
    const doc = {
      trackingId: record.trackingId,
      personId: record.personId,
      firstName: record.firstName,
      lastName: record.lastName,
      companyId: record.companyId || null,
      companyName: record.companyName || null,
      website: record.website,
      domain: record.domain,
      email: record.email || null,
      pattern: record.pattern || null,
      
      // Distinct Status Names:
      // - 'all_patterns_cached_invalid': All 10 patterns were already in cache with low score
      // - 'n2b_validated_invalid': Sent to N2B validation and confirmed invalid
      // - 'cache_miss_pending_n2b': Missing in cache, pending/sent to N2B validation
      status: record.status,
      
      score: record.score ?? null,
      inCache: record.inCache ?? false,
      rawResponse: record.rawResponse || null,
      reason: record.reason || null,
      
      createdAt: new Date(),
      updatedAt: new Date()
    };

    const collection = this.db.collection(this.invalidColName);
    
    // If email is present, upsert by (personId, email); otherwise upsert by (personId, status)
    const filter = doc.email 
      ? { personId: doc.personId, email: doc.email } 
      : { personId: doc.personId, status: doc.status };

    const result = await collection.updateOne(
      filter,
      { $set: doc },
      { upsert: true }
    );

    return { success: true, result };
  }

  /**
   * Batch save multiple invalid email records
   *
   * @param {Array<object>} records
   */
  async saveInvalidEmailsBatch(records = []) {
    if (!records || records.length === 0) return;
    for (const record of records) {
      await this.saveInvalidEmail(record);
    }
  }

  /**
   * Retrieves synchronization checkpoint for patch-continue capability.
   *
   * @param {string} [key='mongo_stage1_sync']
   * @returns {Promise<object|null>}
   */
  async getCheckpoint(key = 'mongo_stage1_sync') {
    const collection = this.db.collection(this.checkpointColName);
    return await collection.findOne({ key });
  }

  /**
   * Updates or creates synchronization checkpoint.
   *
   * @param {string} key
   * @param {object} data
   * @returns {Promise<object>}
   */
  async saveCheckpoint(key = 'mongo_stage1_sync', data = {}) {
    const doc = {
      key,
      ...data,
      updatedAt: new Date()
    };

    const collection = this.db.collection(this.checkpointColName);
    await collection.updateOne(
      { key },
      { $set: doc },
      { upsert: true }
    );
    return doc;
  }

  /**
   * Retrieves collection counts for monitoring.
   *
   * @returns {Promise<{
   *   totalValid: number,
   *   allPatternsCachedInvalid: number,
   *   n2bValidatedInvalid: number
   * }>}
   */
  async getStats() {
    const validCount = await this.db.collection(this.validColName).countDocuments();
    const allCachedInvalidCount = await this.db.collection(this.invalidColName).countDocuments({ status: 'all_patterns_cached_invalid' });
    const n2bInvalidCount = await this.db.collection(this.invalidColName).countDocuments({ status: 'n2b_validated_invalid' });
    
    return {
      totalValid: validCount,
      allPatternsCachedInvalid: allCachedInvalidCount,
      n2bValidatedInvalid: n2bInvalidCount
    };
  }
}
