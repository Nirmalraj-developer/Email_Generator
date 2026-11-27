/**
 * @file sourceMongoReader.js
 * @description Direct Single-Collection MongoDB Reader:
 *              - Reads directly from: dg_data.es_cont_email
 *              - Retrieves: first_name, last_name, company_name, company_id, and website
 *              - NO cross-collection lookup needed (huge performance boost for 187M scale)
 *              - Supports _id cursor pagination for continuous stream (Patch Continue)
 */

import { MongoClient, Long, ObjectId } from 'mongodb';
import { config } from '../config/index.js';

export class SourceMongoReader {
  /**
   * @param {object} [options]
   * @param {string} [options.uri]
   * @param {number} [options.batchSize]
   */
  constructor(options = {}) {
    this.uri = options.uri || config.sourceMongo.uri;
    this.batchSize = options.batchSize || config.sourceMongo.batchSize;
    this.peopleDbName = config.sourceMongo.peopleDb;
    this.peopleColName = config.sourceMongo.peopleCollection;

    this.client = null;
    this.peopleCol = null;
    this.idType = null;
  }

  /**
   * Connects to Source MongoDB collection (read-only) with high-throughput connection pooling.
   */
  async connect() {
    this.client = new MongoClient(this.uri, {
      maxPoolSize: 50,
      minPoolSize: 10,
      serverSelectionTimeoutMS: 15000,
      socketTimeoutMS: 180000
    });
    await this.client.connect();
    this.peopleCol = this.client.db(this.peopleDbName).collection(this.peopleColName);

    // Detect exact BSON type of _id (Long, ObjectId, Number, String)
    const sample = await this.peopleCol.findOne({}, { projection: { _id: 1 } });
    if (sample) {
      if (typeof sample._id === 'object' && Long.isLong(sample._id)) {
        this.idType = 'Long';
      } else if (sample._id instanceof ObjectId) {
        this.idType = 'ObjectId';
      } else if (typeof sample._id === 'number') {
        this.idType = 'Number';
      } else if (typeof sample._id === 'bigint') {
        this.idType = 'BigInt';
      } else {
        this.idType = 'String';
      }
      console.log(`[SourceMongoReader] Detected _id BSON type: ${this.idType}`);
    }
  }

  /**
   * Closes connection.
   */
  async disconnect() {
    if (this.client) {
      await this.client.close();
      this.client = null;
    }
  }

  /**
   * Fetches a batch of people where:
   *   - first_name exists and is non-empty
   *   - website exists and is non-empty
   *   - _id > lastId (indexed cursor pagination)
   *
   * @param {string|null} [lastId=null] - Last processed person _id
   * @returns {Promise<{
   *   records: Array<{
   *     id: string,
   *     firstName: string,
   *     lastName: string,
   *     companyId: string|null,
   *     companyName: string,
   *     website: string,
   *     rawSource: object
   *   }>,
   *   nextLastId: string|null,
   *   batchSize: number
   * }>}
   */
  async fetchBatch(lastId = null, customLimit = null) {
    const fetchLimit = (customLimit && customLimit > 0) ? Math.min(this.batchSize, customLimit) : this.batchSize;
    const { query } = this._buildQuery(lastId);

    const startMs = Date.now();
    console.log(`[SourceMongoReader] Querying MongoDB batch (limit: ${fetchLimit}, lastId: ${lastId || '0'})...`);

    const peopleDocs = await this.peopleCol
      .find(query, {
        projection: {
          _id: 1,
          first_name: 1,
          last_name: 1,
          company_id: 1,
          company_name: 1,
          company: 1,
          website: 1,
          domain: 1,
          company_website: 1,
          website_url: 1
        }
      })
      .sort({ _id: 1 })
      .batchSize(fetchLimit)
      .limit(fetchLimit)
      .toArray();

    console.log(`[SourceMongoReader] Retrieved ${peopleDocs.length} documents from MongoDB in ${Date.now() - startMs}ms`);

    if (peopleDocs.length === 0) {
      return { records: [], nextLastId: null, batchSize: 0 };
    }

    // Always require valid website & firstName regardless of env
    const records = peopleDocs
      .map(person => this._mapPerson(person, true))
      .filter(Boolean);

    const nextLastId = String(peopleDocs[peopleDocs.length - 1]._id);

    return {
      records,
      nextLastId,
      batchSize: peopleDocs.length
    };
  }

  /**
   * Builds the MongoDB query with website non-empty string filter by default
   * (uses website_1 index + _id cursor pagination, ignoring SOURCE_WEBSITE_QUERY_MODE env).
   */
  _buildQuery(lastId = null) {
    const query = {
      website: { $gt: '' }
    };

    if (lastId) {
      let queryId = lastId;
      if (this.idType === 'Long' || (this.idType === null && /^\d+$/.test(String(lastId)))) {
        try {
          queryId = Long.fromString(String(lastId));
        } catch (_) {
          queryId = lastId;
        }
      } else if (this.idType === 'ObjectId') {
        queryId = new ObjectId(String(lastId));
      } else if (this.idType === 'Number') {
        queryId = Number(lastId);
      } else if (this.idType === 'BigInt') {
        queryId = BigInt(String(lastId));
      }
      query._id = { $gt: queryId };
    }
    return { query };
  }

  /**
   * Maps a raw MongoDB document into a worker contact record (or null if filtered out).
   */
  _mapPerson(person, filterWebsiteOnly) {
    let websiteVal = person.website;
    if (typeof websiteVal !== 'string' || !websiteVal) {
      websiteVal = person.domain || person.company_website || person.website_url || person.company || '';
    }
    const cleanWebsite = String(websiteVal || '').trim();
    const firstName = person.first_name || '';

    if (filterWebsiteOnly) {
      if (!cleanWebsite || !cleanWebsite.includes('.') || !firstName.trim()) {
        return null;
      }
    }

    return {
      id: String(person._id),
      firstName,
      lastName: person.last_name || '',
      companyId: person.company_id ? String(person.company_id) : null,
      companyName: person.company_name || person.company || '',
      website: cleanWebsite,
      rawSource: person
    };
  }

  /**
   * Continuous Streaming Cursor:
   * Keeps a single MongoDB B-tree cursor open and yields chunks of `chunkSize` valid contacts
   * immediately as they arrive (no 55s .toArray() stall), keeping workers stocked seamlessly.
   */
  async streamContacts(startLastId = null, { chunkSize = 1000, maxRecords = Infinity, onChunk, shouldStop } = {}) {
    const { query } = this._buildQuery(startLastId);
    const filterWebsiteOnly = true;

    const cursor = this.peopleCol
      .find(query, {
        projection: {
          _id: 1,
          first_name: 1,
          last_name: 1,
          company_id: 1,
          company_name: 1,
          company: 1,
          website: 1,
          domain: 1,
          company_website: 1,
          website_url: 1
        }
      })
      .sort({ _id: 1 })
      .batchSize(2000);

    let buffer = [];
    let rawScanned = 0;
    let totalYielded = 0;
    let currentLastId = startLastId;

    try {
      for await (const person of cursor) {
        if (shouldStop && shouldStop()) break;

        currentLastId = String(person._id);
        rawScanned++;

        const mapped = this._mapPerson(person, filterWebsiteOnly);
        if (mapped) {
          buffer.push(mapped);
        }

        const remaining = maxRecords - totalYielded;
        if (buffer.length >= chunkSize || buffer.length >= remaining || rawScanned >= 10000) {
          const toEmit = buffer.slice(0, remaining);
          totalYielded += toEmit.length;
          await onChunk({ records: toEmit, lastId: currentLastId, rawScanned });
          buffer = [];
          rawScanned = 0;

          if (totalYielded >= maxRecords) break;
        }
      }

      // Flush any trailing records at end of collection
      if (buffer.length > 0 && totalYielded < maxRecords && !(shouldStop && shouldStop())) {
        const remaining = maxRecords - totalYielded;
        const toEmit = buffer.slice(0, remaining);
        await onChunk({ records: toEmit, lastId: currentLastId, rawScanned });
      } else if (rawScanned > 0 && !(shouldStop && shouldStop())) {
        await onChunk({ records: [], lastId: currentLastId, rawScanned });
      }
    } finally {
      await cursor.close().catch(() => {});
    }

    return { lastId: currentLastId };
  }
}
