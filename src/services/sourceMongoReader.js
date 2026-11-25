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
  }

  /**
   * Connects to Source MongoDB collection (read-only).
   */
  async connect() {
    this.client = new MongoClient(this.uri);
    await this.client.connect();
    this.peopleCol = this.client.db(this.peopleDbName).collection(this.peopleColName);
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
  async fetchBatch(lastId = null) {
    const query = {
      first_name: { $exists: true, $nin: ['', null] },
      website: { $exists: true, $nin: ['', null] }
    };

    if (lastId) {
      let queryId = lastId;
      if (typeof lastId === 'string') {
        if (/^\d+$/.test(lastId)) {
          // MongoDB Long (64-bit integer) format
          queryId = Long.fromString(lastId);
        } else if (ObjectId.isValid(lastId) && lastId.length === 24) {
          queryId = new ObjectId(lastId);
        }
      }
      query._id = { $gt: queryId };
    }

    const peopleDocs = await this.peopleCol
      .find(query)
      .sort({ _id: 1 })
      .limit(this.batchSize)
      .toArray();

    if (peopleDocs.length === 0) {
      return { records: [], nextLastId: null, batchSize: 0 };
    }

    // Direct mapping - no extra collection lookup required!
    const records = peopleDocs.map(person => ({
      id: String(person._id),
      firstName: person.first_name || '',
      lastName: person.last_name || '',
      companyId: person.company_id ? String(person.company_id) : null,
      companyName: person.company_name || person.company || '',
      website: person.website,
      rawSource: person
    }));

    const nextLastId = String(peopleDocs[peopleDocs.length - 1]._id);

    return {
      records,
      nextLastId,
      batchSize: peopleDocs.length
    };
  }
}
