/**
 * @file cacheValidationClient.js
 * @description High-throughput HTTP client for querying the email cache endpoint.
 *              Uses persistent HTTP Keep-Alive connection pooling for 400+ req/sec.
 */

import axios from 'axios';
import http from 'http';
import https from 'https';
import { config } from '../config/index.js';
import { errorLogger } from '../utils/errorLogger.js';

export class CacheValidationClient {
  /**
   * @param {string} [baseUrl] - Cache endpoint URL.
   * @param {string} [token]   - Bearer authorization token.
   */
  constructor(baseUrl = config.cacheValidation.url, token = config.cacheValidation.token) {
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    // Normalize token (strip leading 'Bearer ' if provided to avoid 'Bearer Bearer ...')
    this.token = (token || '').replace(/^Bearer\s+/i, '').trim();

    // High performance connection pooling (re-uses TCP sockets across requests)
    const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 200, maxFreeSockets: 50 });
    const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 200, maxFreeSockets: 50 });

    this.client = axios.create({
      timeout: 10000,
      httpAgent,
      httpsAgent,
      headers: {
        'Authorization': `Bearer ${this.token}`,
        'Accept': 'application/json'
      }
    });
  }

  /**
   * Queries the cache endpoint for an email address.
   *
   * @param {string} email - The email candidate to check.
   * @returns {Promise<{
   *   source: string,
   *   email: string,
   *   checked: boolean,
   *   foundInCache: boolean,
   *   deliverable: boolean,
   *   isValid: boolean,
   *   cacheResponse: object|null,
   *   error?: string
   * }>}
   */
  async checkCache(email) {
    try {
      const encodedEmail = encodeURIComponent(email);
      const url = `${this.baseUrl}/${encodedEmail}`;
      const response = await this.client.get(url);
      
      const data = response.data;
      const inCache = Boolean(data?.inCache);
      const isDeliverable = Boolean(data?.isDeliverable);
      const score = Number(data?.score ?? 0);
      const isValid = inCache && isDeliverable && (score >= 90 || score === 99);

      return {
        source: 'cache',
        email,
        checked: true,
        foundInCache: inCache,
        deliverable: isDeliverable,
        isValid,
        score,
        cacheResponse: data
      };
    } catch (error) {
      // Handle rate limit 429 if the upstream ever throttles unexpectedly
      if (error.response && error.response.status === 429) {
        errorLogger.logError('CACHE_API_RATE_LIMIT_429', 'Rate limit encountered, backing off 1s', { email });
        await new Promise(r => setTimeout(r, 1000)); // Brief 1s backoff for safety
      }

      if (error.response && (error.response.status === 404 || error.response.status === 204)) {
        return {
          source: 'cache',
          email,
          checked: true,
          foundInCache: false,
          deliverable: false,
          isValid: false,
          cacheResponse: error.response?.data || null
        };
      }
      
      errorLogger.logError('CACHE_API_NETWORK_ERROR', error, { email });
      return {
        source: 'cache',
        email,
        checked: false,
        foundInCache: false,
        deliverable: false,
        isValid: false,
        error: error.message,
        cacheResponse: null
      };
    }
  }
}
