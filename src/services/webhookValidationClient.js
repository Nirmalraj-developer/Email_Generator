/**
 * @file webhookValidationClient.js
 * @description HTTP client for No2Bounce V6 Webhook validation.
 *              Target: POST https://verifyv6.no2bounce.com/api/v2/single-webhook-validation
 */

import axios from 'axios';
import http from 'http';
import https from 'https';
import { config } from '../config/index.js';
import { errorLogger } from '../utils/errorLogger.js';

export class WebhookValidationClient {
  /**
   * @param {string} [baseUrl]
   * @param {string} [apiToken]
   * @param {string} [callbackBaseUrl]
   */
  constructor(
    baseUrl = config.webhookValidation.url,
    apiToken = config.webhookValidation.apiToken,
    callbackBaseUrl = config.webhookValidation.callbackBaseUrl
  ) {
    this.baseUrl = baseUrl;
    this.apiToken = apiToken;
    this.callbackBaseUrl = callbackBaseUrl.replace(/\/+$/, '');

    const httpAgent = new http.Agent({ keepAlive: true, maxSockets: 200 });
    const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 200 });

    this.client = axios.create({
      timeout: 10000,
      httpAgent,
      httpsAgent,
      headers: {
        'Content-Type': 'application/json',
        'apitoken': this.apiToken
      }
    });
  }

  /**
   * Submits a candidate email to Verify V6 with the server's webhook callback URL.
   *
   * @param {string} email
   * @returns {Promise<{
   *   email: string,
   *   success: boolean,
   *   trackingId: string|null,
   *   message: string,
   *   rawResponse: object|null,
   *   error?: string
   * }>}
   */
  async submitForValidation(email) {
    try {
      // If callbackBaseUrl contains a full path (e.g. webhook.site or custom endpoint), use it directly.
      // Otherwise, append the default /api/webhook/validation endpoint.
      const urlParts = this.callbackBaseUrl.split('/');
      const webhookUrl = (urlParts.length > 3 || this.callbackBaseUrl.includes('webhook.site'))
        ? this.callbackBaseUrl
        : `${this.callbackBaseUrl}/api/webhook/validation`;

      const payload = {
        email,
        webhookUrl
      };

      const response = await this.client.post(this.baseUrl, payload);
      const data = response.data || {};

      return {
        email,
        success: Boolean(data.success),
        trackingId: data.trackingId || null,
        message: data.message || 'Request accepted for real-time validation',
        rawResponse: data
      };
    } catch (error) {
      errorLogger.logError('VERIFYV6_WEBHOOK_SUBMIT_ERROR', error, { email });
      return {
        email,
        success: false,
        trackingId: null,
        message: error.message,
        rawResponse: error.response?.data || null,
        error: error.message
      };
    }
  }
}
