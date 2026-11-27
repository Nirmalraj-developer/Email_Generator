import axios from 'axios';
import http from 'http';
import https from 'https';

/**
 * High-throughput Verify V6 HTTP Client with keep-alive pooling,
 * sliding-window rate pacing (default 3,000 req/s), and exponential backoff retries.
 */
export class V6Client {
  constructor({ url, apiToken, callbackUrl, targetRps = 3000, timeoutMs = 15000, maxRetries = 3, onRetry, onError }) {
    this.url = url;
    this.apiToken = apiToken;
    this.callbackUrl = callbackUrl;
    this.targetRps = targetRps;
    this.maxRetries = maxRetries;
    this.onRetry = onRetry;
    this.onError = onError;

    this.sentCount = 0;
    this.retryCount = 0;
    this.failedCount = 0;

    // 100ms sliding window rate limiter for smooth 3,000 req/s pacing
    this.windowMs = 100;
    this.maxPerWindow = Math.max(1, Math.ceil((targetRps * this.windowMs) / 1000));
    this.windowStart = Date.now();
    this.windowCount = 0;

    this.client = axios.create({
      timeout: timeoutMs,
      httpAgent: new http.Agent({ keepAlive: true, maxSockets: 500, maxFreeSockets: 256, family: 4 }),
      httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 500, maxFreeSockets: 256, family: 4 }),
      headers: {
        'Content-Type': 'application/json',
        ...(apiToken ? { apitoken: apiToken } : {})
      }
    });
  }

  async _pace() {
    if (this.targetRps <= 0) return;
    while (true) {
      const now = Date.now();
      if (now - this.windowStart >= this.windowMs) {
        this.windowStart = now;
        this.windowCount = 0;
      }
      if (this.windowCount < this.maxPerWindow) {
        this.windowCount++;
        return;
      }
      const waitMs = Math.max(1, this.windowMs - (now - this.windowStart));
      await new Promise(r => setTimeout(r, waitMs));
    }
  }

  async submit(email, trackingId) {
    let attempt = 0;
    while (attempt <= this.maxRetries) {
      await this._pace();
      try {
        const res = await this.client.post(this.url, {
          email,
          webhookUrl: this.callbackUrl
        });
        this.sentCount++;
        const data = res.data || {};
        return {
          success: true,
          trackingId: data.trackingId || data.tracking_id || data.id || trackingId
        };
      } catch (err) {
        const status = err.response?.status;
        const retryable = !status || status === 429 || status >= 500;
        if (retryable && attempt < this.maxRetries) {
          attempt++;
          this.retryCount++;
          if (this.onRetry) this.onRetry(email, attempt, err.message);
          await new Promise(r => setTimeout(r, Math.min(2000, 150 * Math.pow(2, attempt - 1))));
          continue;
        }
        this.failedCount++;
        if (this.onError) {
          this.onError('V6_SUBMIT_FAILED', err.message, { email, status });
        }
        return { success: false, error: err.message };
      }
    }
  }
}
