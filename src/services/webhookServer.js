/**
 * @file webhookServer.js
 * @description Dedicated, high-throughput Webhook Receiver HTTP Server.
 *              Can run on ANY server (Master or Worker) or standalone.
 *              - Listens for callbacks from verifyv6.no2bounce.com on POST /api/webhook/validation
 *              - Responds 200 OK immediately (< 5ms) to prevent timeout retries
 *              - Resolves context (personId, firstName, lastName, etc.) from Redis/Memory
 *              - Writes valid matches to valid Parquet file (Option A: First Valid Wins)
 *              - Writes invalid matches to failed_permanent Parquet file
 */

import http from 'http';
import { config } from '../config/index.js';
import { errorLogger } from '../utils/errorLogger.js';

export class WebhookReceiverServer {
  /**
   * @param {object} options
   * @param {number} [options.port]
   * @param {Function} [options.onValidationResult] - Callback processor
   */
  constructor(options = {}) {
    this.port = options.port || config.webhookValidation.serverPort;
    this.onValidationResult = options.onValidationResult || (async () => {});
    this.server = null;
    this.isRunning = false;
  }

  /**
   * Starts the HTTP webhook receiver
   */
  async start() {
    return new Promise((resolve, reject) => {
      this.server = http.createServer(async (req, res) => {
        // Accept POST /api/webhook/validation
        if (req.method === 'POST' && req.url.startsWith('/api/webhook/validation')) {
          let body = '';
          req.on('data', chunk => {
            body += chunk.toString();
          });

          req.on('end', async () => {
            // Respond 200 OK immediately to verifyv6
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ received: true }));

            try {
              const payload = JSON.parse(body || '{}');
              console.log(`[WebhookServer] Received callback from Verify V6: email=${payload.email || 'N/A'}, deliverable=${payload.isDeliverable}, score=${payload.score ?? 0}`);
              await this.onValidationResult(payload);
            } catch (err) {
              errorLogger.logError('WEBHOOK_PAYLOAD_PARSE_ERROR', err, { body });
            }
          });
        } else if (req.method === 'GET' && req.url === '/health') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ status: 'ok', serverId: config.serverId, timestamp: new Date() }));
        } else {
          res.writeHead(404);
          res.end();
        }
      });

      this.server.on('error', err => {
        errorLogger.logError('WEBHOOK_SERVER_PORT_ERROR', err, { port: this.port });
        reject(err);
      });

      this.server.listen(this.port, '0.0.0.0', () => {
        this.isRunning = true;
        console.log(`[WebhookServer] Listening cleanly on 0.0.0.0:${this.port} (POST /api/webhook/validation)`);
        resolve();
      });
    });
  }

  /**
   * Closes HTTP server cleanly
   */
  async stop() {
    if (this.server) {
      return new Promise(resolve => {
        this.server.close(() => {
          this.isRunning = false;
          console.log('[WebhookServer] Stopped cleanly.');
          resolve();
        });
      });
    }
  }
}
