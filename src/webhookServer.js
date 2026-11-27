import http from 'http';

export class WebhookServer {
  constructor({ port = 3000, path = '/api/webhook/validation', onCallback, onError }) {
    this.port = port;
    this.path = path;
    this.onCallback = onCallback;
    this.onError = onError;
    this.server = null;
    this.receivedCount = 0;
  }

  start() {
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => {
        if (req.method === 'GET' && req.url === '/health') {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end('{"status":"ok"}');
          return;
        }

        const reqPath = (req.url || '').split('?')[0];
        if (req.method !== 'POST' || reqPath !== this.path) {
          res.writeHead(404, { 'Content-Type': 'application/json' });
          res.end('{"error":"not_found"}');
          return;
        }

        const chunks = [];
        req.on('data', chunk => chunks.push(chunk));
        req.on('end', () => {
          // Respond 200 OK immediately so Verify V6 never times out
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end('{"received":true}');

          try {
            const raw = Buffer.concat(chunks).toString('utf8');
            if (!raw) return;
            const payload = JSON.parse(raw);
            this.receivedCount++;
            if (this.onCallback) {
              Promise.resolve(this.onCallback(payload)).catch(err => {
                if (this.onError) this.onError('WEBHOOK_HANDLER_ERROR', err.message);
              });
            }
          } catch (err) {
            if (this.onError) this.onError('WEBHOOK_PARSE_ERROR', err.message);
          }
        });

        req.on('error', (err) => {
          if (this.onError) this.onError('WEBHOOK_REQ_ERROR', err.message);
        });
      });

      this.server.keepAliveTimeout = 65000;
      this.server.headersTimeout = 66000;
      this.server.maxConnections = 10000;

      this.server.on('error', reject);
      this.server.listen(this.port, '0.0.0.0', () => {
        resolve();
      });
    });
  }

  stop() {
    return new Promise((resolve) => {
      if (!this.server) return resolve();
      this.server.close(() => resolve());
      setTimeout(resolve, 1500).unref();
    });
  }
}
