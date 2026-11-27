# Verify V6 Waterfall Webhook Runner

Standalone, high-throughput Verify V6 webhook sender, receiver, and 3-stage Parquet writer (no MongoDB, Redis, or Elasticsearch dependencies).

## Flow
1. Streams all `.jsonl`/`.json` slice files from `INPUT_DIR` (`/root/no2bounce-webhook/validation/output`), excluding `all_verifications_unique.jsonl`.
2. Skips completed contacts (`primary_email` set or `email_smt_status` = `completed`/`valid`/`verified`) and already-checkpointed IDs (`data/processed_ids.log`).
3. Extracts only patterns with `score === null` and runs a strict waterfall:
   - Sends Pattern 1 to Verify V6 (`TARGET_RPS=3000`, `MAX_CONCURRENCY=1500`) with exponential backoff retries.
   - Logs every outbound request to `data/webhook_triggers/*.parquet`.
   - Receives callbacks on `0.0.0.0:3000/api/webhook/validation`.
   - On valid callback (`deliverable: true`, `score >= 90`): writes 1 row to `data/valid/*.parquet` and stops further patterns for that person.
   - On invalid callback: writes to `data/invalid/*.parquet` and immediately triggers the next `score === null` pattern.
4. Prints 1-second live stats (`Send/s`, `WebhookIn/s`, `Active`, `InFlight`, `EscalationsQ`, `Valid`, `Invalid`, `Skipped`, `Retries`, `Err`) and seals all `.parquet` files cleanly on `SIGINT`/`SIGTERM`.

## Run
```bash
npm install
cp .env.example .env
npm start
# Or with PM2:
npx pm2 start src/index.js --name "v6-runner"
```
