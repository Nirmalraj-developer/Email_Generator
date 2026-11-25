# Process 1: Email Generation & First-Tier Verification Engine

Production cluster deployment guide for running **Process 1** across 3 servers to hit **150M records in 5 days**.

---

## 1. Process 1 Overview

Process 1 executes the primary high-speed flow:
1. Streams people with `website` directly from `dg_data.es_cont_email`.
2. Generates the 10 canonical email permutations.
3. Tests patterns against `GET https://cachevalidation.no2bounce.com/api/cache/check/{email}` (one by one with early stopping).
4. If deliverable (`score >= 90` or `score: 99`) $\rightarrow$ writes to `data/valid/`.
5. If all 10 patterns failed in cache $\rightarrow$ writes single person record to `data/invalid/permanent_invalid_cache/` (ready for Process 2 later).
6. If patterns are missing in cache (`inCache: false`) $\rightarrow$ submits to `https://verifyv6.no2bounce.com/api/v2/single-webhook-validation` and writes to `data/invalid/waiting_webhook_v6/`.
7. Listens for asynchronous Verify V6 webhooks $\rightarrow$ Option A (first deliverable wins) written to `data/valid/`.

---

## 2. Server Cluster Setup (3 Nodes)

### Server 1: Master Node (Streams DB, Allocates, Works)
1. Configure `.env`:
   ```env
   NODE_ROLE=master
   SERVER_ID=server_1_master

   # Master MongoDB source
   SOURCE_MONGODB_URI=mongodb://localhost:27017
   SOURCE_PEOPLE_DB=dg_data
   SOURCE_PEOPLE_COLLECTION=es_cont_email
   SOURCE_BATCH_SIZE=500

   # Redis accessible by Server 2 & Server 3
   REDIS_HOST=10.0.0.1
   REDIS_PORT=6379

   # Cache & Webhook API
   CACHE_VALIDATION_TOKEN=your_token_here
   WEBHOOK_API_TOKEN=your_token_here
   WEBHOOK_CALLBACK_BASE_URL=http://10.0.0.1:3000
   WEBHOOK_SERVER_PORT=3000

   WORKER_CONCURRENCY=60
   ```
2. Start Server 1:
   ```bash
   npm start
   ```

---

### Server 2: Worker Node (Listens & Works Only)
1. Configure `.env`:
   ```env
   NODE_ROLE=worker
   SERVER_ID=server_2_worker

   # Points to Server 1 Redis
   REDIS_HOST=10.0.0.1
   REDIS_PORT=6379

   # Cache & Webhook API
   CACHE_VALIDATION_TOKEN=your_token_here
   WEBHOOK_API_TOKEN=your_token_here
   WEBHOOK_CALLBACK_BASE_URL=http://10.0.0.2:3000
   WEBHOOK_SERVER_PORT=3000

   WORKER_CONCURRENCY=60
   ```
2. Start Server 2:
   ```bash
   npm run worker
   ```

---

### Server 3: Worker Node (Listens & Works Only)
1. Configure `.env`:
   ```env
   NODE_ROLE=worker
   SERVER_ID=server_3_worker

   # Points to Server 1 Redis
   REDIS_HOST=10.0.0.1
   REDIS_PORT=6379

   # Cache & Webhook API
   CACHE_VALIDATION_TOKEN=your_token_here
   WEBHOOK_API_TOKEN=your_token_here
   WEBHOOK_CALLBACK_BASE_URL=http://10.0.0.3:3000
   WEBHOOK_SERVER_PORT=3000

   WORKER_CONCURRENCY=60
   ```
2. Start Server 3:
   ```bash
   npm run worker
   ```

---

## 3. Real-Time Monitoring & Safe Stop

### Live Dashboard
Open in another terminal:
```bash
npm run monitor
```

### Safe Stop
Hit `Ctrl + C` on any server:
- Finishes active in-flight jobs.
- Flushes and closes all Parquet chunk files cleanly.
- Saves the exact cursor checkpoint to MongoDB so you can resume anytime with zero duplicate work.

---

## 4. Target Velocity Check

- **Cluster Total**: ~350 records/second (21,000 / min, 30M / day)
- **Per Server**: ~117 records/second
- **5-Day Finish**: 150 Million records completed.
