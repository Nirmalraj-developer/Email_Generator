# Email Generator & Verification Pipeline

A production-grade, fault-tolerant Node.js workflow that streams contacts from Elasticsearch, generates email pattern permutations, verifies them cache-first before catch-all validation, and safely stores verified leads in MongoDB.

## Features
- **Strict Read-Only Elasticsearch**: Only reads records matching `email_smt_reason != 'verified' AND website is not null`. No direct index writes or updates.
- **Pattern Generator**: Generates all 10 canonical email permutations using sanitized first name, last name, and cleaned domain.
- **Cache-First Verification**: Checks `https://cachevalidation.no2bounce.com/api/cache/check/{email}` before consuming real-time credits.
- **Catch-All / Real-Time Validation**: Evaluates cache misses against the catch-all SMTP endpoint.
- **Dedicated MongoDB Persistence**: Upserts valid emails with unique indexes and audit metadata.
- **BullMQ Concurrency & Rate Limiting**: Backpressure control with exponential backoff retries (includes automatic in-memory fallback for development when Redis is offline).
- **Safe Stop & Patch Continue**: Checkpointed `search_after` cursors ensure uninterrupted resume capability on restart or crash.
- **Monitoring CLI**: Real-time terminal dashboard.

---

## Quick Start

### 1. Configure Environment
Copy and adjust `.env.example`:
```bash
cp .env.example .env
```

Key environment options:
```env
USE_MOCKS=false                          # Set to true for local testing without live APIs
ELASTICSEARCH_NODE=http://localhost:9200
ELASTICSEARCH_INDEX=contacts
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
MONGODB_URI=mongodb://localhost:27017/email_generator_db
CACHE_VALIDATION_TOKEN=your_bearer_token
CATCHALL_VERIFY_TOKEN=your_bearer_token
WORKER_CONCURRENCY=5
```

### 2. Run Test Suite
```bash
npm test
```

### 3. Start Workflow
```bash
npm start
```

### 4. Open Live Monitor
In another terminal session:
```bash
npm run monitor
```

---

## Architecture & Project Structure
```
├── .env.example
├── package.json
├── src/
│   ├── index.js                     # Main entrypoint with safe shutdown
│   ├── config/index.js              # Centralized configuration & environment loader
│   ├── services/
│   │   ├── patternGenerator.js      # 10 default template generator & domain cleaner
│   │   ├── cacheValidationClient.js # Cache endpoint client with mock mode
│   │   ├── catchAllVerificationClient.js # Real-time catch-all verification client
│   │   └── elasticReader.js         # Read-only Elasticsearch stream reader
│   ├── storage/
│   │   └── mongoRepository.js       # MongoDB persistence & checkpoint store
│   ├── queue/
│   │   └── pipelineManager.js       # BullMQ queue & worker pipeline
│   ├── workflow/
│   │   └── appController.js         # Stream orchestration & patch-continue logic
│   └── cli/
│       └── monitor.js               # Terminal monitoring dashboard
└── tests/
    └── runAllTests.js               # Comprehensive unit & integration tests
```
