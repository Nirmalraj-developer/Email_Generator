import parquet from 'parquetjs-lite';
import { MongoClient } from 'mongodb';
import fs from 'fs';
import path from 'path';

const MONGO_URI = process.env.LOCAL_MONGO_URI || 'mongodb://127.0.0.1:27017';
const DB_NAME = process.env.LOCAL_DB_NAME || 'email_verification_inspection';
const DATA_DIR = process.argv[2] || './downloads';

function replacer(key, value) {
  if (typeof value === 'bigint') {
    return value.toString();
  }
  return value;
}

function sanitizeRecordForMongo(record) {
  const clean = {};
  for (const [key, val] of Object.entries(record)) {
    if (typeof val === 'bigint') {
      clean[key] = Number(val);
    } else {
      clean[key] = val;
    }
  }
  return clean;
}

async function inspectAndLoad() {
  console.log(`🔍 Scanning Parquet files in: ${DATA_DIR}`);

  if (!fs.existsSync(DATA_DIR)) {
    console.error(`Directory not found: ${DATA_DIR}`);
    process.exit(1);
  }

  function getParquetFiles(dir) {
    let results = [];
    const list = fs.readdirSync(dir);
    list.forEach(file => {
      const fullPath = path.join(dir, file);
      const stat = fs.statSync(fullPath);
      if (stat && stat.isDirectory()) {
        results = results.concat(getParquetFiles(fullPath));
      } else if (file.endsWith('.parquet')) {
        results.push(fullPath);
      }
    });
    return results;
  }

  const files = getParquetFiles(DATA_DIR);
  if (files.length === 0) {
    console.log('⚠️ No .parquet files found in directory.');
    return;
  }

  console.log(`📁 Found ${files.length} Parquet file(s)\n`);

  let mongoClient = null;
  let db = null;
  try {
    mongoClient = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 2000 });
    await mongoClient.connect();
    db = mongoClient.db(DB_NAME);
    console.log(` Connected to MongoDB at ${MONGO_URI} [Database: ${DB_NAME}]`);
  } catch (err) {
    console.warn(`\n⚠️ Local MongoDB not reachable at ${MONGO_URI} (${err?.message}). Continuing in inspect mode (Console Output)...\n`);
    mongoClient = null;
    db = null;
  }

  const stats = {
    valid: 0,
    failed_permanent: 0,
    waiting_webhook_v6: 0,
    permanent_invalid_cache: 0,
    skipped_active: 0
  };

  for (const file of files) {
    let category = 'other_invalid';
    let collectionName = 'invalid_emails';

    if (file.includes('/valid/') || path.basename(file).startsWith('valid_emails')) {
      category = 'VALID';
      collectionName = 'valid_emails';
    } else if (file.includes('failed_permanent')) {
      category = 'FAILED_PERMANENT';
      collectionName = 'failed_permanent';
    } else if (file.includes('waiting_webhook_v6')) {
      category = 'WAITING_WEBHOOK_V6';
      collectionName = 'waiting_webhook_v6';
    } else if (file.includes('permanent_invalid_cache')) {
      category = 'PERMANENT_INVALID_CACHE';
      collectionName = 'permanent_invalid_cache';
    }

    console.log(`\n======================================================`);
    console.log(`📄 Reading: ${path.basename(file)} (Category: ${category})`);
    console.log(`======================================================`);

    try {
      const reader = await parquet.ParquetReader.openFile(file);
      const cursor = reader.getCursor();
      let record = null;
      let count = 0;
      const batch = [];

      while ((record = await cursor.next())) {
        count++;
        const cleanRecord = sanitizeRecordForMongo(record);
        batch.push(cleanRecord);

        if (count <= 2) {
          console.log(`[Sample Row #${count}]`, JSON.stringify(record, replacer, 2));
        }

        if (db && batch.length >= 1000) {
          await db.collection(collectionName).insertMany(batch);
          batch.length = 0;
        }
      }

      if (db && batch.length > 0) {
        await db.collection(collectionName).insertMany(batch);
      }

      await reader.close();
      console.log(`✅ Completed ${path.basename(file)}: Total ${count} records.`);

      if (category === 'VALID') stats.valid += count;
      else if (category === 'FAILED_PERMANENT') stats.failed_permanent += count;
      else if (category === 'WAITING_WEBHOOK_V6') stats.waiting_webhook_v6 += count;
      else if (category === 'PERMANENT_INVALID_CACHE') stats.permanent_invalid_cache += count;
    } catch (err) {
      const errMsg = err ? (err.message || String(err)) : 'Unknown error';
      console.warn(`⏳ [Active File Skipped] ${path.basename(file)} is open/unsealed on live server (${errMsg})`);
      stats.skipped_active++;
    }
  }

  console.log(`\n================ CLUSTER DATA SUMMARY ================`);
  console.log(`🎯 Valid Deliverable Emails:          ${stats.valid.toLocaleString()}`);
  console.log(`❌ Failed Permanent (Invalid/Bounce): ${stats.failed_permanent.toLocaleString()}`);
  console.log(`⏳ Waiting Webhook V6 Callback:       ${stats.waiting_webhook_v6.toLocaleString()}`);
  console.log(`🚫 Permanent Invalid (All 10 Cached):  ${stats.permanent_invalid_cache.toLocaleString()}`);
  console.log(`🔄 Active In-Flight Files (Live):     ${stats.skipped_active}`);
  console.log(`======================================================`);

  if (mongoClient) {
    await mongoClient.close();
    console.log(`\n🎉 All completed records cleanly imported into MongoDB database '${DB_NAME}'.`);
  }
}

inspectAndLoad().catch(console.error);
