import parquet from 'parquetjs-lite';
import fs from 'fs';
import path from 'path';

const DOWNLOADS_DIR = process.argv[2] || './downloads';

function sanitize(val) {
  if (typeof val === 'bigint') {
    return Number(val);
  }
  return val;
}

function sanitizeRecord(rec) {
  const clean = {};
  for (const [k, v] of Object.entries(rec)) {
    clean[k] = sanitize(v);
  }
  return clean;
}

async function runDirectTally() {
  console.log('==================================================================');
  console.log('🎯 DIRECT PARQUET CLUSTER PERSON-LEVEL AUDIT & TALLY');
  console.log(`   Scanning: ${DOWNLOADS_DIR}`);
  console.log('==================================================================\n');

  const servers = ['server2_173.249.56.10', 'server3_20.77.104.131', 'server4_173.249.52.244'];
  const validMap = new Map(); // personId -> record
  const invalidPersonsMap = new Map(); // personId -> summary
  const validByServer = {};
  const triggersByServer = {};

  // 1. Scan Valid Parquet across all servers (First-Valid-Wins)
  for (const server of servers) {
    const validDir = path.join(DOWNLOADS_DIR, server, 'valid');
    validByServer[server] = 0;
    if (!fs.existsSync(validDir)) continue;

    const files = fs.readdirSync(validDir).filter(f => f.endsWith('.parquet'));
    for (const file of files) {
      try {
        const reader = await parquet.ParquetReader.openFile(path.join(validDir, file));
        const cursor = reader.getCursor();
        let rec;
        while ((rec = await cursor.next())) {
          const clean = sanitizeRecord(rec);
          if (clean.personId && !validMap.has(clean.personId)) {
            validMap.set(clean.personId, clean);
            validByServer[server]++;
          }
        }
        await reader.close();
      } catch (err) {
        // Skip unclosed or corrupted part files
      }
    }
  }

  // 2. Scan Webhook Triggers Parquet across all servers (Audit trail of V6 submissions)
  for (const server of servers) {
    const triggerDir = path.join(DOWNLOADS_DIR, server, 'webhook_triggers');
    triggersByServer[server] = 0;
    if (!fs.existsSync(triggerDir)) continue;

    const files = fs.readdirSync(triggerDir).filter(f => f.endsWith('.parquet'));
    for (const file of files) {
      try {
        const reader = await parquet.ParquetReader.openFile(path.join(triggerDir, file));
        const cursor = reader.getCursor();
        let rec;
        while ((rec = await cursor.next())) {
          const clean = sanitizeRecord(rec);
          if (clean.personId && !validMap.has(clean.personId) && !invalidPersonsMap.has(clean.personId)) {
            invalidPersonsMap.set(clean.personId, {
              personId: clean.personId,
              firstName: clean.firstName || '',
              lastName: clean.lastName || '',
              companyId: clean.companyId || '',
              companyName: clean.companyName || '',
              website: clean.website || '',
              domain: clean.domain || '',
              status: 'triggered_webhook_v6_cache_miss',
              reason: 'All candidate patterns missed cache and were dispatched to Verify V6',
              evaluatedAt: clean.createdAt || Date.now()
            });
            triggersByServer[server]++;
          }
        }
        await reader.close();
      } catch (err) {
        // Skip unclosed or corrupted part files
      }
    }
  }

  const validArray = Array.from(validMap.values());
  const invalidArray = Array.from(invalidPersonsMap.values());
  const totalEvaluated = validArray.length + invalidArray.length;

  console.log('1️⃣  DELIVERABLE VALID EMAILS (Found in Cache, Score 99):');
  for (const s of servers) {
    console.log(`   - ${s}: ${validByServer[s] || 0} unique people`);
  }
  console.log(`   -------------------------------------------------`);
  console.log(`   🎯 Total Valid People:        ${validArray.length.toLocaleString()}`);

  console.log('\n2️⃣  CACHE MISSES / WEBHOOK TRIGGERED PEOPLE (Sent to Verify V6):');
  for (const s of servers) {
    console.log(`   - ${s}: ${triggersByServer[s] || 0} unique people`);
  }
  console.log(`   -------------------------------------------------`);
  console.log(`   ⏳ Total Webhook Triggered:   ${invalidArray.length.toLocaleString()}`);

  console.log('\n==================================================================');
  console.log('🎯 CLUSTER FINAL BALANCE TALLY (5,000 Target)');
  console.log('==================================================================');
  console.log(`✅ Valid Deliverable People (Score 99):        ${validArray.length.toLocaleString()}`);
  console.log(`❌ Invalid / Webhook Triggered People:         ${invalidArray.length.toLocaleString()}`);
  console.log('------------------------------------------------------------------');
  console.log(`TOTAL DISTINCT PEOPLE TALLIED =              ${totalEvaluated.toLocaleString()} (Out of 4,912 target)`);
  console.log('==================================================================');

  // Export to cluster_tally_summary
  const outDir = path.join(DOWNLOADS_DIR, 'cluster_tally_summary');
  fs.mkdirSync(outDir, { recursive: true });

  fs.writeFileSync(path.join(outDir, 'all_valid_people.json'), JSON.stringify(validArray, null, 2), 'utf-8');
  fs.writeFileSync(path.join(outDir, 'all_invalid_people.json'), JSON.stringify(invalidArray, null, 2), 'utf-8');

  console.log(`\n📁 Exported clean files:`);
  console.log(`   - ${outDir}/all_valid_people.json   (${validArray.length.toLocaleString()} records)`);
  console.log(`   - ${outDir}/all_invalid_people.json (${invalidArray.length.toLocaleString()} records)\n`);
}

runDirectTally().catch(console.error);
