import fs from 'fs';
import path from 'path';

const DOWNLOADS_DIR = './downloads';

async function audit5k() {
  console.log('==================================================================');
  console.log('📊 COMPREHENSIVE 5,000 CONTACTS CLUSTER TALLY & AUDIT');
  console.log('==================================================================\n');

  const validFiles = [
    'downloads/server2_173.249.56.10/json_exports/valid.json',
    'downloads/server3_20.77.104.131/json_exports/valid.json',
    'downloads/server4_173.249.52.244/json_exports/valid.json'
  ];

  const waitingFiles = [
    'downloads/server2_173.249.56.10/json_exports/waiting_webhook_v6.json',
    'downloads/server3_20.77.104.131/json_exports/waiting_webhook_v6.json',
    'downloads/server4_173.249.52.244/json_exports/waiting_webhook_v6.json'
  ];

  let totalValid = 0;
  const validByServer = {};
  const validPersonIds = new Set();
  const validEmails = new Set();
  const validSources = {};

  for (const f of validFiles) {
    const sName = f.split('/')[1];
    if (fs.existsSync(f)) {
      const data = JSON.parse(fs.readFileSync(f, 'utf8'));
      validByServer[sName] = data.length;
      totalValid += data.length;

      data.forEach(row => {
        if (row.personId) validPersonIds.add(row.personId);
        if (row.email) validEmails.add(row.email.toLowerCase());
        const src = row.source || 'unknown';
        validSources[src] = (validSources[src] || 0) + 1;
      });
    }
  }

  console.log('1️⃣  DELIVERABLE VALID EMAILS FOUND:');
  console.log(`   - Server 2: ${validByServer['server2_173.249.56.10'] || 0}`);
  console.log(`   - Server 3: ${validByServer['server3_20.77.104.131'] || 0}`);
  console.log(`   - Server 4: ${validByServer['server4_173.249.52.244'] || 0}`);
  console.log(`   ---------------------------------------------`);
  console.log(`   🎯 Total Valid Records:           ${totalValid}`);
  console.log(`   👤 Unique Persons with Valid Email: ${validPersonIds.size}`);
  console.log(`   📧 Unique Valid Email Addresses:   ${validEmails.size}`);
  console.log(`   🔍 Validation Sources Breakdown:   ${JSON.stringify(validSources)}`);

  console.log('\n2️⃣  WAITING WEBHOOK V6 (DISPATCHED CANDIDATES):');
  let totalWaiting = 0;
  const waitingPersonIds = new Set();
  const waitingByServer = {};

  for (const f of waitingFiles) {
    const sName = f.split('/')[1];
    if (fs.existsSync(f)) {
      const data = JSON.parse(fs.readFileSync(f, 'utf8'));
      waitingByServer[sName] = data.length;
      totalWaiting += data.length;

      data.forEach(row => {
        if (row.personId) waitingPersonIds.add(row.personId);
      });
    }
  }

  console.log(`   - Server 2: ${waitingByServer['server2_173.249.56.10'] || 0}`);
  console.log(`   - Server 3: ${waitingByServer['server3_20.77.104.131'] || 0}`);
  console.log(`   - Server 4: ${waitingByServer['server4_173.249.52.244'] || 0}`);
  console.log(`   ---------------------------------------------`);
  console.log(`   ⏳ Total Candidate Patterns Dispatched: ${totalWaiting.toLocaleString()}`);
  console.log(`   👤 Unique Persons in Dispatches:        ${waitingPersonIds.size.toLocaleString()}`);

  const allPersons = new Set([...validPersonIds, ...waitingPersonIds]);
  console.log('\n3️⃣  INPUT CONTACTS TALLY:');
  console.log(`   🎯 Total Distinct People Processed:     ${allPersons.size.toLocaleString()} / 5,000 target`);
  console.log(`   ⚡ Valid Hit Rate:                      ${((validPersonIds.size / allPersons.size) * 100).toFixed(2)}%`);

  // Check 5 sample valid emails
  console.log('\n4️⃣  SAMPLE DELIVERABLE VALID EMAILS:');
  const sampleValid = JSON.parse(fs.readFileSync(validFiles[0], 'utf8')).slice(0, 3);
  sampleValid.forEach((s, idx) => {
    console.log(`   #${idx+1}: ${s.firstName} ${s.lastName} -> ${s.email} (Pattern: ${s.pattern}, Score: ${s.score}, Source: ${s.source})`);
  });
}

audit5k().catch(console.error);
