import fs from 'fs';
import path from 'path';

const DOWNLOADS_DIR = './downloads';

function exportPeopleSummary() {
  const validFiles = [
    'downloads/server2_173.249.56.10/json_exports/valid.json',
    'downloads/server3_20.77.104.131/json_exports/valid.json',
    'downloads/server4_173.249.52.244/json_exports/valid.json'
  ];

  const triggerFiles = [
    'downloads/server2_173.249.56.10/json_exports/webhook_triggers.json',
    'downloads/server3_20.77.104.131/json_exports/webhook_triggers.json',
    'downloads/server4_173.249.52.244/json_exports/webhook_triggers.json',
    'downloads/server2_173.249.56.10/json_exports/waiting_webhook_v6.json',
    'downloads/server3_20.77.104.131/json_exports/waiting_webhook_v6.json',
    'downloads/server4_173.249.52.244/json_exports/waiting_webhook_v6.json'
  ];

  const invalidFiles = [
    'downloads/server2_173.249.56.10/json_exports/invalid.json',
    'downloads/server3_20.77.104.131/json_exports/invalid.json',
    'downloads/server4_173.249.52.244/json_exports/invalid.json'
  ];

  const validMap = new Map();
  validFiles.forEach(f => {
    if (fs.existsSync(f)) {
      const arr = JSON.parse(fs.readFileSync(f, 'utf8'));
      arr.forEach(r => {
        if (r.personId) validMap.set(r.personId, r);
      });
    }
  });

  const invalidPersonsMap = new Map();
  const markInvalidPerson = (r, defaultReason) => {
    if (!r.personId) return;
    if (!validMap.has(r.personId) && !invalidPersonsMap.has(r.personId)) {
      invalidPersonsMap.set(r.personId, {
        personId: r.personId,
        firstName: r.firstName,
        lastName: r.lastName,
        companyId: r.companyId,
        companyName: r.companyName,
        website: r.website,
        domain: r.domain,
        status: r.status || 'invalid_or_unresolved_cache_miss',
        reason: r.reason || defaultReason,
        evaluatedAt: r.createdAt
      });
    }
  };

  triggerFiles.forEach(f => {
    if (fs.existsSync(f)) {
      const arr = JSON.parse(fs.readFileSync(f, 'utf8'));
      arr.forEach(r => markInvalidPerson(r, 'All candidate patterns missed cache and were dispatched to Verify V6'));
    }
  });

  invalidFiles.forEach(f => {
    if (fs.existsSync(f)) {
      const arr = JSON.parse(fs.readFileSync(f, 'utf8'));
      arr.forEach(r => markInvalidPerson(r, 'No deliverable email found'));
    }
  });

  const validArray = Array.from(validMap.values());
  const invalidArray = Array.from(invalidPersonsMap.values());

  const outDir = './downloads/cluster_tally_summary';
  fs.mkdirSync(outDir, { recursive: true });

  fs.writeFileSync(path.join(outDir, 'all_valid_people.json'), JSON.stringify(validArray, null, 2));
  fs.writeFileSync(path.join(outDir, 'all_invalid_people.json'), JSON.stringify(invalidArray, null, 2));

  console.log(`\n==================================================================`);
  console.log(`🎯 CLUSTER PERSON-LEVEL BALANCE TALLY (5,000 Target)`);
  console.log(`==================================================================`);
  console.log(`✅ Valid Deliverable People (Found in Cache):  ${validArray.length.toLocaleString()}`);
  console.log(`❌ Invalid / Cache Miss People (Sent to V6):   ${invalidArray.length.toLocaleString()}`);
  console.log(`------------------------------------------------------------------`);
  console.log(`TOTAL DISTINCT PEOPLE TALLIED =              ${(validArray.length + invalidArray.length).toLocaleString()} (Out of 4,912 - 5,000)`);
  console.log(`==================================================================`);
  console.log(`📁 Exported clean files:`);
  console.log(`   - ${outDir}/all_valid_people.json   (${validArray.length} records)`);
  console.log(`   - ${outDir}/all_invalid_people.json (${invalidArray.length} records)\n`);
}

exportPeopleSummary();
