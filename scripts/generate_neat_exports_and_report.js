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

class JsonArrayStreamWriter {
  constructor(filePath) {
    this.filePath = filePath;
    const parentDir = path.dirname(filePath);
    if (!fs.existsSync(parentDir)) fs.mkdirSync(parentDir, { recursive: true });
    this.stream = fs.createWriteStream(filePath, { encoding: 'utf8' });
    this.stream.write('[\n');
    this.count = 0;
  }

  async writeRecord(record) {
    const prefix = this.count === 0 ? '  ' : ',\n  ';
    const chunk = prefix + JSON.stringify(record);
    this.count++;
    if (!this.stream.write(chunk)) {
      await new Promise(r => this.stream.once('drain', r));
    }
  }

  async finish() {
    this.stream.write('\n]\n');
    await new Promise((resolve, reject) => {
      this.stream.end();
      this.stream.on('finish', resolve);
      this.stream.on('error', reject);
    });
  }
}

async function generateNeatExportsAndReport() {
  console.log('==================================================================');
  console.log('🚀 CLUSTER PARQUET TO NEAT JSON EXPORTER & AUDIT REPORT GENERATOR');
  console.log(`   Input directory: ${DOWNLOADS_DIR}`);
  console.log('==================================================================\n');

  if (!fs.existsSync(DOWNLOADS_DIR)) {
    console.error(`❌ Downloads directory not found at: ${DOWNLOADS_DIR}`);
    process.exit(1);
  }

  const serverFolders = fs.readdirSync(DOWNLOADS_DIR).filter(item => {
    const full = path.join(DOWNLOADS_DIR, item);
    return fs.statSync(full).isDirectory() && item.startsWith('server');
  });

  if (serverFolders.length === 0) {
    console.log('⚠️ No server folders found in downloads directory.');
    return;
  }

  const outClusterDir = path.join(DOWNLOADS_DIR, 'cluster_exports');
  fs.mkdirSync(outClusterDir, { recursive: true });

  const clusterValidPeopleWriter = new JsonArrayStreamWriter(path.join(outClusterDir, 'all_valid_deliverable_people.json'));
  const clusterInvalidPeopleWriter = new JsonArrayStreamWriter(path.join(outClusterDir, 'all_invalid_dead_lead_people.json'));

  const validPersonsMap = new Map(); // personId -> winner
  const invalidPersonsMap = new Map(); // personId -> summary
  const grandSummary = {};

  // Step 1: Process each server's Parquet files into per-server clean JSON
  for (const server of serverFolders) {
    const serverPath = path.join(DOWNLOADS_DIR, server);
    const jsonOut = path.join(serverPath, 'json_exports');
    fs.mkdirSync(jsonOut, { recursive: true });

    console.log(`🔹 Processing ${server}...`);

    function getFiles(dir) {
      let files = [];
      if (!fs.existsSync(dir)) return files;
      for (const item of fs.readdirSync(dir)) {
        if (item === 'json_exports' || item.startsWith('.')) continue;
        const full = path.join(dir, item);
        if (fs.statSync(full).isDirectory()) {
          files = files.concat(getFiles(full));
        } else if (item.endsWith('.parquet')) {
          files.push(full);
        }
      }
      return files;
    }

    const files = getFiles(serverPath);
    const writers = {
      valid: new JsonArrayStreamWriter(path.join(jsonOut, 'valid.json')),
      webhook_triggers: new JsonArrayStreamWriter(path.join(jsonOut, 'webhook_triggers.json')),
      invalid: new JsonArrayStreamWriter(path.join(jsonOut, 'invalid.json'))
    };

    for (const f of files) {
      const bName = path.basename(f);
      let cat = 'invalid';
      if (f.includes('/valid/') || bName.startsWith('valid_emails')) {
        cat = 'valid';
      } else if (f.includes('/webhook_triggers/') || bName.startsWith('webhook_triggers')) {
        cat = 'webhook_triggers';
      } else if (f.includes('/invalid/') || bName.startsWith('invalid_emails')) {
        cat = 'invalid';
      }

      try {
        const reader = await parquet.ParquetReader.openFile(f);
        const cursor = reader.getCursor();
        let rec;
        while ((rec = await cursor.next())) {
          const clean = sanitizeRecord(rec);
          await writers[cat].writeRecord(clean);

          // Track for person-level tally
          const pId = clean.personId;
          if (pId) {
            if (cat === 'valid' && !validPersonsMap.has(pId)) {
              validPersonsMap.set(pId, clean);
            } else if ((cat === 'webhook_triggers' || cat === 'invalid') && !validPersonsMap.has(pId) && !invalidPersonsMap.has(pId)) {
              invalidPersonsMap.set(pId, {
                personId: pId,
                firstName: clean.firstName || '',
                lastName: clean.lastName || '',
                companyId: clean.companyId || '',
                companyName: clean.companyName || '',
                website: clean.website || '',
                domain: clean.domain || '',
                status: clean.status || 'undeliverable_candidate',
                score: clean.score ?? 0,
                deliverable: Boolean(clean.deliverable ?? false),
                evaluatedAt: clean.createdAt || Date.now()
              });
            }
          }
        }
        await reader.close();
      } catch (err) {
        // Skip unclosed or invalid files
      }
    }

    await writers.valid.finish();
    await writers.webhook_triggers.finish();
    await writers.invalid.finish();

    grandSummary[server] = {
      validRows: writers.valid.count,
      webhookTriggerRows: writers.webhook_triggers.count,
      invalidRows: writers.invalid.count
    };
  }

  // Step 2: Stream out Person-level cluster winners and dead leads
  for (const person of validPersonsMap.values()) {
    await clusterValidPeopleWriter.writeRecord(person);
  }
  await clusterValidPeopleWriter.finish();

  for (const person of invalidPersonsMap.values()) {
    if (!validPersonsMap.has(person.personId)) {
      await clusterInvalidPeopleWriter.writeRecord(person);
    }
  }
  await clusterInvalidPeopleWriter.finish();

  const totalValidPeople = validPersonsMap.size;
  const totalInvalidPeople = clusterInvalidPeopleWriter.count;
  const totalDistinctPeople = totalValidPeople + totalInvalidPeople;

  // Step 3: Write Markdown and JSON Audit Reports
  const reportData = {
    generatedAt: new Date().toISOString(),
    totalDistinctPeopleEvaluated: totalDistinctPeople,
    validDeliverablePeople: totalValidPeople,
    invalidDeadLeadPeople: totalInvalidPeople,
    validHitRate: totalDistinctPeople > 0 ? `${((totalValidPeople / totalDistinctPeople) * 100).toFixed(2)}%` : '0%',
    serverBreakdown: grandSummary,
    outputFiles: {
      validDeliverablePeople: path.join(outClusterDir, 'all_valid_deliverable_people.json'),
      invalidDeadLeadPeople: path.join(outClusterDir, 'all_invalid_dead_lead_people.json')
    }
  };

  fs.writeFileSync(path.join(outClusterDir, 'audit_summary_report.json'), JSON.stringify(reportData, null, 2), 'utf8');

  const mdReport = `# 📊 Email Generator & Verification Cluster Audit Report
**Generated At:** ${new Date().toLocaleString()}

---

### 🎯 Executive Person-Level Tally

| Metric | Count | Percentage |
| :--- | :---: | :---: |
| **Total Distinct People Evaluated** | **${totalDistinctPeople.toLocaleString()}** | **100.00%** |
| ✅ **Valid Deliverable People (Score 99/90)** | **${totalValidPeople.toLocaleString()}** | **${reportData.validHitRate}** |
| ❌ **Invalid / Dead Lead People** | **${totalInvalidPeople.toLocaleString()}** | **${(100 - parseFloat(reportData.validHitRate)).toFixed(2)}%** |

$$\\text{Total Evaluated (${totalDistinctPeople.toLocaleString()})} = \\text{Valid (${totalValidPeople.toLocaleString()})} + \\text{Invalid (${totalInvalidPeople.toLocaleString()})}$$

---

### 🖥️ Per-Server Raw Row Breakdown

| Server | Valid Rows | Webhook Triggers | Invalid Rows |
| :--- | :---: | :---: | :---: |
${Object.entries(grandSummary).map(([srv, counts]) => `| **${srv}** | ${counts.validRows.toLocaleString()} | ${counts.webhookTriggerRows.toLocaleString()} | ${counts.invalidRows.toLocaleString()} |`).join('\n')}

---

### 📁 Generated Clean Export Files
* **[all_valid_deliverable_people.json](${path.join(outClusterDir, 'all_valid_deliverable_people.json')})** $\\rightarrow$ Exactly 1 deliverable email per verified contact.
* **[all_invalid_dead_lead_people.json](${path.join(outClusterDir, 'all_invalid_dead_lead_people.json')})** $\\rightarrow$ Clean list of confirmed dead leads / bounces.
* **[audit_summary_report.json](${path.join(outClusterDir, 'audit_summary_report.json')})** $\\rightarrow$ Machine-readable audit tally.
`;

  fs.writeFileSync(path.join(outClusterDir, 'audit_summary_report.md'), mdReport, 'utf8');

  // Display summary in console
  console.log('\n==================================================================');
  console.log('🎉 EXPORT & AUDIT REPORT COMPLETED SUCCESSFULLY!');
  console.log('==================================================================');
  console.table(grandSummary);
  console.log('------------------------------------------------------------------');
  console.log(`✅ Valid Deliverable People:     ${totalValidPeople.toLocaleString()} (${reportData.validHitRate})`);
  console.log(`❌ Invalid / Dead Lead People:    ${totalInvalidPeople.toLocaleString()}`);
  console.log(`🎯 Total Distinct People:        ${totalDistinctPeople.toLocaleString()}`);
  console.log('==================================================================');
  console.log(`📁 All reports and clean files saved to: ${outClusterDir}/\n`);
}

generateNeatExportsAndReport().catch(console.error);
