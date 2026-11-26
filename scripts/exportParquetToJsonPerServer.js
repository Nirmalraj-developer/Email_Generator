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

async function convertParquetToJsonPerServer() {
  console.log(`==================================================================`);
  console.log(`📦 EXTRACTING 3-STAGE PARQUET DATA TO JSON PER SERVER`);
  console.log(`   Scanning: ${DOWNLOADS_DIR}`);
  console.log(`==================================================================\n`);

  if (!fs.existsSync(DOWNLOADS_DIR)) {
    console.error(`Directory not found: ${DOWNLOADS_DIR}`);
    process.exit(1);
  }

  const serverFolders = fs.readdirSync(DOWNLOADS_DIR).filter(item => {
    const full = path.join(DOWNLOADS_DIR, item);
    return fs.statSync(full).isDirectory() && item.startsWith('server');
  });

  if (serverFolders.length === 0) {
    console.log('No server subdirectories found in ./downloads.');
    return;
  }

  const grandSummary = {};

  for (const server of serverFolders) {
    const serverPath = path.join(DOWNLOADS_DIR, server);
    console.log(`\n======================================================`);
    console.log(`🖥️  SERVER: ${server}`);
    console.log(`======================================================`);

    const jsonOutputDir = path.join(serverPath, 'json_exports');
    fs.mkdirSync(jsonOutputDir, { recursive: true });

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

    const parquetFiles = getFiles(serverPath);
    console.log(`📁 Found ${parquetFiles.length} Parquet file(s) for ${server}`);

    const serverData = {
      valid: [],
      webhook_triggers: [],
      invalid: []
    };

    for (const file of parquetFiles) {
      const fileName = path.basename(file);
      let targetCategory = 'invalid';

      if (file.includes('/valid/') || fileName.startsWith('valid_emails')) {
        targetCategory = 'valid';
      } else if (file.includes('/webhook_triggers/') || fileName.startsWith('webhook_triggers') || fileName.startsWith('waiting_webhook')) {
        targetCategory = 'webhook_triggers';
      } else if (file.includes('/invalid/') || fileName.startsWith('invalid_emails') || fileName.startsWith('failed_permanent') || fileName.startsWith('permanent_invalid')) {
        targetCategory = 'invalid';
      }

      try {
        const reader = await parquet.ParquetReader.openFile(file);
        const cursor = reader.getCursor();
        let record = null;
        let count = 0;

        while ((record = await cursor.next())) {
          count++;
          const clean = sanitizeRecord(record);
          serverData[targetCategory].push(clean);
        }
        await reader.close();
        console.log(`  ✓ Read ${fileName}: ${count} records -> [${targetCategory}]`);
      } catch (err) {
        console.warn(`  ⚠️ Skipped ${fileName}: ${err.message}`);
      }
    }

    for (const [category, records] of Object.entries(serverData)) {
      const jsonFile = path.join(jsonOutputDir, `${category}.json`);
      fs.writeFileSync(jsonFile, JSON.stringify(records, null, 2), 'utf-8');
      console.log(`  💾 Wrote ${records.length.toLocaleString()} records to: ${jsonFile}`);
    }

    grandSummary[server] = {
      valid: serverData.valid.length,
      webhook_triggers: serverData.webhook_triggers.length,
      invalid: serverData.invalid.length
    };
  }

  console.log(`\n==================================================================`);
  console.log(`🎉 3-STAGE PARQUET DATA EXTRACTED TO JSON!`);
  console.log(`==================================================================`);
  console.table(grandSummary);
}

convertParquetToJsonPerServer().catch(console.error);
