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

async function convertParquetToJsonPerServer() {
  console.log(`==================================================================`);
  console.log(`📦 STREAMING 3-STAGE PARQUET DATA TO JSON PER SERVER`);
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
    try {
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

      // Initialize direct streaming writers for the 3 clean stages
      const writers = {
        valid: new JsonArrayStreamWriter(path.join(jsonOutputDir, 'valid.json')),
        webhook_triggers: new JsonArrayStreamWriter(path.join(jsonOutputDir, 'webhook_triggers.json')),
        invalid: new JsonArrayStreamWriter(path.join(jsonOutputDir, 'invalid.json'))
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
            await writers[targetCategory].writeRecord(clean);
          }
          await reader.close();
          console.log(`  ✓ Read ${fileName}: ${count} records -> [${targetCategory}]`);
        } catch (err) {
          console.warn(`  ⚠️ Skipped ${fileName}: ${err.message}`);
        }
      }

      // Close all streams cleanly
      for (const [cat, w] of Object.entries(writers)) {
        await w.finish();
        console.log(`  💾 Wrote ${w.count.toLocaleString()} records to: ${w.filePath}`);
      }

      grandSummary[server] = {
        valid: writers.valid.count,
        webhook_triggers: writers.webhook_triggers.count,
        invalid: writers.invalid.count
      };
    } catch (serverErr) {
      console.error(`❌ Error on server ${server}:`, serverErr.message);
    }
  }

  console.log(`\n==================================================================`);
  console.log(`🎉 3-STAGE PARQUET DATA STREAMED TO JSON SUCCESSFULLY!`);
  console.log(`==================================================================`);
  console.table(grandSummary);
}

convertParquetToJsonPerServer().catch(console.error);
