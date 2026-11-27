import fs from 'fs';
import path from 'path';
import { config } from './config.js';

// Cache line counts for immutable (already-rotated) .jsonl files so disk scans stay < 50ms
const immutableFileLineCache = new Map();

function countNewlinesFast(filePath) {
  return new Promise((resolve) => {
    let lines = 0;
    const stream = fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 });
    stream.on('data', (chunk) => {
      for (let i = 0; i < chunk.length; i++) {
        if (chunk[i] === 10) lines++;
      }
    });
    stream.on('end', () => resolve(lines));
    stream.on('error', () => resolve(0));
  });
}

async function scanJsonlFolder(dir) {
  if (!fs.existsSync(dir)) return { files: 0, rows: 0, sizeMB: '0.0' };
  const list = fs.readdirSync(dir)
    .filter(f => f.endsWith('.jsonl'))
    .map(f => {
      const full = path.join(dir, f);
      const st = fs.statSync(full);
      return { name: f, full, size: st.size, mtimeMs: st.mtimeMs };
    })
    .sort((a, b) => a.mtimeMs - b.mtimeMs);

  let totalBytes = 0;
  let totalRows = 0;

  for (let i = 0; i < list.length; i++) {
    const file = list[i];
    totalBytes += file.size;
    const isLatestFile = (i === list.length - 1);

    if (!isLatestFile && immutableFileLineCache.has(file.full)) {
      totalRows += immutableFileLineCache.get(file.full);
    } else {
      const count = await countNewlinesFast(file.full);
      if (!isLatestFile) {
        immutableFileLineCache.set(file.full, count);
      }
      totalRows += count;
    }
  }

  return {
    files: list.length,
    rows: totalRows,
    sizeMB: (totalBytes / (1024 * 1024)).toFixed(1)
  };
}

let diskTotals = {
  valid: { files: 0, rows: 0, sizeMB: '0.0' },
  invalid: { files: 0, rows: 0, sizeMB: '0.0' },
  triggers: { files: 0, rows: 0, sizeMB: '0.0' },
  peopleCompleted: 0,
  scanning: false
};

async function refreshDiskTotals() {
  if (diskTotals.scanning) return;
  diskTotals.scanning = true;
  try {
    const [valid, invalid, triggers, peopleCompleted] = await Promise.all([
      scanJsonlFolder(path.join(config.output.baseDir, 'valid')),
      scanJsonlFolder(path.join(config.output.baseDir, 'invalid')),
      scanJsonlFolder(path.join(config.output.baseDir, 'webhook_triggers')),
      fs.existsSync(config.checkpointFile) ? countNewlinesFast(config.checkpointFile) : Promise.resolve(0)
    ]);
    diskTotals = { valid, invalid, triggers, peopleCompleted, scanning: false };
  } catch {
    diskTotals.scanning = false;
  }
}

function render() {
  const statsPath = path.join(config.output.baseDir, 'live_stats.json');
  let s = null;
  if (fs.existsSync(statsPath)) {
    try {
      s = JSON.parse(fs.readFileSync(statsPath, 'utf8'));
    } catch {}
  }

  const allRecv = diskTotals.valid.rows + diskTotals.invalid.rows;

  console.clear();
  console.log('╔════════════════════════════════════════════════════════════════════════╗');
  console.log('║              ⚡ VERIFY V6 WATERFALL RUNNER - LIVE MONITOR              ║');
  console.log('╠════════════════════════════════════════════════════════════════════════╣');
  if (!s) {
    console.log('║  Waiting for v6-runner to start (data/live_stats.json)...              ║');
  } else {
    const sessionRecv = (s.validCount || 0) + (s.invalidCount || 0);
    console.log(`║  Server: ${String(s.serverId).padEnd(25)} Updated: ${new Date(s.updatedAt).toLocaleTimeString().padEnd(21)}║`);
    console.log('╠════════════════════════════════════════════════════════════════════════╣');
    console.log('║  CURRENT SESSION (NEW / LIVE)                                          ║');
    console.log(`║    Outbound Send/s:   ${String(s.sendRate.toLocaleString() + ' /s').padEnd(16)} Target: ${String(s.targetRps + ' /s').padEnd(22)}║`);
    console.log(`║    Webhook In/s:      ${String(s.webhookInRate.toLocaleString() + ' /s').padEnd(16)} InFlight CB: ${String(s.inFlight.toLocaleString()).padEnd(17)}║`);
    console.log(`║    Active HTTP:       ${String(s.activeSends).padEnd(16)} EscalationsQ: ${String(s.escalationsQ.toLocaleString()).padEnd(16)}║`);
    console.log(`║    Contacts Loaded:   ${String(s.loadedContacts.toLocaleString()).padEnd(16)} Skipped: ${String(s.skippedCompleted.toLocaleString()).padEnd(21)}║`);
    console.log(`║    ✅ Valid (New):    ${String(s.validCount.toLocaleString()).padEnd(16)} ❌ Invalid (New): ${String(s.invalidCount.toLocaleString()).padEnd(12)}║`);
    console.log(`║    📨 Triggers (New): ${String(s.triggersWritten.toLocaleString()).padEnd(16)} 📬 Recv (New):    ${String(sessionRecv.toLocaleString()).padEnd(12)}║`);
    console.log(`║    🔄 Retries:        ${String(s.retries.toLocaleString()).padEnd(16)} Errors: ${String(s.errors.toLocaleString()).padEnd(22)}║`);
  }
  console.log('╠════════════════════════════════════════════════════════════════════════╣');
  console.log('║  ALL-TIME TOTALS ON DISK (OLD + NEW COMBINED)                          ║');
  console.log(`║    🎯 People Completed:      ${String(diskTotals.peopleCompleted.toLocaleString()).padEnd(42)}║`);
  console.log(`║    ✅ Total Valid Emails:    ${String(diskTotals.valid.rows.toLocaleString() + ` (${diskTotals.valid.files} files, ${diskTotals.valid.sizeMB} MB)`).padEnd(42)}║`);
  console.log(`║    ❌ Total Invalid Emails:  ${String(diskTotals.invalid.rows.toLocaleString() + ` (${diskTotals.invalid.files} files, ${diskTotals.invalid.sizeMB} MB)`).padEnd(42)}║`);
  console.log(`║    📨 Total Triggers Sent:   ${String(diskTotals.triggers.rows.toLocaleString() + ` (${diskTotals.triggers.files} files, ${diskTotals.triggers.sizeMB} MB)`).padEnd(42)}║`);
  console.log(`║    📬 Total Webhooks Recv:   ${String(allRecv.toLocaleString()).padEnd(42)}║`);
  console.log('╚════════════════════════════════════════════════════════════════════════╝');
  console.log('Press Ctrl+C to exit monitor.');
}

refreshDiskTotals().then(render);
setInterval(render, 1000);
setInterval(refreshDiskTotals, 5000);
