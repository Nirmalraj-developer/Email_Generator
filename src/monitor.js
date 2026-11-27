import fs from 'fs';
import path from 'path';
import { config } from './config.js';

function countJsonlFiles(dir) {
  if (!fs.existsSync(dir)) return { files: 0, sizeMB: '0.0' };
  const list = fs.readdirSync(dir).filter(f => f.endsWith('.jsonl'));
  let bytes = 0;
  for (const f of list) {
    try { bytes += fs.statSync(path.join(dir, f)).size; } catch {}
  }
  return { files: list.length, sizeMB: (bytes / (1024 * 1024)).toFixed(1) };
}

function render() {
  const statsPath = path.join(config.output.baseDir, 'live_stats.json');
  let s = null;
  if (fs.existsSync(statsPath)) {
    try {
      s = JSON.parse(fs.readFileSync(statsPath, 'utf8'));
    } catch {}
  }

  const validDisk = countJsonlFiles(path.join(config.output.baseDir, 'valid'));
  const invalidDisk = countJsonlFiles(path.join(config.output.baseDir, 'invalid'));
  const triggerDisk = countJsonlFiles(path.join(config.output.baseDir, 'webhook_triggers'));

  console.clear();
  console.log('╔══════════════════════════════════════════════════════════════════╗');
  console.log('║           ⚡ VERIFY V6 WATERFALL RUNNER - LIVE MONITOR           ║');
  console.log('╠══════════════════════════════════════════════════════════════════╣');
  if (!s) {
    console.log('║  Waiting for v6-runner to start (data/live_stats.json)...        ║');
  } else {
    console.log(`║  Server: ${String(s.serverId).padEnd(22)} Updated: ${new Date(s.updatedAt).toLocaleTimeString().padEnd(18)}║`);
    console.log('╠══════════════════════════════════════════════════════════════════╣');
    console.log(`║  Outbound Send/s:   ${String(s.sendRate.toLocaleString() + ' /s').padEnd(16)} Target: ${String(s.targetRps + ' /s').padEnd(16)}║`);
    console.log(`║  Webhook In/s:      ${String(s.webhookInRate.toLocaleString() + ' /s').padEnd(44)}║`);
    console.log(`║  Active HTTP:       ${String(s.activeSends).padEnd(16)} InFlight CB: ${String(s.inFlight.toLocaleString()).padEnd(11)}║`);
    console.log(`║  Escalations Queue: ${String(s.escalationsQ.toLocaleString()).padEnd(44)}║`);
    console.log('╠══════════════════════════════════════════════════════════════════╣');
    console.log(`║  Contacts Loaded:   ${String(s.loadedContacts.toLocaleString()).padEnd(16)} Skipped: ${String(s.skippedCompleted.toLocaleString()).padEnd(15)}║`);
    console.log(`║  ✅ Valid Saved:    ${String(s.validCount.toLocaleString()).padEnd(44)}║`);
    console.log(`║  ❌ Invalid Saved:  ${String(s.invalidCount.toLocaleString()).padEnd(44)}║`);
    console.log(`║  📨 Triggers Sent:  ${String(s.triggersWritten.toLocaleString()).padEnd(44)}║`);
    console.log(`║  🔄 Retries:        ${String(s.retries.toLocaleString()).padEnd(16)} Errors: ${String(s.errors.toLocaleString()).padEnd(16)}║`);
  }
  console.log('╠══════════════════════════════════════════════════════════════════╣');
  console.log(`║  JSONL Files (data/valid):            ${String(validDisk.files + ' files (' + validDisk.sizeMB + ' MB)').padEnd(26)}║`);
  console.log(`║  JSONL Files (data/invalid):          ${String(invalidDisk.files + ' files (' + invalidDisk.sizeMB + ' MB)').padEnd(26)}║`);
  console.log(`║  JSONL Files (data/webhook_triggers): ${String(triggerDisk.files + ' files (' + triggerDisk.sizeMB + ' MB)').padEnd(26)}║`);
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  console.log('Press Ctrl+C to exit monitor.');
}

render();
setInterval(render, 1000);
