import assert from 'assert';
import http from 'http';
import fs from 'fs';
import path from 'path';
import parquet from 'parquetjs-lite';
import { V6WaterfallRunner } from '../src/runner.js';
import { getSliceFiles } from '../src/sliceReader.js';

const TEST_DIR = './tests/tmp_test_data';

function cleanup() {
  if (fs.existsSync(TEST_DIR)) {
    fs.rmSync(TEST_DIR, { recursive: true, force: true });
  }
}

async function readParquetRows(dir) {
  if (!fs.existsSync(dir)) return [];
  const files = fs.readdirSync(dir).filter(f => f.endsWith('.parquet'));
  const rows = [];
  for (const f of files) {
    const reader = await parquet.ParquetReader.openFile(path.join(dir, f));
    const cursor = reader.getCursor();
    let rec;
    while ((rec = await cursor.next())) rows.push(rec);
    await reader.close();
  }
  return rows;
}

async function runTests() {
  cleanup();
  const inputDir = path.join(TEST_DIR, 'input');
  const outDir = path.join(TEST_DIR, 'out');
  fs.mkdirSync(inputDir, { recursive: true });

  // 1. Create excluded file (must be ignored) and slice_0.jsonl
  fs.writeFileSync(path.join(inputDir, 'all_verifications_unique.jsonl'), '{"person_id":"ignored_1","patterns":[{"email":"x@y.com","score":null}]}\n');

  const completedDoc = {
    _id: 'done_1',
    person_id: 'done_1',
    company_id: 'c1',
    patterns: [{ pattern: '<first>@', email: 'done@a.com', score: null }],
    primary_email: 'done@a.com',
    email_smt_status: 'completed'
  };
  const pendingDoc = {
    _id: 'p_100',
    person_id: 'p_100',
    company_id: 'c_100',
    patterns: [
      { pattern: '<first>.<last>@', email: 'already.checked@test.com', score: 0 },
      { pattern: '<first><last>@', email: 'p1.invalid@test.com', score: null },
      { pattern: '<f><last>@', email: 'p2.valid@test.com', score: null }
    ],
    primary_email: null,
    email_smt_status: null
  };

  fs.writeFileSync(
    path.join(inputDir, 'pattern_verifications_slice_0.jsonl'),
    JSON.stringify(completedDoc) + '\n' + JSON.stringify(pendingDoc) + '\n'
  );

  // Verify excluded file filtering
  const files = getSliceFiles(inputDir, ['all_verifications_unique.jsonl']);
  assert.strictEqual(files.length, 1, 'Should exclude all_verifications_unique.jsonl');

  // 2. Start Mock Verify V6 API server that posts callbacks to our runner
  const mockV6 = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => {
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ trackingId: body.tracking_id }));

      // Simulate async Verify V6 webhook callback
      setTimeout(() => {
        const isValid = body.email === 'p2.valid@test.com';
        const cbData = JSON.stringify({
          trackingId: body.tracking_id,
          email: body.email,
          deliverable: isValid,
          score: isValid ? 99 : 0,
          reason: isValid ? 'deliverable' : 'undeliverable'
        });
        const cbReq = http.request(body.callback_url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(cbData) }
        });
        cbReq.write(cbData);
        cbReq.end();
      }, 20);
    });
  });

  await new Promise(resolve => mockV6.listen(4091, '127.0.0.1', resolve));

  const runner = new V6WaterfallRunner({
    serverId: 'test_v6',
    inputDir,
    excludedFiles: ['all_verifications_unique.jsonl'],
    v6: { url: 'http://127.0.0.1:4091/validate', apiToken: 'test', targetRps: 3000, maxConcurrency: 100, timeoutMs: 5000, maxRetries: 1 },
    webhook: { port: 4092, path: '/api/webhook/validation', callbackBaseUrl: 'http://127.0.0.1:4092' },
    parquet: { baseDir: outDir, maxCapacity: 1000000 },
    checkpointFile: path.join(TEST_DIR, 'processed_ids.log'),
    errorLogFile: path.join(TEST_DIR, 'error.log'),
    statsIntervalMs: 5000
  });

  await runner.start({ waitForInFlightMs: 1500 });
  await runner.safeStop();
  await new Promise(resolve => mockV6.close(resolve));

  const validRows = await readParquetRows(path.join(outDir, 'valid'));
  const invalidRows = await readParquetRows(path.join(outDir, 'invalid'));
  const triggerRows = await readParquetRows(path.join(outDir, 'webhook_triggers'));

  assert.strictEqual(validRows.length, 1, 'Expected 1 valid row');
  assert.strictEqual(validRows[0].email, 'p2.valid@test.com');
  assert.strictEqual(invalidRows.length, 1, 'Expected 1 invalid row');
  assert.strictEqual(invalidRows[0].email, 'p1.invalid@test.com');
  assert.strictEqual(triggerRows.length, 2, 'Expected 2 webhook triggers (skipped non-null score)');

  cleanup();
  console.log('✅ All v6-webhook-runner tests passed!');
}

runTests().catch(err => {
  console.error('❌ Test failed:', err);
  cleanup();
  process.exit(1);
});
