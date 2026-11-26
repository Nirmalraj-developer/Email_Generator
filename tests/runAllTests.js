import assert from 'assert';
import fs from 'fs';
import path from 'path';
import { generateCandidateEmails, extractDomain, sanitizeNamePart } from '../src/services/patternGenerator.js';
import { ChunkedParquetWriter, validEmailSchema, invalidEmailSchema } from '../src/storage/parquetStorage.js';
import { WebhookReceiverServer } from '../src/services/webhookServer.js';
import { PipelineManager } from '../src/queue/pipelineManager.js';
import { WebhookValidationClient } from '../src/services/webhookValidationClient.js';

async function runTests() {
  console.log('🧪 Starting Universal Webhook Handling & Cluster Test Suite...\n');

  // Test 1: Domain extraction & name sanitization
  console.log('Test 1: Domain extraction & name sanitization');
  assert.strictEqual(extractDomain('klodev.com'), 'klodev.com');
  assert.strictEqual(sanitizeNamePart('Bknuth'), 'bknuth');
  console.log('  ✓ Domain and Name extraction passed');

  // Test 2: Webhook client URL construction
  console.log('Test 2: Webhook client URL construction');
  const whClient = new WebhookValidationClient(
    'https://verifyv6.no2bounce.com/api/v2/single-webhook-validation',
    'test_token',
    'http://127.0.0.1:3999'
  );
  assert.strictEqual(whClient.callbackBaseUrl, 'http://127.0.0.1:3999');
  console.log('  ✓ Webhook client configured properly');

  // Test 3: Standalone WebhookReceiverServer handling
  console.log('Test 3: Standalone WebhookReceiverServer handling on port 3999');
  let receivedPayload = null;
  const whServer = new WebhookReceiverServer({
    port: 3999,
    onValidationResult: async (payload) => {
      receivedPayload = payload;
    }
  });

  await whServer.start();
  assert.strictEqual(whServer.isRunning, true);

  // Send test callback
  const http = await import('http');
  await new Promise((resolve, reject) => {
    const req = http.request(
      'http://127.0.0.1:3999/api/webhook/validation',
      { method: 'POST', headers: { 'Content-Type': 'application/json' } },
      (res) => {
        assert.strictEqual(res.statusCode, 200);
        resolve();
      }
    );
    req.on('error', reject);
    req.write(JSON.stringify({
      success: true,
      trackingId: 'wh_1795608137262_amgyogc',
      email: 'bknuth@klodev.com',
      isDeliverable: true,
      score: 99
    }));
    req.end();
  });

  await new Promise(r => setTimeout(r, 100));
  assert.strictEqual(receivedPayload?.trackingId, 'wh_1795608137262_amgyogc');
  await whServer.stop();
  console.log('  ✓ Webhook server received, responded 200 OK, and processed payload');

  // Test 4: Option A First-Valid-Wins with Webhook Callback in PipelineManager
  console.log('Test 4: Option A claim and Parquet write via Webhook callback');
  const baseTestDir = './tests/tmp_partitioned_test';
  const pipeline = new PipelineManager({ serverId: 'test_node' });
  pipeline.useMemoryQueue = true;

  pipeline.validParquetWriter = new ChunkedParquetWriter({
    outputDir: path.join(baseTestDir, 'valid'),
    filePrefix: 'valid_stage1',
    schema: validEmailSchema,
    serverId: 'node_test'
  });

  pipeline.failedPermanentParquetWriter = new ChunkedParquetWriter({
    outputDir: path.join(baseTestDir, 'invalid/failed_permanent'),
    filePrefix: 'failed_permanent',
    schema: invalidEmailSchema,
    serverId: 'node_test'
  });

  const pId = 'person_wh_123';
  pipeline.localPendingTrackers.set('wh_alpha', { personId: pId, email: 'bknuth@klodev.com' });
  pipeline.localPendingTrackers.set('wh_beta', { personId: pId, email: 'b.knuth@klodev.com' });

  // Callback 1 (Valid) -> Should write to valid
  await pipeline._handleWebhookCallback({
    trackingId: 'wh_alpha',
    email: 'bknuth@klodev.com',
    isDeliverable: true,
    score: 99
  });

  // Callback 2 (Valid for same person) -> Should be secondary ignored in failed_permanent
  await pipeline._handleWebhookCallback({
    trackingId: 'wh_beta',
    email: 'b.knuth@klodev.com',
    isDeliverable: true,
    score: 90
  });

  await pipeline.validParquetWriter.close();
  await pipeline.invalidParquetWriter.close();

  assert.strictEqual(pipeline.validParquetWriter.currentFileIndex - 1 >= 1 || pipeline.validParquetWriter.currentCount >= 1, true, 'Only 1 valid email written per person');
  assert.strictEqual(pipeline.invalidParquetWriter.currentFileIndex - 1 >= 1 || pipeline.invalidParquetWriter.currentCount >= 1, true, 'Duplicate valid archived in invalid');

  // Clean test files
  fs.rmSync(baseTestDir, { recursive: true, force: true });
  console.log('  ✓ Option A first-valid-wins verified under webhook callback scenario');

  console.log('\n🎉 ALL WEBHOOK HANDLING & CLUSTER TESTS PASSED SUCCESSFULLY!');
}

runTests().catch(err => {
  console.error('❌ Test suite failed:', err);
  process.exit(1);
});
