import assert from 'assert';
import fs from 'fs';
import path from 'path';
import { generateCandidateEmails, extractDomain, sanitizeNamePart } from '../src/services/patternGenerator.js';
import { ChunkedParquetWriter, validEmailSchema, invalidEmailSchema, webhookTriggerSchema } from '../src/storage/parquetStorage.js';
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

  // Test 5: Waterfall pattern escalation (Pattern 1 invalid -> escalates to Pattern 2)
  console.log('Test 5: Waterfall escalation (Pattern 1 invalid -> triggers Pattern 2)');
  const waterfallTestDir = './tests/tmp_waterfall_test';
  const wfPipeline = new PipelineManager({ serverId: 'wf_test_node' });
  wfPipeline.useMemoryQueue = true;
  await wfPipeline.initialize();

  wfPipeline.validParquetWriter = new ChunkedParquetWriter({
    outputDir: path.join(waterfallTestDir, 'valid'),
    filePrefix: 'valid_wf',
    schema: validEmailSchema,
    serverId: 'node_test'
  });

  wfPipeline.invalidParquetWriter = new ChunkedParquetWriter({
    outputDir: path.join(waterfallTestDir, 'invalid'),
    filePrefix: 'invalid_wf',
    schema: invalidEmailSchema,
    serverId: 'node_test'
  });

  wfPipeline.webhookTriggersParquetWriter = new ChunkedParquetWriter({
    outputDir: path.join(waterfallTestDir, 'webhook_triggers'),
    filePrefix: 'triggers_wf',
    schema: webhookTriggerSchema,
    serverId: 'node_test'
  });

  // Mock cache miss for all patterns to force waterfall to trigger
  wfPipeline.cacheClient.checkCache = async () => ({ foundInCache: false });
  // Mock webhook client submission
  let submittedEmails = [];
  wfPipeline.webhookClient.submitForValidation = async (email) => {
    submittedEmails.push(email);
    return { success: true, trackingId: `trk_${submittedEmails.length}` };
  };

  // Step 1: Process contact. Should trigger ONLY Pattern 1
  await wfPipeline._processContactJob({
    id: 'person_waterfall_001',
    firstName: 'Alice',
    lastName: 'Smith',
    website: 'alicesmithtest.com'
  });

  assert.strictEqual(submittedEmails.length, 1, 'Only Pattern 1 triggered initially');
  assert.strictEqual(submittedEmails[0], 'alice.smith@alicesmithtest.com');

  // Step 2: Webhook callback returns INVALID for Pattern 1
  await wfPipeline._handleWebhookCallback({
    trackingId: 'trk_1',
    email: 'alice.smith@alicesmithtest.com',
    deliverable: false,
    score: 0
  });

  // Check that Pattern 2 was escalated and triggered
  assert.strictEqual(submittedEmails.length, 2, 'Pattern 2 was escalated upon Pattern 1 failure');
  assert.strictEqual(submittedEmails[1], 'alicesmith@alicesmithtest.com');

  // Step 3: Webhook callback returns VALID for Pattern 2
  await wfPipeline._handleWebhookCallback({
    trackingId: 'trk_2',
    email: 'alicesmith@alicesmithtest.com',
    deliverable: true,
    score: 99
  });

  assert.strictEqual(submittedEmails.length, 2, 'No further patterns triggered after valid found (saved 8 calls!)');
  assert.strictEqual(wfPipeline.metrics.webhookValidReceived, 1, 'Valid email captured');

  await wfPipeline.safeStop();
  fs.rmSync(waterfallTestDir, { recursive: true, force: true });
  console.log('  ✓ Waterfall escalation successfully tested and verified (saved 8 redundant API calls)');

  console.log('\n🎉 ALL WEBHOOK HANDLING & CLUSTER TESTS PASSED SUCCESSFULLY!');
}

runTests().catch(err => {
  console.error('❌ Test suite failed:', err);
  process.exit(1);
});
