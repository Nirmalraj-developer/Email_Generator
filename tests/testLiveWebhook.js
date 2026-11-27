import { config } from '../src/config.js';
import { WebhookServer } from '../src/webhookServer.js';
import { V6Client } from '../src/v6Client.js';

const testEmail = process.argv[2] || 'test@gmail.com';
const timeoutMs = parseInt(process.argv[3] || '30000', 10);

async function testLiveWebhook() {
  const callbackUrl = `${config.webhook.callbackBaseUrl}${config.webhook.path}`;
  console.log('====================================================');
  console.log('🔍 LIVE WEBHOOK CONNECTIVITY TEST');
  console.log(`   Listening on: 0.0.0.0:${config.webhook.port}${config.webhook.path}`);
  console.log(`   Callback URL: ${callbackUrl}`);
  console.log(`   V6 Endpoint:  ${config.v6.url}`);
  console.log(`   Test Email:   ${testEmail}`);
  console.log('====================================================\n');

  let receivedPayload = null;
  let resolveCallback;
  const callbackPromise = new Promise(resolve => { resolveCallback = resolve; });

  const server = new WebhookServer({
    port: config.webhook.port,
    path: config.webhook.path,
    onCallback: (payload) => {
      receivedPayload = payload;
      resolveCallback(payload);
    },
    onError: (tag, msg) => console.error(`[WebhookError:${tag}]`, msg)
  });

  try {
    await server.start();
    console.log(`✅ Port ${config.webhook.port} is open and listening.`);
  } catch (err) {
    console.error(`❌ Failed to start WebhookServer on port ${config.webhook.port}: ${err.message}`);
    console.error('   Check if another process (e.g. pm2) is already using port 3000.');
    process.exit(1);
  }

  const client = new V6Client({
    url: config.v6.url,
    apiToken: config.v6.apiToken,
    callbackUrl,
    targetRps: 10,
    timeoutMs: config.v6.timeoutMs,
    maxRetries: 1,
    onError: (tag, msg, meta) => console.error(`[V6Error:${tag}]`, msg, meta)
  });

  const trackingId = `live_test_${Date.now()}`;
  console.log(`📤 Sending 1 test request to Verify V6 (trackingId: ${trackingId})...`);
  const submitRes = await client.submit(testEmail, trackingId);

  if (!submitRes.success) {
    console.error(`❌ Verify V6 API submission failed: ${submitRes.error}`);
    await server.stop();
    process.exit(1);
  }

  console.log(`✅ Verify V6 accepted request (returned trackingId: ${submitRes.trackingId}).`);
  console.log(`⏳ Waiting up to ${timeoutMs / 1000}s for Verify V6 callback to reach ${callbackUrl}...`);

  const timer = setTimeout(() => resolveCallback(null), timeoutMs);
  await callbackPromise;
  clearTimeout(timer);
  await server.stop();

  if (receivedPayload) {
    console.log('\n🎉 SUCCESS! Webhook callback RECEIVED from Verify V6:');
    console.log(JSON.stringify(receivedPayload, null, 2));
    process.exit(0);
  } else {
    console.error(`\n❌ TIMEOUT: No callback arrived at ${callbackUrl} within ${timeoutMs / 1000}s.`);
    console.error('   Troubleshooting checklist:');
    console.error(`   1. Ensure firewall allows inbound TCP ${config.webhook.port}: ufw allow ${config.webhook.port}/tcp`);
    console.error(`   2. Confirm WEBHOOK_CALLBACK_BASE_URL in .env matches this server's public IP.`);
    console.error('   3. Check cloud provider security group / firewall rules for port 3000.');
    process.exit(1);
  }
}

testLiveWebhook().catch(err => {
  console.error('❌ Live webhook test error:', err);
  process.exit(1);
});
