import { V6WaterfallRunner } from './runner.js';

const runner = new V6WaterfallRunner();

let stopping = false;
async function shutdown(signal) {
  if (stopping) return;
  stopping = true;
  console.log(`\nReceived ${signal}...`);
  await runner.safeStop();
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

runner.start()
  .then(() => runner.safeStop())
  .then(() => process.exit(0))
  .catch(async (err) => {
    console.error('Fatal runner error:', err);
    await runner.safeStop();
    process.exit(1);
  });
