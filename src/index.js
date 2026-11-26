import { AppController } from './workflow/appController.js';

async function bootstrap() {
  const app = new AppController();
  await app.init();
  await app.run();
  await app.stop();
  process.exit(0);
}

// Handle transient Redis connection drops without crashing
process.on('uncaughtException', (err) => {
  if (err && (err.message?.includes('Connection is closed') || err.message?.includes('ECONNRESET'))) {
    console.warn('[Redis] Transient connection dropout detected, reconnecting automatically...');
    return;
  }
  console.error('Fatal application error:', err);
  process.exit(1);
});

bootstrap().catch(err => {
  console.error('Fatal application error:', err);
  process.exit(1);
});
