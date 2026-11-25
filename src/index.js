import { AppController } from './workflow/appController.js';

async function bootstrap() {
  const app = new AppController();
  await app.init();
  await app.run();
  await app.stop();
  process.exit(0);
}

bootstrap().catch(err => {
  console.error('Fatal application error:', err);
  process.exit(1);
});
