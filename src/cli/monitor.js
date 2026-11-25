/**
 * @file monitor.js
 * @description Real-time, professional terminal dashboard:
 *              - Live throughput speedometer (Records/Sec, Records/Min)
 *              - 5-Day Completion Progress Meter toward 150M target
 *              - Real-time queue status (Waiting, Active, Completed, Failed)
 *              - Partitioned Parquet file counters on disk
 *              - Error rates with clean status
 */

import { PipelineManager } from '../queue/pipelineManager.js';
import { MongoClient } from 'mongodb';
import { config } from '../config/index.js';
import chalk from 'chalk';
import fs from 'fs';
import path from 'path';

function countFilesAndRows(dirPath) {
  if (!fs.existsSync(dirPath)) return { fileCount: 0 };
  const files = fs.readdirSync(dirPath).filter(f => f.endsWith('.parquet'));
  return { fileCount: files.length };
}

function countErrorLines(logPath) {
  if (!fs.existsSync(logPath)) return 0;
  try {
    const content = fs.readFileSync(logPath, 'utf8');
    return content.split('\n').filter(Boolean).length;
  } catch (err) {
    return 0;
  }
}

async function runDashboard() {
  const pipeline = new PipelineManager({ serverId: config.serverId });
  await pipeline.initialize();

  let checkpointCol = null;
  if (config.nodeRole === 'master') {
    try {
      const client = new MongoClient(config.sourceMongo.uri);
      await client.connect();
      checkpointCol = client
        .db(config.sourceMongo.checkpointDb)
        .collection(config.sourceMongo.checkpointCollection);
    } catch (e) {
      // Worker only or mongo disconnected
    }
  }

  let prevProcessed = 0;
  let prevTimestamp = Date.now();
  const TARGET_TOTAL = 150000000;
  const errorLogFile = path.resolve('./logs/error.log');

  console.clear();

  const updateInterval = 2000;

  setInterval(async () => {
    try {
      const now = Date.now();
      const status = await pipeline.getStatus();
      
      let checkpoint = null;
      if (checkpointCol) {
        checkpoint = await checkpointCol.findOne({ key: 'mongo_stream_parquet_checkpoint' });
      }

      const totalProcessedThisNode = status.metrics.contactsReceived;
      
      // Calculate real-time throughput rate
      const elapsedSec = (now - prevTimestamp) / 1000;
      const deltaProcessed = totalProcessedThisNode - prevProcessed;
      const recordsPerSec = elapsedSec > 0 ? (deltaProcessed / elapsedSec).toFixed(1) : '0.0';
      const recordsPerMin = (Number(recordsPerSec) * 60).toFixed(0);

      prevProcessed = totalProcessedThisNode;
      prevTimestamp = now;

      // Parquet physical storage breakdown on disk
      const validFiles = countFilesAndRows(config.parquet.validDir);
      const waitingFiles = countFilesAndRows(config.parquet.waitingWebhookDir);
      const permInvalidFiles = countFilesAndRows(config.parquet.permanentInvalidCacheDir);
      const failedPermFiles = countFilesAndRows(config.parquet.failedPermanentDir);
      const totalErrorsLogged = countErrorLines(errorLogFile);

      // Percentage toward 150M target
      const globalIngested = checkpoint?.totalIngested || totalProcessedThisNode;
      const progressPercent = ((globalIngested / TARGET_TOTAL) * 100).toFixed(3);

      console.clear();
      console.log(chalk.bold.cyan('╔════════════════════════════════════════════════════════════════════════════╗'));
      console.log(chalk.bold.cyan(`║  ⚡ EMAIL GENERATOR & VERIFICATION DASHBOARD  [150M TARGET]                 ║`));
      console.log(chalk.bold.cyan(`║  Node: ${chalk.yellow(config.serverId.padEnd(18))} Role: ${chalk.green(config.nodeRole.toUpperCase().padEnd(8))} Time: ${new Date().toLocaleTimeString().padEnd(10)} ║`));
      console.log(chalk.bold.cyan('╠════════════════════════════════════════════════════════════════════════════╣'));

      // Throughput & Target Speedometer
      console.log(`║  ${chalk.bold('CURRENT THROUGHPUT:')}                                                         ║`);
      console.log(`║    Speed:   ${chalk.bold.green(recordsPerSec.padStart(6))} records/sec   |   ${chalk.bold.green(recordsPerMin.padStart(7))} records/min            ║`);
      console.log(`║    Target:  ${chalk.cyan('~350.0')} records/sec (Cluster) |  ${chalk.cyan('~117.0')} records/sec (This Node)         ║`);
      console.log(`║    Overall: ${chalk.yellow(globalIngested.toLocaleString().padStart(12))} / ${TARGET_TOTAL.toLocaleString()} (${chalk.bold.green(progressPercent + '%')})                       ║`);
      console.log(chalk.bold.cyan('╠════════════════════════════════════════════════════════════════════════════╣'));

      // Real-time Queue Counters
      console.log(`║  ${chalk.bold('REDIS BULLMQ QUEUE:')}                                                        ║`);
      console.log(`║    Waiting:   ${chalk.blue(String(status.queueStatus.waiting).padStart(8))}   Active:    ${chalk.green(String(status.queueStatus.active).padStart(8))}                        ║`);
      console.log(`║    Completed: ${chalk.green(String(status.queueStatus.completed).padStart(8))}   Failed:    ${chalk.red(String(status.queueStatus.failed).padStart(8))}                        ║`);
      console.log(chalk.bold.cyan('╠════════════════════════════════════════════════════════════════════════════╣'));

      // Stage 1 Verification Breakdown
      console.log(`║  ${chalk.bold('VERIFICATION BREAKDOWN (THIS NODE):')}                                      ║`);
      console.log(`║    Contacts Evaluated:             ${chalk.white(String(status.metrics.contactsReceived).padStart(12))}                    ║`);
      console.log(`║    Candidate Patterns Tested:      ${chalk.white(String(status.metrics.candidatesGenerated).padStart(12))}                    ║`);
      console.log(`║    Cache Valid Hits (score >= 90): ${chalk.bold.green(String(status.metrics.validFromCache).padStart(12))}                    ║`);
      console.log(`║    Cache All 10 Failed (Perm):     ${chalk.yellow(String(status.metrics.allCachedInvalid).padStart(12))}                    ║`);
      console.log(`║    Sent to Verify V6 Webhook:      ${chalk.magenta(String(status.metrics.webhookSubmitted).padStart(12))}                    ║`);
      console.log(chalk.bold.cyan('╠════════════════════════════════════════════════════════════════════════════╣'));

      // Partitioned Parquet Storage on Disk
      console.log(`║  ${chalk.bold('PARTITIONED PARQUET ON DISK:')}                                             ║`);
      console.log(`║    data/valid/                     ${chalk.green(String(validFiles.fileCount).padStart(4))} files (${chalk.gray('Working Leads')})               ║`);
      console.log(`║    data/invalid/permanent_cache/   ${chalk.yellow(String(permInvalidFiles.fileCount).padStart(4))} files (${chalk.gray('Input for Process 2')})         ║`);
      console.log(`║    data/invalid/waiting_v6/        ${chalk.magenta(String(waitingFiles.fileCount).padStart(4))} files (${chalk.gray('Awaiting V6 Callback')})       ║`);
      console.log(`║    data/invalid/failed_permanent/  ${chalk.red(String(failedPermFiles.fileCount).padStart(4))} files (${chalk.gray('Dead Leads')})                  ║`);
      console.log(chalk.bold.cyan('╠════════════════════════════════════════════════════════════════════════════╣'));

      // Error Log Status
      const errorStatusStr = totalErrorsLogged === 0 
        ? chalk.green('0 errors (Clean)') 
        : chalk.red(`${totalErrorsLogged} logged to logs/error.log`);
      console.log(`║  ${chalk.bold('ERROR LOG STATUS:')} ${errorStatusStr.padEnd(52)}║`);
      console.log(chalk.bold.cyan('╚════════════════════════════════════════════════════════════════════════════╝'));
      console.log(chalk.gray('Press Ctrl+C to exit monitor (Pipeline runs continuously in background)'));

    } catch (err) {
      console.error(chalk.red('[Monitor Update Error]:'), err.message);
    }
  }, updateInterval);
}

runDashboard().catch(err => {
  console.error('Fatal dashboard error:', err);
  process.exit(1);
});
