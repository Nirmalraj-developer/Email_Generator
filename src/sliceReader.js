import fs from 'fs';
import path from 'path';
import readline from 'readline';

/**
 * Checks whether a JSONL contact record is already completed and should be skipped.
 */
export function isRecordCompleted(doc) {
  if (!doc || typeof doc !== 'object') return true;
  if (doc.primary_email !== null && doc.primary_email !== undefined && String(doc.primary_email).trim() !== '') {
    return true;
  }
  const status = String(doc.email_smt_status || '').toLowerCase();
  if (status === 'completed' || status === 'valid' || status === 'verified') {
    return true;
  }
  return false;
}

/**
 * Extracts only patterns whose score is null/undefined, preserving pattern order.
 */
export function extractNullPatterns(doc) {
  if (!Array.isArray(doc?.patterns)) return [];
  return doc.patterns
    .filter(p => p && p.email && (p.score === null || p.score === undefined))
    .map(p => ({
      pattern: String(p.pattern || ''),
      email: String(p.email).trim()
    }));
}

/**
 * Loads processed person IDs from checkpoint log for safe resume.
 */
export function loadCheckpointSet(checkpointFile) {
  const set = new Set();
  if (!fs.existsSync(checkpointFile)) return set;
  const content = fs.readFileSync(checkpointFile, 'utf8');
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (trimmed) set.add(trimmed);
  }
  return set;
}

/**
 * Lists all JSONL/JSON slice files in inputDir, excluding all_verifications_unique.jsonl.
 */
export function getSliceFiles(inputDir, excludedFiles = ['all_verifications_unique.jsonl']) {
  if (!fs.existsSync(inputDir)) {
    throw new Error(`Input directory not found: ${inputDir}`);
  }
  const excludedSet = new Set(excludedFiles.map(f => f.toLowerCase()));
  return fs.readdirSync(inputDir)
    .filter(name => {
      if (name.startsWith('.')) return false;
      if (excludedSet.has(name.toLowerCase())) return false;
      const lower = name.toLowerCase();
      return lower.endsWith('.jsonl') || lower.endsWith('.json');
    })
    .sort()
    .map(name => path.join(inputDir, name));
}

/**
 * Streams pending contacts from all slice files one by one with minimal RAM.
 */
export async function* streamPendingContacts({ inputDir, excludedFiles, processedSet, shouldStop, onSkipped }) {
  const files = getSliceFiles(inputDir, excludedFiles);
  for (const filePath of files) {
    if (shouldStop && shouldStop()) break;
    const fileStream = fs.createReadStream(filePath, { encoding: 'utf8', highWaterMark: 256 * 1024 });
    const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

    try {
      for await (const line of rl) {
        if (shouldStop && shouldStop()) break;
        const trimmed = line.trim();
        if (!trimmed) continue;

        let doc;
        try {
          doc = JSON.parse(trimmed);
        } catch {
          if (onSkipped) onSkipped('parse_error');
          continue;
        }

        const personId = String(doc.person_id || doc._id || '').trim();
        if (!personId) {
          if (onSkipped) onSkipped('missing_id');
          continue;
        }

        if (processedSet && processedSet.has(personId)) {
          if (onSkipped) onSkipped('checkpoint');
          continue;
        }

        if (isRecordCompleted(doc)) {
          if (onSkipped) onSkipped('completed');
          continue;
        }

        const pendingPatterns = extractNullPatterns(doc);
        if (pendingPatterns.length === 0) {
          if (onSkipped) onSkipped('no_null_patterns');
          continue;
        }

        yield {
          personId,
          companyId: doc.company_id ? String(doc.company_id) : '',
          pendingPatterns,
          sourceFile: path.basename(filePath)
        };
      }
    } finally {
      rl.close();
      fileStream.destroy();
    }
  }
}
