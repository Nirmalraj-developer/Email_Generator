/**
 * @file patternGenerator.js
 * @description Generates email candidate permutations based on first name, last name, and domain,
 *              and provides utility functions for sanitizing names and extracting root domains.
 */

/**
 * 10 canonical email pattern templates specified for validation.
 * Supported placeholder tokens:
 *   - <first>: full first name (e.g., 'priscila')
 *   - <last>:  full last name  (e.g., 'simoes')
 *   - <f>:     first initial   (e.g., 'p')
 *   - <l>:     last initial    (e.g., 's')
 *   - @:       terminating symbol before domain
 */
export const DEFAULT_PATTERN_TEMPLATES = [
  "<first>.<last>@",
  "<first><last>@",
  "<f><last>@",
  "<first>@",
  "<f>.<last>@",
  "<first><l>@",
  "<first>.<l>@",
  "<last>.<first>@",
  "<last><first>@",
  "<first>_<last>@"
];

/**
 * Normalizes and extracts clean hostname domain from arbitrary URL/website strings.
 * Strips protocol, www prefix, URL paths, query parameters, and ports.
 *
 * @example
 *   extractDomain('https://www.elanco.com/about?x=1') // returns 'elanco.com'
 *   extractDomain('sub.example.co.uk:8080/home')       // returns 'sub.example.co.uk'
 *
 * @param {string} website - Raw website URL or domain string.
 * @returns {string|null} Normalized domain, or null if invalid.
 */
export function extractDomain(website) {
  if (!website || typeof website !== 'string') return null;
  let clean = website.trim().toLowerCase();
  
  // Remove protocol (http/https) and leading 'www.'
  clean = clean.replace(/^(?:https?:\/\/)?(?:www\.)?/i, '');
  
  // Extract domain portion prior to path, query string, or port
  const match = clean.match(/^([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+)/i);
  if (match) {
    return match[1];
  }
  
  const slashIdx = clean.indexOf('/');
  if (slashIdx !== -1) clean = clean.substring(0, slashIdx);
  const colonIdx = clean.indexOf(':');
  if (colonIdx !== -1) clean = clean.substring(0, colonIdx);

  return clean.length > 3 && clean.includes('.') ? clean : null;
}

/**
 * Sanitizes a person's name part for email local-part generation.
 * - Normalizes unicode (decomposing diacritics / accents)
 * - Removes accents (e.g. 'Simões' -> 'simoes')
 * - Strips non-alphanumeric characters and converts to lowercase
 *
 * @param {string} str - Name string (first name or last name).
 * @returns {string} Sanitized string containing only lowercase a-z and 0-9.
 */
export function sanitizeNamePart(str) {
  if (!str || typeof str !== 'string') return '';
  return str
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')
    .trim();
}

/**
 * Generates an array of email candidate objects from first name, last name, and website.
 *
 * Rules:
 *   - If first name is missing, returns [] (cannot form valid name patterns).
 *   - If last name is missing, only templates without <last> or <l> (such as <first>@) are generated.
 *   - Deduplicates identical candidates if name initials collide.
 *
 * @param {string} firstName - Contact's first name.
 * @param {string} lastName  - Contact's last name.
 * @param {string} website   - Contact's website or domain.
 * @param {string[]} [templates=DEFAULT_PATTERN_TEMPLATES] - List of pattern templates to use.
 * @returns {Array<{ email: string, localPart: string, domain: string, pattern: string, template: string }>}
 */
export function generateCandidateEmails(firstName, lastName, website, templates = DEFAULT_PATTERN_TEMPLATES) {
  const domain = extractDomain(website);
  if (!domain) {
    return [];
  }

  const first = sanitizeNamePart(firstName);
  const last = sanitizeNamePart(lastName);

  if (!first) {
    return [];
  }

  const f = first.charAt(0);
  const l = last.length > 0 ? last.charAt(0) : '';

  const results = [];
  const seen = new Set();

  for (const template of templates) {
    // If template requires last name or initial and last name is missing, skip
    if ((template.includes('<last>') || template.includes('<l>')) && !last) {
      continue;
    }

    const localPart = template
      .replace(/<first>/g, first)
      .replace(/<last>/g, last)
      .replace(/<f>/g, f)
      .replace(/<l>/g, l)
      .replace(/@$/, '');

    // Skip invalid local parts (e.g. starting/ending with dot or underscore)
    if (!localPart || localPart.startsWith('.') || localPart.endsWith('.') || localPart.startsWith('_') || localPart.endsWith('_')) {
      continue;
    }

    const email = `${localPart}@${domain}`;
    if (!seen.has(email)) {
      seen.add(email);
      results.push({
        email,
        localPart,
        domain,
        pattern: template + domain,
        template
      });
    }
  }

  return results;
}
