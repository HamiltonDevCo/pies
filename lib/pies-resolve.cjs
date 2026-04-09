#!/usr/bin/env node
/**
 * PIES Entity Resolution Gateway
 *
 * The single source of truth for person resolution and upsert.
 * ALL writes to the people collection must go through resolveAndUpsert().
 *
 * Resolution order:
 *   1. Exact phone match (E.164 normalized, checks phones[], identities.phones, phone)
 *   2. Exact email match (lowercased, checks emails[], identities.emails)
 *   3. Fuzzy name match (Jaro-Winkler ≥ 0.88 within same source or city)
 *   4. No match → create new canonical record
 *
 * On match: merge fields (keep best per field, store provenance)
 * On create: full schema with all designed fields initialized
 *
 * Usage:
 *   const { resolveAndUpsert, resolveOnly, getConnection } = require('./lib/pies-resolve');
 *   const person = await resolveAndUpsert(db, { phone: '+15023372875', name: 'Sammy Hamilton' });
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// -------------------------------------------------------------------
// Phone normalization (E.164)
// -------------------------------------------------------------------

function normalizePhone(raw) {
  if (!raw || typeof raw !== 'string') return null;
  // Strip everything except digits and leading +
  let digits = raw.replace(/[^\d+]/g, '');
  if (!digits) return null;
  // Remove leading + for digit processing
  const hasPlus = digits.startsWith('+');
  if (hasPlus) digits = digits.slice(1);
  // US numbers: 10 digits → +1, 11 digits starting with 1 → +1
  if (digits.length === 10) digits = '1' + digits;
  if (digits.length === 11 && digits.startsWith('1')) {
    return '+' + digits;
  }
  // International: just prepend + if reasonable length
  if (digits.length >= 7 && digits.length <= 15) {
    return '+' + digits;
  }
  return null;
}

function normalizeEmail(raw) {
  if (!raw || typeof raw !== 'string') return null;
  const trimmed = raw.trim().toLowerCase();
  if (!trimmed.includes('@')) return null;
  return trimmed;
}

// -------------------------------------------------------------------
// Jaro-Winkler similarity (for fuzzy name matching)
// -------------------------------------------------------------------

function jaroWinkler(s1, s2) {
  if (!s1 || !s2) return 0;
  s1 = s1.toLowerCase().trim();
  s2 = s2.toLowerCase().trim();
  if (s1 === s2) return 1;

  const len1 = s1.length;
  const len2 = s2.length;
  const maxDist = Math.floor(Math.max(len1, len2) / 2) - 1;
  if (maxDist < 0) return 0;

  const s1Matches = new Array(len1).fill(false);
  const s2Matches = new Array(len2).fill(false);
  let matches = 0;
  let transpositions = 0;

  for (let i = 0; i < len1; i++) {
    const start = Math.max(0, i - maxDist);
    const end = Math.min(i + maxDist + 1, len2);
    for (let j = start; j < end; j++) {
      if (s2Matches[j] || s1[i] !== s2[j]) continue;
      s1Matches[i] = true;
      s2Matches[j] = true;
      matches++;
      break;
    }
  }

  if (matches === 0) return 0;

  let k = 0;
  for (let i = 0; i < len1; i++) {
    if (!s1Matches[i]) continue;
    while (!s2Matches[k]) k++;
    if (s1[i] !== s2[k]) transpositions++;
    k++;
  }

  const jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3;

  // Winkler prefix boost (up to 4 chars)
  let prefix = 0;
  for (let i = 0; i < Math.min(4, Math.min(len1, len2)); i++) {
    if (s1[i] === s2[i]) prefix++;
    else break;
  }

  return jaro + prefix * 0.1 * (1 - jaro);
}

// -------------------------------------------------------------------
// Nickname mapping for common name variants
// -------------------------------------------------------------------

const NICKNAME_MAP = new Map([
  ['rob', 'robert'], ['robert', 'rob'],
  ['joe', 'joseph'], ['joseph', 'joe'],
  ['sam', 'samuel'], ['samuel', 'sam'],
  ['sammy', 'samuel'], ['samuel', 'sammy'],
  ['bill', 'william'], ['william', 'bill'],
  ['bob', 'robert'], ['robert', 'bob'],
  ['dick', 'richard'], ['richard', 'dick'],
  ['mike', 'michael'], ['michael', 'mike'],
  ['jim', 'james'], ['james', 'jim'],
  ['ben', 'benjamin'], ['benjamin', 'ben'],
  ['dan', 'daniel'], ['daniel', 'dan'],
  ['tom', 'thomas'], ['thomas', 'tom'],
  ['dave', 'david'], ['david', 'dave'],
  ['nick', 'nicholas'], ['nicholas', 'nick'],
  ['pat', 'patrick'], ['patrick', 'pat'],
  ['ed', 'edward'], ['edward', 'ed'],
  ['ted', 'theodore'], ['theodore', 'ted'],
  ['tony', 'anthony'], ['anthony', 'tony'],
  ['steve', 'steven'], ['steven', 'steve'],
  ['jon', 'jonathan'], ['jonathan', 'jon'],
  ['zach', 'zachary'], ['zachary', 'zach'],
  ['matt', 'matthew'], ['matthew', 'matt'],
  ['chris', 'christopher'], ['christopher', 'chris'],
  ['alex', 'alexander'], ['alexander', 'alex'],
  ['kate', 'katherine'], ['katherine', 'kate'],
  ['liz', 'elizabeth'], ['elizabeth', 'liz'],
  ['jen', 'jennifer'], ['jennifer', 'jen'],
  ['meg', 'margaret'], ['margaret', 'meg'],
  ['maggie', 'margaret'], ['margaret', 'maggie'],
]);

/**
 * Check if two first names are nickname variants of each other.
 */
function areNicknameVariants(name1, name2) {
  const n1 = name1.toLowerCase();
  const n2 = name2.toLowerCase();
  if (n1 === n2) return true;
  return NICKNAME_MAP.get(n1) === n2 || NICKNAME_MAP.get(n2) === n1;
}

// -------------------------------------------------------------------
// Canonical person schema (all fields from PIES-DESIGN.md)
// -------------------------------------------------------------------

function canonicalPersonSchema(overrides = {}) {
  const now = new Date();
  return {
    // Identity
    name: overrides.name || 'Unknown',
    aliases: overrides.aliases || [],

    // Identifiers (structured)
    phones: overrides.phones || [],       // [{number, type, primary}]
    emails: overrides.emails || [],       // [{address, type, primary}]
    discord_ids: overrides.discord_ids || [],
    slack_ids: overrides.slack_ids || [],
    basecamp_ids: overrides.basecamp_ids || [],

    // Fast-lookup identities (flat arrays for indexing)
    identities: {
      phones: overrides._identityPhones || [],  // ['+15023372875']
      emails: overrides._identityEmails || [],  // ['user@example.com']
      social: overrides._identitySocial || {},
    },

    // Classification
    tier: overrides.tier || 'unknown',
    auto_tier: overrides.auto_tier || null,
    tier_locked: overrides.tier_locked || false,
    tags: overrides.tags || [],

    // Relationships
    relationship: overrides.relationship || null,   // 'family', 'business', 'friend'
    role: overrides.role || null,                    // 'wife', 'son', 'client'
    company_id: overrides.company_id || null,
    company_ids: overrides.company_ids || [],
    reports_to: overrides.reports_to || null,

    // Intelligence scores
    confidence_score: overrides.confidence_score || 0,
    relationship_strength: overrides.relationship_strength || 0,
    engagement_score: overrides.engagement_score || 0,
    recency_score: overrides.recency_score || 0,
    sentiment: overrides.sentiment || null,

    // Tracking
    first_seen: overrides.first_seen || now,
    first_channel: overrides.first_channel || null,
    last_interaction: overrides.last_interaction || null,
    interaction_count: overrides.interaction_count || 0,

    // Context
    notes: overrides.notes || '',
    topics: overrides.topics || [],
    communication_style: overrides.communication_style || null,
    response_pattern: overrides.response_pattern || null,

    // Employment & social
    employment: overrides.employment || [],
    relationships_array: overrides.relationships_array || [],   // inline relationships (legacy)
    linkedin_url: overrides.linkedin_url || null,
    twitter_handle: overrides.twitter_handle || null,
    photo_url: overrides.photo_url || null,

    // Enrichment
    last_enriched: overrides.last_enriched || null,
    enrichment_sources: overrides.enrichment_sources || [],

    // Location
    city: overrides.city || null,
    state: overrides.state || null,
    country: overrides.country || null,
    postalCode: overrides.postalCode || null,

    // External IDs
    external_ids: overrides.external_ids || {},

    // Metadata
    source: overrides.source || 'auto',
    sources: overrides.sources || [overrides.source || 'auto'],
    metadata: overrides.metadata || {},
    merge_provenance: [],   // Track all merges

    // Timestamps
    created_at: overrides.created_at || now,
    updated_at: now,
  };
}

// -------------------------------------------------------------------
// Core: Find matching person across all identifier formats
// -------------------------------------------------------------------

async function findByPhone(collection, phone) {
  const normalized = normalizePhone(phone);
  if (!normalized) return null;

  return collection.findOne({
    $or: [
      { 'phones.number': normalized },
      { 'identities.phones': normalized },
      { phone: normalized },
      // Also check without +1 prefix for legacy data
      { 'phones.number': normalized.replace(/^\+1/, '') },
      // Check un-normalized flat array
      { phones: normalized },
    ]
  });
}

async function findByEmail(collection, email) {
  const normalized = normalizeEmail(email);
  if (!normalized) return null;

  return collection.findOne({
    $or: [
      { 'emails.address': normalized },
      { 'identities.emails': normalized },
      { email: normalized },
      { emails: normalized },
    ]
  });
}

async function findByFuzzyName(collection, name, hints = {}) {
  if (!name || name === 'Unknown') return null;

  const nameParts = name.trim().split(/\s+/);
  const firstName = nameParts[0];
  const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1] : null;

  if (!lastName || lastName.length < 2) return null;

  // Narrow by last name (escape special regex characters)
  const escapedLast = lastName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const candidates = await collection.find({
    name: { $regex: escapedLast, $options: 'i' }
  }).limit(200).toArray();

  let bestMatch = null;
  let bestScore = 0;
  const THRESHOLD = 0.85;

  for (const candidate of candidates) {
    let score = jaroWinkler(name, candidate.name);

    // Boost score if first names are nickname variants
    const candParts = (candidate.name || '').trim().split(/\s+/);
    const candFirst = candParts[0];
    if (candFirst && firstName && areNicknameVariants(firstName, candFirst)) {
      // Nickname match: treat as very high confidence if last names match
      const candLast = candParts.length > 1 ? candParts[candParts.length - 1] : '';
      if (candLast.toLowerCase() === lastName.toLowerCase()) {
        score = Math.max(score, 0.96); // Strong boost — nicknames with same last name are near-certain
      }
    }

    if (score > bestScore && score >= THRESHOLD) {
      bestScore = score;
      bestMatch = candidate;
    }
  }

  return bestMatch;
}

// -------------------------------------------------------------------
// Merge logic: combine incoming data into existing record
// -------------------------------------------------------------------

function mergeIntoExisting(existing, incoming) {
  const updates = {};
  const provenance = {
    merged_at: new Date(),
    source: incoming.source || 'unknown',
    fields_updated: [],
  };

  // Name: prefer non-"Unknown", prefer longer/more complete name
  if (incoming.name && incoming.name !== 'Unknown' &&
      (!existing.name || existing.name === 'Unknown' ||
       (incoming.name.split(/\s+/).length > existing.name.split(/\s+/).length))) {
    updates.name = incoming.name;
    provenance.fields_updated.push('name');
  }

  // Phones: add any new normalized phones
  if (incoming.phone) {
    const normalized = normalizePhone(incoming.phone);
    if (normalized) {
      const existingPhones = (existing.phones || []).map(p => p.number || p);
      const existingIdentPhones = existing.identities?.phones || [];
      const allExisting = new Set([...existingPhones, ...existingIdentPhones]);

      if (!allExisting.has(normalized)) {
        // Add to phones[] array
        const newPhones = [...(existing.phones || [])];
        newPhones.push({ number: normalized, type: 'mobile', primary: newPhones.length === 0 });
        updates.phones = newPhones;

        // Add to identities.phones
        const newIdentPhones = [...(existing.identities?.phones || [])];
        newIdentPhones.push(normalized);
        updates['identities.phones'] = newIdentPhones;

        provenance.fields_updated.push('phones');
      }
    }
  }

  // Emails: add any new normalized emails
  if (incoming.email) {
    const normalized = normalizeEmail(incoming.email);
    if (normalized) {
      const existingEmails = (existing.emails || []).map(e => e.address || e);
      const existingIdentEmails = existing.identities?.emails || [];
      const allExisting = new Set([...existingEmails, ...existingIdentEmails]);

      if (!allExisting.has(normalized)) {
        const newEmails = [...(existing.emails || [])];
        newEmails.push({ address: normalized, type: 'personal', primary: newEmails.length === 0 });
        updates.emails = newEmails;

        const newIdentEmails = [...(existing.identities?.emails || [])];
        newIdentEmails.push(normalized);
        updates['identities.emails'] = newIdentEmails;

        provenance.fields_updated.push('emails');
      }
    }
  }

  // Tags: merge, dedupe
  if (incoming.tags?.length) {
    const merged = [...new Set([...(existing.tags || []), ...incoming.tags])];
    if (merged.length > (existing.tags || []).length) {
      updates.tags = merged;
      provenance.fields_updated.push('tags');
    }
  }

  // Sources: track all data sources
  if (incoming.source) {
    const sources = [...new Set([...(existing.sources || [existing.source || 'unknown']), incoming.source])];
    updates.sources = sources;
  }

  // Location: fill gaps
  for (const field of ['city', 'state', 'country', 'postalCode']) {
    if (incoming[field] && !existing[field]) {
      updates[field] = incoming[field];
      provenance.fields_updated.push(field);
    }
  }

  // Relationship fields: only update if not already set (user-confirmed takes priority)
  for (const field of ['relationship', 'role', 'tier']) {
    if (incoming[field] && !existing[field]) {
      updates[field] = incoming[field];
      provenance.fields_updated.push(field);
    }
  }

  // External IDs: merge
  if (incoming.external_ids && Object.keys(incoming.external_ids).length) {
    updates.external_ids = { ...(existing.external_ids || {}), ...incoming.external_ids };
    provenance.fields_updated.push('external_ids');
  }

  // Metadata: deep merge
  if (incoming.metadata && Object.keys(incoming.metadata).length) {
    updates.metadata = { ...(existing.metadata || {}), ...incoming.metadata };
    provenance.fields_updated.push('metadata');
  }

  updates.updated_at = new Date();

  return { updates, provenance };
}

// -------------------------------------------------------------------
// Main: resolveAndUpsert
// -------------------------------------------------------------------

/**
 * Resolve a person by identifiers and upsert into PIES.
 *
 * @param {Db} db - MongoDB database instance
 * @param {Object} data - Person data
 * @param {string} [data.phone] - Phone number (any format, will normalize)
 * @param {string} [data.email] - Email address
 * @param {string} [data.name] - Person name
 * @param {string} [data.source] - Data source (e.g., 'sms', 'gmail', 'ghl')
 * @param {Object} [data.metadata] - Additional metadata
 * @param {Object} [data.*] - Any other person fields to set/merge
 * @returns {Object} { person, action: 'matched'|'created'|'merged', matchMethod }
 */
async function resolveAndUpsert(db, data) {
  const people = db.collection('people');

  // Step 1: Exact phone match
  if (data.phone) {
    const match = await findByPhone(people, data.phone);
    if (match) {
      const { updates, provenance } = mergeIntoExisting(match, data);
      if (Object.keys(updates).length > 1) { // > 1 because updated_at is always there
        await people.updateOne(
          { _id: match._id },
          {
            $set: updates,
            $push: { merge_provenance: provenance }
          }
        );
      }
      const person = await people.findOne({ _id: match._id });
      return { person, action: 'matched', matchMethod: 'phone' };
    }
  }

  // Step 2: Exact email match
  if (data.email) {
    const match = await findByEmail(people, data.email);
    if (match) {
      const { updates, provenance } = mergeIntoExisting(match, data);
      if (Object.keys(updates).length > 1) {
        await people.updateOne(
          { _id: match._id },
          {
            $set: updates,
            $push: { merge_provenance: provenance }
          }
        );
      }
      const person = await people.findOne({ _id: match._id });
      return { person, action: 'matched', matchMethod: 'email' };
    }
  }

  // Step 3: Fuzzy name match
  if (data.name && data.name !== 'Unknown') {
    const match = await findByFuzzyName(people, data.name, { source: data.source, city: data.city });
    if (match) {
      const { updates, provenance } = mergeIntoExisting(match, data);
      provenance.match_type = 'fuzzy_name';
      provenance.match_score = jaroWinkler(data.name, match.name);

      if (Object.keys(updates).length > 1) {
        await people.updateOne(
          { _id: match._id },
          {
            $set: updates,
            $push: { merge_provenance: provenance }
          }
        );
      }
      const person = await people.findOne({ _id: match._id });
      return { person, action: 'merged', matchMethod: 'fuzzy_name' };
    }
  }

  // Step 4: No match — create new canonical record
  const normalized = {};

  // Build phones
  if (data.phone) {
    const norm = normalizePhone(data.phone);
    if (norm) {
      normalized.phones = [{ number: norm, type: 'mobile', primary: true }];
      normalized._identityPhones = [norm];
    }
  }

  // Build emails
  if (data.email) {
    const norm = normalizeEmail(data.email);
    if (norm) {
      normalized.emails = [{ address: norm, type: 'personal', primary: true }];
      normalized._identityEmails = [norm];
    }
  }

  const record = canonicalPersonSchema({
    ...data,
    ...normalized,
    first_channel: data.channel || data.source || null,
  });

  // Remove internal fields
  delete record._identityPhones;
  delete record._identityEmails;

  const result = await people.insertOne(record);
  record._id = result.insertedId;

  return { person: record, action: 'created', matchMethod: null };
}

/**
 * Resolve only — find the person without creating/updating.
 * Returns null if no match.
 */
async function resolveOnly(db, data) {
  const people = db.collection('people');

  if (data.phone) {
    const match = await findByPhone(people, data.phone);
    if (match) return { person: match, matchMethod: 'phone' };
  }
  if (data.email) {
    const match = await findByEmail(people, data.email);
    if (match) return { person: match, matchMethod: 'email' };
  }
  if (data.name && data.name !== 'Unknown') {
    const match = await findByFuzzyName(people, data.name);
    if (match) return { person: match, matchMethod: 'fuzzy_name' };
  }
  return null;
}

// -------------------------------------------------------------------
// Connection helper (shared 1Password-based connection)
// -------------------------------------------------------------------

const OP_SA = path.join(process.env.HOME || '', '.local', 'bin', 'op-sa');
const OP_CMD = fs.existsSync(OP_SA) ? `"${OP_SA}"` : 'op';

function getConnectionString() {
  // Fast path: env var
  if (process.env.MONGO_URI) return process.env.MONGO_URI;
  if (process.env.MONGODB_URI) return process.env.MONGODB_URI;

  // Local Docker MongoDB on port 27018, no auth needed (secured via Tailscale)
  return 'mongodb://localhost:27018/clawdbot?directConnection=true';
}

// -------------------------------------------------------------------
// Exports
// -------------------------------------------------------------------

module.exports = {
  resolveAndUpsert,
  resolveOnly,
  normalizePhone,
  normalizeEmail,
  jaroWinkler,
  areNicknameVariants,
  canonicalPersonSchema,
  getConnectionString,
  findByPhone,
  findByEmail,
  findByFuzzyName,
  mergeIntoExisting,
};
