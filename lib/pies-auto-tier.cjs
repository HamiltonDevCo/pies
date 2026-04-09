#!/usr/bin/env node
/**
 * PIES Auto-Tiering Module
 *
 * Calculates and assigns tiers to people based on:
 *   VIP-1: Immediate family (spouse, children, parents, siblings) OR $5k+ business clients
 *   VIP-2: Extended family, close friends, active business contacts (closeness >= 60)
 *   Standard: Regular contacts with some interaction history
 *   Low: Imported contacts with no/minimal interaction
 *
 * Also recalculates relationship_strength and engagement_score.
 *
 * Usage:
 *   const { autoTierAll, calculateTier, recalcScores } = require('./lib/pies-auto-tier');
 *   await autoTierAll(db);
 */

const VIP1_ROLES = new Set(['spouse', 'wife', 'husband', 'son', 'daughter', 'child', 'father', 'mother', 'parent', 'brother', 'sister']);
const VIP2_ROLES = new Set(['nephew', 'niece', 'uncle', 'aunt', 'cousin', 'second_cousin', 'close_friend', 'grandparent', 'grandchild']);

/**
 * Calculate tier for a single person based on their data and relationships.
 */
function calculateTier(person, relationships = []) {
  // Skip if tier is locked by user
  if (person.tier_locked) return person.tier;

  // VIP-1: Immediate family
  const hasVip1Role = VIP1_ROLES.has(person.role);
  const hasVip1Relationship = relationships.some(r => VIP1_ROLES.has(r.role));
  if (hasVip1Role || hasVip1Relationship) return 'vip-1';

  // VIP-1: High-value business (placeholder — needs Stripe/invoice data)
  // TODO: Check for $5k+ revenue once company/invoice data is integrated

  // VIP-2: Extended family, close friends
  const hasVip2Role = VIP2_ROLES.has(person.role);
  const hasVip2Relationship = relationships.some(r => VIP2_ROLES.has(r.role));
  const highCloseness = (person.metadata?.relationship?.closeness_score || 0) >= 60;
  if (hasVip2Role || hasVip2Relationship || highCloseness) return 'vip-2';

  // Standard: Has meaningful interaction history
  const interactionCount = person.interaction_count ||
    person.metadata?.relationship?.metrics?.message_count || 0;
  if (interactionCount >= 5) return 'standard';

  // Low: Everything else
  return 'low';
}

/**
 * Recalculate engagement and relationship scores for a person.
 * Formula: 0.3×recency + 0.3×frequency + 0.2×reciprocity + 0.2×depth
 */
function recalcScores(person) {
  const metrics = person.metadata?.relationship?.metrics || {};
  const now = Date.now();

  // Recency: days since last contact (decays over 90 days)
  const lastContact = metrics.last_contact ? new Date(metrics.last_contact).getTime() : 0;
  const daysSince = lastContact ? (now - lastContact) / (1000 * 60 * 60 * 24) : 999;
  const recencyScore = Math.max(0, 1 - (daysSince / 90));

  // Frequency: message count, logarithmic scale
  const msgCount = metrics.message_count || person.interaction_count || 0;
  const frequencyScore = msgCount > 0 ? Math.min(1, Math.log10(msgCount) / 3) : 0; // 1000 msgs = 1.0

  // Reciprocity: ratio of inbound to outbound
  const inbound = metrics.inbound_count || 0;
  const outbound = metrics.outbound_count || 0;
  const total = inbound + outbound;
  const reciprocityScore = total > 0 ? 1 - Math.abs(inbound - outbound) / total : 0;

  // Depth: number of communication channels (sms, email, slack, etc.)
  const sources = person.sources || [];
  const depthScore = Math.min(1, sources.length / 4); // 4+ channels = max

  // Weighted average
  const engagement = (0.3 * recencyScore + 0.3 * frequencyScore + 0.2 * reciprocityScore + 0.2 * depthScore);
  const strength = person.metadata?.relationship?.closeness_score
    ? person.metadata.relationship.closeness_score / 100
    : engagement;

  return {
    recency_score: Math.round(recencyScore * 100) / 100,
    engagement_score: Math.round(engagement * 100) / 100,
    relationship_strength: Math.round(strength * 100) / 100,
  };
}

/**
 * Auto-tier all people in the database.
 * Returns stats on what changed.
 */
async function autoTierAll(db) {
  const people = db.collection('people');
  const rels = db.collection('relationships');

  const allPeople = await people.find({}).toArray();
  const stats = { total: allPeople.length, changed: 0, byTier: {} };

  for (const person of allPeople) {
    // Get this person's relationships (inbound edges = someone else → this person)
    const personRels = await rels.find({ to: person._id }).toArray();

    const newTier = calculateTier(person, personRels);
    const scores = recalcScores(person);

    const updates = {
      auto_tier: newTier,
      ...scores,
      updated_at: new Date(),
    };

    // Only overwrite tier if not locked
    if (!person.tier_locked) {
      updates.tier = newTier;
    }

    const oldTier = person.tier || 'unknown';
    if (oldTier !== newTier || person.engagement_score !== scores.engagement_score) {
      await people.updateOne({ _id: person._id }, { $set: updates });
      if (oldTier !== newTier) stats.changed++;
    }

    stats.byTier[newTier] = (stats.byTier[newTier] || 0) + 1;
  }

  return stats;
}

module.exports = { autoTierAll, calculateTier, recalcScores };
