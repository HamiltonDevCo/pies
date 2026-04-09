#!/usr/bin/env node
/**
 * PIES Relationships Module
 *
 * Manages typed, directional relationship edges between people.
 * Stored in a dedicated `relationships` collection for fast graph queries.
 *
 * Relationship types: family, business, friend, acquaintance
 * Roles are directional: { from: Justin, to: Nicole, role: "spouse", reverse_role: "spouse" }
 *
 * Usage:
 *   const { addRelationship, getRelationships, getPhoneTree, getFamilyMembers } = require('./lib/pies-relationships');
 */

const { ObjectId } = require('mongodb');

// -------------------------------------------------------------------
// Schema
// -------------------------------------------------------------------

function relationshipSchema(data) {
  const now = new Date();
  return {
    from: data.from,                 // ObjectId of person A
    to: data.to,                     // ObjectId of person B
    type: data.type || 'unknown',    // family, business, friend, acquaintance
    role: data.role || null,         // from's role relative to to (e.g., "spouse", "father")
    reverse_role: data.reverse_role || null,  // to's role relative to from
    strength: data.strength ?? 0.5,  // 0-1 relationship strength
    since: data.since || null,       // when relationship started
    source: data.source || 'user_confirmed',  // how we know
    notes: data.notes || null,
    phone_tree_order: data.phone_tree_order ?? null,  // emergency phone tree position
    created_at: data.created_at || now,
    updated_at: now,
  };
}

// -------------------------------------------------------------------
// Reverse role mapping
// -------------------------------------------------------------------

const REVERSE_ROLES = {
  spouse: 'spouse',
  wife: 'husband',
  husband: 'wife',
  father: 'child',
  mother: 'child',
  son: 'parent',
  daughter: 'parent',
  child: 'parent',
  parent: 'child',
  brother: 'brother',
  sister: 'sibling',
  sibling: 'sibling',
  nephew: 'uncle',
  niece: 'uncle',
  uncle: 'nephew',
  aunt: 'niece',
  cousin: 'cousin',
  second_cousin: 'second_cousin',
  grandparent: 'grandchild',
  grandchild: 'grandparent',
  // Business
  manager: 'report',
  report: 'manager',
  client: 'vendor',
  vendor: 'client',
  colleague: 'colleague',
  // Social
  close_friend: 'close_friend',
  friend: 'friend',
  mentor: 'mentee',
  mentee: 'mentor',
};

function inferReverseRole(role) {
  if (!role) return null;
  return REVERSE_ROLES[role] || role;
}

// -------------------------------------------------------------------
// Core: Add / Update relationship
// -------------------------------------------------------------------

/**
 * Add or update a relationship edge between two people.
 * Automatically creates the reverse edge.
 *
 * @param {Db} db - MongoDB database
 * @param {Object} data - { from, to, type, role, reverse_role, strength, since, source, phone_tree_order }
 * @returns {Object} { forward, reverse } — the two edge documents
 */
async function addRelationship(db, data) {
  const rels = db.collection('relationships');

  const fromId = typeof data.from === 'string' ? new ObjectId(data.from) : data.from;
  const toId = typeof data.to === 'string' ? new ObjectId(data.to) : data.to;

  const reverseRole = data.reverse_role || inferReverseRole(data.role);

  // Upsert forward edge
  const forward = relationshipSchema({
    ...data,
    from: fromId,
    to: toId,
  });

  await rels.updateOne(
    { from: fromId, to: toId, type: data.type },
    { $set: forward },
    { upsert: true }
  );

  // Upsert reverse edge
  const reverse = relationshipSchema({
    from: toId,
    to: fromId,
    type: data.type,
    role: reverseRole,
    reverse_role: data.role,
    strength: data.strength,
    since: data.since,
    source: data.source,
    phone_tree_order: null, // phone tree is one-directional
  });

  await rels.updateOne(
    { from: toId, to: fromId, type: data.type },
    { $set: reverse },
    { upsert: true }
  );

  return { forward, reverse };
}

// -------------------------------------------------------------------
// Query: Get all relationships for a person
// -------------------------------------------------------------------

async function getRelationships(db, personId, opts = {}) {
  const rels = db.collection('relationships');
  const people = db.collection('people');
  const id = typeof personId === 'string' ? new ObjectId(personId) : personId;

  const filter = { from: id };
  if (opts.type) filter.type = opts.type;

  const edges = await rels.find(filter).sort({ strength: -1 }).toArray();

  // Hydrate with person data
  const toIds = edges.map(e => e.to);
  const persons = await people.find({ _id: { $in: toIds } }).toArray();
  const personMap = new Map(persons.map(p => [p._id.toString(), p]));

  return edges.map(e => ({
    ...e,
    person: personMap.get(e.to.toString()) || null,
  }));
}

// -------------------------------------------------------------------
// Query: Get family members
// -------------------------------------------------------------------

async function getFamilyMembers(db, personId) {
  return getRelationships(db, personId, { type: 'family' });
}

// -------------------------------------------------------------------
// Query: Emergency phone tree
// -------------------------------------------------------------------

/**
 * Get the emergency phone tree — family + close friends with phones,
 * ordered by phone_tree_order (then strength).
 */
async function getPhoneTree(db, personId) {
  const rels = db.collection('relationships');
  const people = db.collection('people');
  const id = typeof personId === 'string' ? new ObjectId(personId) : personId;

  const edges = await rels.find({
    from: id,
    phone_tree_order: { $ne: null },
  }).sort({ phone_tree_order: 1 }).toArray();

  const toIds = edges.map(e => e.to);
  const persons = await people.find({ _id: { $in: toIds } }).toArray();
  const personMap = new Map(persons.map(p => [p._id.toString(), p]));

  return edges.map(e => {
    const person = personMap.get(e.to.toString());
    const phone = person?.phones?.[0]?.number ||
                  person?.identities?.phones?.[0] ||
                  person?.phone || null;
    return {
      order: e.phone_tree_order,
      name: person?.name || 'Unknown',
      role: e.role,
      phone,
      person_id: e.to,
      relationship: e,
    };
  }).filter(entry => entry.phone); // Only include entries with phones
}

// -------------------------------------------------------------------
// Query: Find relationship between two specific people
// -------------------------------------------------------------------

async function getRelationshipBetween(db, personA, personB) {
  const rels = db.collection('relationships');
  const aId = typeof personA === 'string' ? new ObjectId(personA) : personA;
  const bId = typeof personB === 'string' ? new ObjectId(personB) : personB;

  return rels.findOne({ from: aId, to: bId });
}

// -------------------------------------------------------------------
// Ensure indexes
// -------------------------------------------------------------------

async function ensureIndexes(db) {
  const rels = db.collection('relationships');
  await rels.createIndex({ from: 1, type: 1 });
  await rels.createIndex({ to: 1, type: 1 });
  await rels.createIndex({ from: 1, to: 1, type: 1 }, { unique: true });
  await rels.createIndex({ from: 1, phone_tree_order: 1 });
}

// -------------------------------------------------------------------
// Exports
// -------------------------------------------------------------------

module.exports = {
  addRelationship,
  getRelationships,
  getFamilyMembers,
  getPhoneTree,
  getRelationshipBetween,
  ensureIndexes,
  inferReverseRole,
  REVERSE_ROLES,
};
