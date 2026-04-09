/**
 * PIES Knowledge Graph API — Standalone
 *
 * Endpoints:
 *   GET  /person?q=<search>         — Fuzzy person search
 *   GET  /person/:id                — Get person by ID
 *   GET  /person/:id/relationships  — All relationships for a person
 *   GET  /person/:id/context        — Full context (person + relationships + interactions)
 *   GET  /family                    — All family members
 *   GET  /phone-tree                — Emergency phone tree
 *   POST /relationship              — Create/update relationship (auth required)
 *   POST /resolve                   — Resolve person (read-only, auth required)
 *   POST /upsert                    — Resolve + create/merge (auth required)
 *   POST /interaction               — Log interaction (auth required)
 *   POST /interaction/batch         — Batch log interactions (auth required)
 *   GET  /stats                     — Database stats
 *   POST /auto-tier                 — Run auto-tiering (auth required)
 */

import { Router } from 'express'
import { ObjectId } from 'mongodb'
import { createRequire } from 'module'
import { getDb } from './db.js'

const require = createRequire(import.meta.url)
const { resolveAndUpsert, resolveOnly, normalizePhone, normalizeEmail, jaroWinkler } = require('./lib/pies-resolve.cjs')
const { addRelationship, getRelationships, getFamilyMembers, getPhoneTree, ensureIndexes } = require('./lib/pies-relationships.cjs')
const { autoTierAll } = require('./lib/pies-auto-tier.cjs')

const router = Router()

// -------------------------------------------------------------------
// Auth middleware for write operations
// -------------------------------------------------------------------

const SYNC_TOKEN = process.env.PIES_SYNC_TOKEN || ''

function requireAuth(req, res, next) {
  if (!SYNC_TOKEN) return next()
  const authHeader = req.headers.authorization || ''
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : ''
  if (token !== SYNC_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' })
  }
  next()
}

router.use((req, res, next) => {
  if (['POST', 'PUT', 'DELETE'].includes(req.method)) {
    return requireAuth(req, res, next)
  }
  next()
})

// -------------------------------------------------------------------
// Ensure indexes on first request
// -------------------------------------------------------------------

let _indexesEnsured = false
router.use(async (req, res, next) => {
  if (!_indexesEnsured) {
    try {
      const db = await getDb()
      await ensureIndexes(db)
      // PIES-specific indexes
      const people = db.collection('people')
      await people.createIndex({ 'phones.number': 1 })
      await people.createIndex({ 'identities.phones': 1 })
      await people.createIndex({ 'emails.address': 1 })
      await people.createIndex({ 'identities.emails': 1 })
      await people.createIndex({ name: 'text' })
      await people.createIndex({ tier: 1 })
      _indexesEnsured = true
    } catch (err) {
      console.error('[PIES] Index creation error:', err.message)
    }
  }
  next()
})

// Helper: find Justin (Self) record
let _justinId = null
async function getJustinId(db) {
  if (_justinId) return _justinId
  const justin = await db.collection('people').findOne({ name: /justin hamilton.*self/i })
  if (justin) _justinId = justin._id
  return _justinId
}

// -------------------------------------------------------------------
// GET /person?q=<search>&phone=<phone>&email=<email>
// -------------------------------------------------------------------

router.get('/person', async (req, res) => {
  try {
    const db = await getDb()
    const people = db.collection('people')
    const { q, phone, email, limit = 10 } = req.query

    if (phone) {
      const norm = normalizePhone(phone)
      if (!norm) return res.json({ results: [], query: { phone } })
      const person = await people.findOne({
        $or: [
          { 'phones.number': norm },
          { 'identities.phones': norm },
          { phone: norm },
        ]
      })
      return res.json({ results: person ? [person] : [], query: { phone: norm } })
    }

    if (email) {
      const norm = normalizeEmail(email)
      if (!norm) return res.json({ results: [], query: { email } })
      const person = await people.findOne({
        $or: [
          { 'emails.address': norm },
          { 'identities.emails': norm },
        ]
      })
      return res.json({ results: person ? [person] : [], query: { email: norm } })
    }

    if (q) {
      const exactResults = await people.find({
        name: { $regex: q, $options: 'i' }
      }).limit(parseInt(limit)).toArray()

      if (exactResults.length > 0) {
        return res.json({ results: exactResults, query: { q }, matchType: 'regex' })
      }

      const allCandidates = await people.find({}).project({
        name: 1, phones: 1, emails: 1, identities: 1, tier: 1, relationship: 1, role: 1,
        city: 1, state: 1, tags: 1, sources: 1
      }).toArray()

      const scored = allCandidates
        .map(p => ({ ...p, score: jaroWinkler(q, p.name || '') }))
        .filter(p => p.score >= 0.75)
        .sort((a, b) => b.score - a.score)
        .slice(0, parseInt(limit))

      return res.json({ results: scored, query: { q }, matchType: 'fuzzy' })
    }

    res.status(400).json({ error: 'Provide q, phone, or email parameter' })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// GET /person/:id
// -------------------------------------------------------------------

router.get('/person/:id', async (req, res) => {
  try {
    const db = await getDb()
    const person = await db.collection('people').findOne({ _id: new ObjectId(req.params.id) })
    if (!person) return res.status(404).json({ error: 'Person not found' })
    res.json(person)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// GET /person/:id/relationships
// -------------------------------------------------------------------

router.get('/person/:id/relationships', async (req, res) => {
  try {
    const db = await getDb()
    const rels = await getRelationships(db, req.params.id, { type: req.query.type || undefined })
    res.json({ relationships: rels, count: rels.length })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// GET /person/:id/context
// -------------------------------------------------------------------

router.get('/person/:id/context', async (req, res) => {
  try {
    const db = await getDb()
    const id = new ObjectId(req.params.id)
    const limit = parseInt(req.query.interaction_limit || '20')

    const [person, relationships, recentInteractions] = await Promise.all([
      db.collection('people').findOne({ _id: id }),
      getRelationships(db, id),
      db.collection('interactions').find({ person_id: id })
        .sort({ timestamp: -1 }).limit(limit).toArray(),
    ])

    if (!person) return res.status(404).json({ error: 'Person not found' })

    res.json({
      person,
      relationships: relationships.map(r => ({
        type: r.type, role: r.role, strength: r.strength,
        name: r.person?.name,
        phone: r.person?.phones?.[0]?.number || r.person?.identities?.phones?.[0],
      })),
      recent_interactions: recentInteractions,
      summary: {
        relationship_count: relationships.length,
        interaction_count: person.interaction_count || recentInteractions.length,
        tier: person.tier,
        last_interaction: person.last_interaction,
      },
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// GET /family
// -------------------------------------------------------------------

router.get('/family', async (req, res) => {
  try {
    const db = await getDb()
    const justinId = await getJustinId(db)
    if (!justinId) return res.status(404).json({ error: 'Justin (Self) record not found' })

    const family = await getFamilyMembers(db, justinId)
    res.json({
      family: family.map(f => ({
        name: f.person?.name, role: f.role,
        phone: f.person?.phones?.[0]?.number || f.person?.identities?.phones?.[0],
        strength: f.strength, person_id: f.to?.toString(),
        phone_tree_order: f.phone_tree_order,
      })),
      count: family.length,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// GET /phone-tree
// -------------------------------------------------------------------

router.get('/phone-tree', async (req, res) => {
  try {
    const db = await getDb()
    const justinId = await getJustinId(db)
    if (!justinId) return res.status(404).json({ error: 'Justin (Self) record not found' })

    const tree = await getPhoneTree(db, justinId)
    res.json({
      phone_tree: tree.map(t => ({
        order: t.order, name: t.name, role: t.role, phone: t.phone,
      })),
      count: tree.length,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// POST /relationship
// -------------------------------------------------------------------

router.post('/relationship', async (req, res) => {
  try {
    const db = await getDb()
    const { from, to, type, role, reverse_role, strength, since, phone_tree_order } = req.body
    if (!from || !to || !type) {
      return res.status(400).json({ error: 'from, to, and type are required' })
    }
    const result = await addRelationship(db, {
      from, to, type, role, reverse_role, strength, since, phone_tree_order, source: 'api',
    })
    res.json({ status: 'ok', forward: result.forward, reverse: result.reverse })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// POST /resolve
// -------------------------------------------------------------------

router.post('/resolve', async (req, res) => {
  try {
    const db = await getDb()
    const result = await resolveOnly(db, req.body)
    if (!result) return res.json({ found: false })
    res.json({ found: true, person: result.person, matchMethod: result.matchMethod })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// POST /upsert
// -------------------------------------------------------------------

router.post('/upsert', async (req, res) => {
  try {
    const db = await getDb()
    const result = await resolveAndUpsert(db, req.body)
    res.json({ person: result.person, action: result.action, matchMethod: result.matchMethod })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// POST /interaction
// -------------------------------------------------------------------

router.post('/interaction', async (req, res) => {
  try {
    const db = await getDb()
    const { phone, email, name, direction = 'inbound', channel = 'sms',
      message, duration, sentiment, timestamp } = req.body

    if (!phone && !email) {
      return res.status(400).json({ error: 'phone or email required' })
    }

    const personData = {}
    if (phone) personData.phone = phone
    if (email) personData.email = email
    if (name) personData.name = name
    personData.source = `phone-sync:${channel}`

    const result = await resolveAndUpsert(db, personData)
    const person = result?.person
    if (!person) return res.status(500).json({ error: 'Failed to resolve person' })

    const interaction = {
      person_id: person._id,
      person_name: person.name || name || phone || email,
      direction, channel,
      timestamp: timestamp ? new Date(timestamp) : new Date(),
      logged_at: new Date(),
    }
    if (message) interaction.message = message
    if (duration !== undefined) interaction.duration = parseInt(duration)
    if (sentiment) interaction.sentiment = sentiment

    await db.collection('interactions').insertOne(interaction)
    await db.collection('people').updateOne(
      { _id: person._id },
      {
        $set: { last_interaction: interaction.timestamp, recency_score: 1.0, updated_at: new Date() },
        $inc: { interaction_count: 1 },
      }
    )

    res.json({ ok: true, person_id: person._id.toString(), person_name: person.name })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// POST /interaction/batch
// -------------------------------------------------------------------

router.post('/interaction/batch', async (req, res) => {
  try {
    const db = await getDb()
    const { interactions } = req.body
    if (!Array.isArray(interactions) || interactions.length === 0) {
      return res.status(400).json({ error: 'interactions array required' })
    }

    const results = { logged: 0, errors: 0, people_resolved: new Set() }

    for (const item of interactions) {
      try {
        const { phone, email, name, direction, channel, message, duration, sentiment, timestamp } = item
        if (!phone && !email) { results.errors++; continue }

        const personData = {}
        if (phone) personData.phone = phone
        if (email) personData.email = email
        if (name) personData.name = name
        personData.source = `phone-sync:${channel || 'unknown'}`

        const resolved = await resolveAndUpsert(db, personData)
        const person = resolved?.person
        if (!person) { results.errors++; continue }

        const interaction = {
          person_id: person._id,
          person_name: person.name || name || phone || email,
          direction: direction || 'inbound',
          channel: channel || 'sms',
          timestamp: timestamp ? new Date(timestamp) : new Date(),
          logged_at: new Date(),
        }
        if (message) interaction.message = message
        if (duration !== undefined) interaction.duration = parseInt(duration)
        if (sentiment) interaction.sentiment = sentiment

        await db.collection('interactions').insertOne(interaction)
        results.people_resolved.add(person._id.toString())

        await db.collection('people').updateOne(
          { _id: person._id },
          {
            $set: { last_interaction: interaction.timestamp, recency_score: 1.0, updated_at: new Date() },
            $inc: { interaction_count: 1 },
          }
        )
        results.logged++
      } catch { results.errors++ }
    }

    res.json({ ok: true, logged: results.logged, errors: results.errors, people_resolved: results.people_resolved.size })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// GET /stats
// -------------------------------------------------------------------

router.get('/stats', async (req, res) => {
  try {
    const db = await getDb()
    const [totalPeople, withPhones, withEmails, totalInteractions, totalRelationships, tierCounts] = await Promise.all([
      db.collection('people').countDocuments(),
      db.collection('people').countDocuments({ 'phones.0': { $exists: true } }),
      db.collection('people').countDocuments({ 'emails.0': { $exists: true } }),
      db.collection('interactions').countDocuments(),
      db.collection('relationships').countDocuments(),
      db.collection('people').aggregate([
        { $group: { _id: '$tier', count: { $sum: 1 } } }
      ]).toArray(),
    ])

    res.json({
      people: { total: totalPeople, with_phones: withPhones, with_emails: withEmails },
      interactions: totalInteractions,
      relationships: totalRelationships,
      tiers: Object.fromEntries(tierCounts.map(t => [t._id || 'null', t.count])),
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// POST /auto-tier — Run auto-tiering on all people
// -------------------------------------------------------------------

router.post('/auto-tier', async (req, res) => {
  try {
    const db = await getDb()
    const stats = await autoTierAll(db)
    res.json({ ok: true, ...stats })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

export default router
