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
 *   GET  /person/:id/profile        — Get behavioral profile
 *   POST /person/:id/profile        — Generate/refresh behavioral profile (auth required)
 *   GET  /profiles                  — List all profiled people
 *   POST /profile/batch             — Batch profile top contacts (auth required)
 */

import { Router } from 'express'
import { ObjectId } from 'mongodb'
import { createRequire } from 'module'
import { getDb } from './db.js'

const require = createRequire(import.meta.url)
const { resolveAndUpsert, resolveOnly, normalizePhone, normalizeEmail, jaroWinkler } = require('./lib/pies-resolve.cjs')
const { addRelationship, getRelationships, getFamilyMembers, getPhoneTree, ensureIndexes } = require('./lib/pies-relationships.cjs')
const { autoTierAll } = require('./lib/pies-auto-tier.cjs')
const { runEnrichment } = require('./lib/pies-enrich.cjs')

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
// GET /people — Paginated people list with filters
// -------------------------------------------------------------------

router.get('/people', async (req, res) => {
  try {
    const db = await getDb()
    const people = db.collection('people')
    const page = Math.max(1, parseInt(req.query.page || '1'))
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit || '50')))
    const skip = (page - 1) * limit

    const filter = {}
    if (req.query.tier) filter.tier = req.query.tier
    if (req.query.entity_type) filter.entity_type = req.query.entity_type
    if (req.query.q) filter.name = { $regex: req.query.q, $options: 'i' }
    if (req.query.has_profile === 'true') filter.behavioral_profile = { $exists: true }
    if (req.query.has_profile === 'false') filter.behavioral_profile = { $exists: false }
    if (req.query.family_type) filter.family_type = req.query.family_type
    if (req.query.living_status) filter.living_status = req.query.living_status

    // Sort
    const sortField = req.query.sort || '-interaction_count'
    const sortDir = sortField.startsWith('-') ? -1 : 1
    const sortKey = sortField.replace(/^-/, '')
    const sort = { [sortKey]: sortDir }

    const [docs, total] = await Promise.all([
      people.find(filter).sort(sort).skip(skip).limit(limit).toArray(),
      people.countDocuments(filter),
    ])

    res.json({
      people: docs,
      total,
      page,
      pages: Math.ceil(total / limit),
      limit,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// PUT /person/:id — Update person fields
// -------------------------------------------------------------------

router.put('/person/:id', async (req, res) => {
  try {
    const db = await getDb()
    const id = new ObjectId(req.params.id)
    const updates = { ...req.body }
    delete updates._id // never overwrite _id
    updates.updated_at = new Date()

    const result = await db.collection('people').findOneAndUpdate(
      { _id: id },
      { $set: updates },
      { returnDocument: 'after' }
    )
    if (!result) return res.status(404).json({ error: 'Person not found' })
    res.json(result)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// PUT /relationship/:id — Update relationship fields
// -------------------------------------------------------------------

router.put('/relationship/:id', async (req, res) => {
  try {
    const db = await getDb()
    const id = new ObjectId(req.params.id)
    const updates = { ...req.body }
    delete updates._id
    updates.updated_at = new Date()

    const result = await db.collection('relationships').findOneAndUpdate(
      { _id: id },
      { $set: updates },
      { returnDocument: 'after' }
    )
    if (!result) return res.status(404).json({ error: 'Relationship not found' })
    res.json(result)
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

    // Enrich relationships with the other person's data
    const enrichedRels = await Promise.all(relationships.map(async r => {
      const otherId = r.to?.toString() === id.toString() ? r.from : r.to
      let other_person = null
      if (otherId) {
        try {
          other_person = await db.collection('people').findOne(
            { _id: new ObjectId(otherId) },
            { projection: { name: 1, tier: 1, family_type: 1, living_status: 1 } }
          )
        } catch {}
      }
      return { ...r, other_person }
    }))

    res.json({
      person,
      relationships: enrichedRels,
      interactions: recentInteractions,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// GET /churn-alerts — Top churn risk contacts
// -------------------------------------------------------------------

router.get('/churn-alerts', async (req, res) => {
  try {
    const db = await getDb()
    const limit = parseInt(req.query.limit || '20')

    const alerts = await db.collection('relationships').find({
      churn_risk: { $exists: true, $gt: 0.3 },
      relationship_type: { $ne: 'spam' },
    })
    .sort({ churn_risk: -1 })
    .limit(limit)
    .toArray()

    // Enrich with person names
    const enriched = await Promise.all(alerts.map(async a => {
      const person = await db.collection('people').findOne(
        { _id: a.to },
        { projection: { name: 1, tier: 1 } }
      )
      return {
        _id: a.to?.toString(),
        name: person?.name || 'Unknown',
        churn_risk: a.churn_risk,
        days_silent: a.days_silent || 0,
        reason: a.churn_reason || 'silent',
        tie_strength: a.tie_strength || 0,
        dunbar_layer: a.dunbar_layer || 'unknown',
      }
    }))

    res.json(enriched)
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

    // Return full person objects for the family view
    const personIds = family.map(f => f.to).filter(Boolean)
    const people = await db.collection('people').find({
      _id: { $in: personIds }
    }).toArray()

    res.json(people)
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
// GET /interactions — Bulk list interactions across all people
// -------------------------------------------------------------------

router.get('/interactions', async (req, res) => {
  try {
    const db = await getDb()
    const page = Math.max(1, parseInt(req.query.page) || 1)
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 50))
    const skip = (page - 1) * limit

    const filter = {}
    if (req.query.channel) filter.channel = req.query.channel
    if (req.query.direction) filter.direction = req.query.direction
    if (req.query.person_id) {
      filter.person_id = new ObjectId(req.query.person_id)
    }
    if (req.query.since) {
      filter.timestamp = { $gte: new Date(req.query.since) }
    }

    const [interactions, total] = await Promise.all([
      db.collection('interactions')
        .find(filter)
        .sort({ timestamp: -1 })
        .skip(skip)
        .limit(limit)
        .toArray(),
      db.collection('interactions').countDocuments(filter),
    ])

    // Gather channel counts for filter UI
    const channelAgg = await db.collection('interactions').aggregate([
      { $group: { _id: '$channel', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
    ]).toArray()
    const channels = channelAgg.map(c => ({ channel: c._id, count: c.count }))

    res.json({
      interactions,
      total,
      page,
      pages: Math.ceil(total / limit),
      channels,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// GET /graph — Relationship graph data for visualization
// -------------------------------------------------------------------

router.get('/graph', async (req, res) => {
  try {
    const db = await getDb()

    // Get all relationships with enriched person data
    const relationships = await db.collection('relationships').find({}).toArray()

    // Collect all person IDs referenced
    const personIds = new Set()
    for (const r of relationships) {
      if (r.from) personIds.add(r.from.toString())
      if (r.to) personIds.add(r.to.toString())
    }

    // Fetch all referenced people
    const people = await db.collection('people').find({
      _id: { $in: [...personIds].map(id => {
        try { return new ObjectId(id) } catch { return id }
      }) }
    }, {
      projection: { name: 1, tier: 1, entity_type: 1, interaction_count: 1, family_type: 1 }
    }).toArray()

    const peopleMap = {}
    for (const p of people) {
      peopleMap[p._id.toString()] = p
    }

    // Build nodes and edges for visualization
    const nodes = people.map(p => ({
      id: p._id.toString(),
      name: p.name,
      tier: p.tier,
      entity_type: p.entity_type,
      interaction_count: p.interaction_count || 0,
      family_type: p.family_type,
    }))

    const edges = relationships.map(r => ({
      id: r._id.toString(),
      source: r.from?.toString(),
      target: r.to?.toString(),
      type: r.type || r.relationship_type,
      tie_strength: r.tie_strength,
      dunbar_layer: r.dunbar_layer,
      momentum: r.momentum,
      community_id: r.community_id,
      churn_risk: r.churn_risk,
      source_name: peopleMap[r.from?.toString()]?.name,
      target_name: peopleMap[r.to?.toString()]?.name,
    }))

    // Community summary
    const communities = {}
    for (const r of relationships) {
      if (r.community_id != null) {
        if (!communities[r.community_id]) communities[r.community_id] = { id: r.community_id, members: new Set() }
        if (r.from) communities[r.community_id].members.add(r.from.toString())
        if (r.to) communities[r.community_id].members.add(r.to.toString())
      }
    }
    const communitySummary = Object.values(communities).map(c => ({
      id: c.id,
      size: c.members.size,
    }))

    res.json({ nodes, edges, communities: communitySummary })
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
// POST /import/bulk — Bulk import raw documents (for migration)
// Accepts { collection: "people"|"interactions"|"relationships", documents: [...] }
// -------------------------------------------------------------------

router.post('/import/bulk', async (req, res) => {
  try {
    const db = await getDb()
    const { collection, documents } = req.body
    if (!collection || !Array.isArray(documents) || documents.length === 0) {
      return res.status(400).json({ error: 'collection and documents[] required' })
    }
    if (!['people', 'interactions', 'relationships'].includes(collection)) {
      return res.status(400).json({ error: 'collection must be people, interactions, or relationships' })
    }

    // Convert _id strings back to ObjectId and other ObjectId fields
    const { ObjectId } = await import('mongodb')
    const docs = documents.map(doc => {
      const d = { ...doc }
      if (d._id?.$oid) d._id = new ObjectId(d._id.$oid)
      else if (typeof d._id === 'string' && d._id.length === 24) d._id = new ObjectId(d._id)
      // Convert person_id, from, to fields
      for (const field of ['person_id', 'from', 'to', 'company_id', 'reports_to']) {
        if (d[field]?.$oid) d[field] = new ObjectId(d[field].$oid)
        else if (typeof d[field] === 'string' && d[field].length === 24) d[field] = new ObjectId(d[field])
      }
      return d
    })

    // Use ordered:false for speed (continue on duplicate key errors)
    const result = await db.collection(collection).insertMany(docs, { ordered: false }).catch(err => {
      if (err.code === 11000) {
        return { insertedCount: err.result?.insertedCount || 0, duplicates: err.writeErrors?.length || 0 }
      }
      throw err
    })

    res.json({ ok: true, inserted: result.insertedCount, duplicates: result.duplicates || 0 })
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

// -------------------------------------------------------------------
// GET /person/:id/profile — Get behavioral profile
// -------------------------------------------------------------------

router.get('/person/:id/profile', async (req, res) => {
  try {
    const db = await getDb()
    const person = await db.collection('people').findOne({ _id: new ObjectId(req.params.id) })
    if (!person) return res.status(404).json({ error: 'Person not found' })
    if (!person.behavioral_profile) return res.status(404).json({ error: 'No profile generated yet', person_name: person.name })
    res.json({ person_name: person.name, profile: person.behavioral_profile })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// POST /person/:id/profile — Generate/refresh behavioral profile
// -------------------------------------------------------------------

router.post('/person/:id/profile', async (req, res) => {
  try {
    const db = await getDb()
    const { profilePerson } = require('./lib/pies-profiler.cjs')
    const result = await profilePerson(db, req.params.id)
    if (result.error) return res.status(400).json(result)
    res.json(result)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// GET /profiles — List all profiled people
// -------------------------------------------------------------------

router.get('/profiles', async (req, res) => {
  try {
    const db = await getDb()
    const limit = parseInt(req.query.limit || '50')
    const profiled = await db.collection('people').find(
      { behavioral_profile: { $exists: true } },
      { projection: { name: 1, tier: 1, interaction_count: 1, last_profiled: 1, 'behavioral_profile.behavioral.relationship_dynamic': 1, 'behavioral_profile.behavioral.communication_style': 1, 'behavioral_profile.quantitative.engagement_trend': 1 } }
    ).sort({ interaction_count: -1 }).limit(limit).toArray()

    res.json({ count: profiled.length, profiles: profiled })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// POST /profile/batch — Batch profile top contacts
// -------------------------------------------------------------------

router.post('/profile/batch', async (req, res) => {
  try {
    const db = await getDb()
    const { profileTopContacts } = require('./lib/pies-profiler.cjs')
    const options = {
      minInteractions: req.body.minInteractions || 20,
      limit: req.body.limit || 50,
      forceRefresh: req.body.forceRefresh || false,
    }
    const results = await profileTopContacts(db, options)
    res.json({ ok: true, ...results })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// -------------------------------------------------------------------
// POST /enrich — Run enrichment pipeline
// -------------------------------------------------------------------

router.post('/enrich', async (req, res) => {
  try {
    const db = await getDb()
    const justinId = req.body.justinId ? new ObjectId(req.body.justinId) : null
    const stats = await runEnrichment(db, { justinId })
    res.json({ ok: true, ...stats })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

export default router
