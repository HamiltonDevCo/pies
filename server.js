import express from 'express'
import cors from 'cors'
import piesRouter from './pies-api.js'

const app = express()
const PORT = parseInt(process.env.PORT || '3200')

app.use(cors())
app.use(express.json({ limit: '50mb' }))

// Health check (lightweight — doesn't require MongoDB to be up)
app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime() })
})

// Deep health check (includes MongoDB connectivity)
app.get('/health/deep', async (req, res) => {
  try {
    const { getDb } = await import('./db.js')
    const db = await getDb()
    const stats = await db.command({ ping: 1 })
    res.json({ status: 'ok', mongo: stats.ok === 1 ? 'connected' : 'error', uptime: process.uptime() })
  } catch (err) {
    res.status(503).json({ status: 'error', error: err.message })
  }
})

// Mount PIES API
app.use('/api/pies', piesRouter)

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[PIES] Standalone service listening on :${PORT}`)
})
