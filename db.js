import { MongoClient } from 'mongodb'

let _client = null
let _db = null

export async function getDb() {
  if (_db) return _db
  const uri = process.env.MONGO_URI || 'mongodb://mongo:27017/pies?directConnection=true'
  _client = new MongoClient(uri)
  await _client.connect()
  _db = _client.db()
  return _db
}

export async function closeDb() {
  if (_client) {
    await _client.close()
    _client = null
    _db = null
  }
}
