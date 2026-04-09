#!/usr/bin/env bash
# Migrate PIES data from local MongoDB to the dedicated PIES node.
# Usage: ./migrate-from-local.sh <remote-mongo-uri>
#
# Exports people, interactions, and relationships collections from the
# local clawdbot database and imports them into the remote pies database.

set -euo pipefail

REMOTE_URI="${1:?Usage: ./migrate-from-local.sh <remote-mongo-uri>}"
LOCAL_URI="mongodb://localhost:27018/clawdbot?directConnection=true"
DUMP_DIR="/tmp/pies-migration-$(date +%s)"

echo "=== PIES Migration ==="
echo "Source: $LOCAL_URI"
echo "Target: $REMOTE_URI"
echo ""

# Export from local
echo "Exporting from local..."
mkdir -p "$DUMP_DIR"
for coll in people interactions relationships; do
  echo "  - $coll"
  mongoexport --uri="$LOCAL_URI" --collection="$coll" --out="$DUMP_DIR/$coll.json" --jsonArray 2>/dev/null
  count=$(python3 -c "import json; print(len(json.load(open('$DUMP_DIR/$coll.json'))))" 2>/dev/null || echo "?")
  echo "    exported $count documents"
done

# Import to remote
echo ""
echo "Importing to remote..."
for coll in people interactions relationships; do
  echo "  - $coll"
  mongoimport --uri="$REMOTE_URI" --collection="$coll" --file="$DUMP_DIR/$coll.json" --jsonArray --mode=upsert 2>/dev/null
done

# Cleanup
rm -rf "$DUMP_DIR"
echo ""
echo "Migration complete."
