#!/bin/bash
# run_migration.sh
# ============================================================================
# Orchestrates the full one-off migration inside Railway:
#   1. Download SQLite files from Dropbox
#   2. Validate each downloaded file is actually a SQLite database
#   3. Apply the Postgres schema (001_initial.sql)
#   4. Run the migration script (all tables)
#   5. Verify row counts
# ============================================================================

set -euo pipefail

echo "=========================================="
echo "FII Guia: SQLite -> Postgres migration"
echo "Started: $(date)"
echo "=========================================="

# -----------------------------------------------------------------------------
# Sanity check
# -----------------------------------------------------------------------------
if [ -z "${DATABASE_URL:-}" ]; then
    echo "ERROR: DATABASE_URL not set. In Railway, reference the Postgres"
    echo "       service variable: \${{Postgres.DATABASE_URL}}"
    exit 1
fi

mkdir -p backend/data

# -----------------------------------------------------------------------------
# 1. Download SQLite files from Dropbox
# -----------------------------------------------------------------------------
echo ""
echo "[1/4] Downloading SQLite files from Dropbox..."

wget --quiet --show-progress -O backend/data/b3.db \
    "https://www.dropbox.com/scl/fi/h9p6dkp2wy91bmpa91d8u/b3.db?rlkey=ec23sb9j2mkmyqnwez1me5p48&st=wuqbnp72&dl=1"

wget --quiet --show-progress -O backend/data/dividendos.db \
    "https://www.dropbox.com/scl/fi/1je6v5ewqk5fjubr3m0vy/dividendos.db?rlkey=pfbhmudlo4vy8kaasl73byxah&dl=1"

wget --quiet --show-progress -O backend/data/informe_mensal.db \
    "https://www.dropbox.com/scl/fi/6sxikc6sb9zdrjwqzpkbp/informe_mensal.db?rlkey=on9yrbva9oy3xe7v0rfzkvsqd&st=z1mbats9&dl=1"

wget --quiet --show-progress -O backend/data/gestores.db \
    "https://www.dropbox.com/scl/fi/oo31qrrlvypnhpz8qyqwa/gestores.db?rlkey=tf75spkbvyyzsbh1q2h0u1hln&st=zq1gk8ax&dl=1"

echo "Downloaded:"
ls -lh backend/data/

# -----------------------------------------------------------------------------
# Validate each file is a real SQLite database
# ----------------------------------------------------------------------------
# Dropbox share links that are stale or missing session tokens sometimes serve
# an HTML error page instead of the file. SQLite opens the HTML "successfully"
# but then fails with "file is not a database" during queries. We catch that
# here, up front, with a clear error message instead of a cryptic stack trace.
# Every valid SQLite file starts with: "SQLite format 3" (15 bytes + NUL).
# -----------------------------------------------------------------------------
echo ""
echo "Verifying SQLite file headers..."
for db in backend/data/*.db; do
    header=$(head -c 15 "$db")
    if [ "$header" = "SQLite format 3" ]; then
        size=$(du -h "$db" | cut -f1)
        echo "  OK   $db ($size)"
    else
        echo ""
        echo "  FAIL $db -- not a valid SQLite file"
        echo "       First 300 chars of downloaded content:"
        head -c 300 "$db" | sed 's/^/         /'
        echo ""
        echo "  Most likely cause: the Dropbox share link is stale or missing"
        echo "  a session token. Update the URL in run_migration.sh."
        exit 1
    fi
done

# -----------------------------------------------------------------------------
# 2. Apply schema
# -----------------------------------------------------------------------------
echo ""
echo "[2/4] Applying schema (migrations/001_initial.sql)..."

python -c "
import os, psycopg2
with open('migrations/001_initial.sql') as f:
    sql = f.read()
conn = psycopg2.connect(os.environ['DATABASE_URL'])
with conn.cursor() as cur:
    cur.execute(sql)
conn.commit()
conn.close()
print('Schema applied.')
"

# -----------------------------------------------------------------------------
# 3. Run the migration
# -----------------------------------------------------------------------------
echo ""
echo "[3/4] Running migration (all tables)..."
python migrate_sqlite_to_pg.py --all

# -----------------------------------------------------------------------------
# 4. Verify
# -----------------------------------------------------------------------------
echo ""
echo "[4/4] Verifying row counts..."
python migrate_sqlite_to_pg.py --verify

echo ""
echo "=========================================="
echo "Migration complete: $(date)"
echo "Delete this Railway service when ready."
echo "=========================================="
