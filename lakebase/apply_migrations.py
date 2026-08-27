#!/usr/bin/env python3
"""Apply the Ironbark Lakebase schema and migrations, idempotently.

Every statement is IF NOT EXISTS or otherwise safe to re-run, which is what
makes "promote to main by re-applying the validated migration" safe. Lakebase
branches are copy-on-write and have no merge operation, so re-applying the same
migration file to the root branch IS the promotion step.

Usage:
  python3 lakebase/apply_migrations.py [branch]     # default: production
"""
import glob, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lb import connect

BRANCH = sys.argv[1] if len(sys.argv) > 1 else "production"
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

files = [os.path.join(HERE, "01_operational_schema.sql")]
files += sorted(glob.glob(os.path.join(ROOT, "submission1", "migrations", "*.sql")))

conn, host = connect(BRANCH)
print(f"branch {BRANCH} @ {host}")
with conn:
    for f in files:
        conn.execute(open(f).read())
        print(f"  applied {os.path.relpath(f, ROOT)}")
    tables = conn.execute("""SELECT tablename FROM pg_tables
                            WHERE schemaname='plant' ORDER BY 1""").fetchall()
    fks = conn.execute("""SELECT count(*) FROM pg_constraint
                          WHERE contype='f' AND connamespace='plant'::regnamespace""").fetchone()[0]
    print(f"  plant now has {len(tables)} tables, {fks} foreign keys: "
          f"{', '.join(t[0] for t in tables)}")
