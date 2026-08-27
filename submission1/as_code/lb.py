"""Shared Lakebase connection helper. Branch-aware."""
import json, subprocess, psycopg

P = "ironbark"
PROJ = "projects/ironbark-ops"

def _cli(*a):
    r = subprocess.run(["databricks", *a, "--profile", P, "-o", "json"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"CLI failed: {' '.join(a)}\n{r.stderr[:400]}")
    return json.loads(r.stdout)

def connect(branch="production"):
    ep = f"{PROJ}/branches/{branch}/endpoints/primary"
    host = _cli("postgres", "get-endpoint", ep)["status"]["hosts"]["host"]
    tok  = _cli("postgres", "generate-database-credential", ep)["token"]
    usr  = _cli("current-user", "me")["userName"]
    return psycopg.connect(host=host, user=usr, password=tok,
                           dbname="databricks_postgres", sslmode="require",
                           autocommit=True), host
