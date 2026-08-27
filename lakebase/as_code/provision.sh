#!/usr/bin/env bash
# Ironbark Lakebase provisioning, as code.
#
# NOTE ON DATABRICKS ASSET BUNDLES: DABs has no native resource for either a
# Lakebase synced table on Autoscaling (the legacy `synced_database_tables`
# resource maps to a retired API and fails; `postgres_synced_tables` does not
# exist yet) or for a Lakebase CDF configuration. The supported programmatic
# path today is the `databricks postgres` CLI / REST API / SDK, which is what
# this script uses. It is committed, reviewable, idempotent and re-runnable, so
# the sync is defined as code rather than clicked through the UI. Run it from a
# bundle job (see bundle.yml alongside) if you want it inside a DAB lifecycle.
set -euo pipefail

PROFILE="${PROFILE:-ironbark}"
PROJECT="ironbark-ops"
BRANCH="production"
DB_ID="databricks-postgres"
UC_CATALOG="jack_freeman_catalog"
UC_SCHEMA="tech_summit_scada_build"
PG_SCHEMA="plant"
DB_PATH="projects/${PROJECT}/branches/${BRANCH}/databases/${DB_ID}"

echo "==> 1. Lakebase project (Postgres 17, native login on for executor-side sinks)"
databricks postgres get-project "projects/${PROJECT}" --profile "$PROFILE" >/dev/null 2>&1 || \
databricks postgres create-project "$PROJECT" --profile "$PROFILE" --json '{
  "spec": {"display_name":"ironbark-ops","pg_version":17,"enable_pg_native_login":true,
           "default_endpoint_settings":{"autoscaling_limit_min_cu":1,"autoscaling_limit_max_cu":8}}}'

echo "==> 2. Operational schema and migrations"
python3 "$(dirname "$0")/../apply_migrations.py"

echo "==> 3. Forward sync: governed UC table -> Lakebase (read-only in Postgres)"
# TRIGGERED requires Delta CDF on the source table.
databricks experimental aitools tools query \
  "ALTER TABLE ${UC_CATALOG}.${UC_SCHEMA}.dim_tag SET TBLPROPERTIES (delta.enableChangeDataFeed = true)" \
  --profile "$PROFILE" >/dev/null
databricks postgres get-synced-table \
  "synced_tables/${UC_CATALOG}.${UC_SCHEMA}.dim_tag_online" --profile "$PROFILE" >/dev/null 2>&1 || \
databricks postgres create-synced-table "${UC_CATALOG}.${UC_SCHEMA}.dim_tag_online" \
  --profile "$PROFILE" --timeout 15m --json "{
    \"spec\": {
      \"source_table_full_name\": \"${UC_CATALOG}.${UC_SCHEMA}.dim_tag\",
      \"primary_key_columns\": [\"tag_id\"],
      \"scheduling_policy\": \"TRIGGERED\",
      \"branch\": \"projects/${PROJECT}/branches/${BRANCH}\",
      \"postgres_database\": \"databricks_postgres\",
      \"create_database_objects_if_missing\": true,
      \"new_pipeline_spec\": {\"storage_catalog\": \"${UC_CATALOG}\",
                              \"storage_schema\": \"${UC_SCHEMA}\"}
    }}"

echo "==> 4. Reverse sync: writable Postgres schema -> UC Delta, SCD Type 2"
# Configured at schema level: every current and future table in plant joins the
# feed. Each source table needs REPLICA IDENTITY FULL, which the migrations set.
databricks postgres get-cdf-config "${DB_PATH}/cdf-configs/ironbark_plant_cdf" \
  --profile "$PROFILE" >/dev/null 2>&1 || \
databricks postgres create-cdf-config "$DB_PATH" "$UC_CATALOG" "$UC_SCHEMA" "$PG_SCHEMA" \
  --cdf-config-id ironbark_plant_cdf --profile "$PROFILE" --timeout 15m

echo "==> 5. Lakebase Search (hybrid vector + full text)"
# pgvector MUST exist before lakebase_vector: it is not created automatically.
# Both lakebase_* extensions additionally require shared_preload_libraries,
# which is enabled per workspace on the Previews page (admin only, no API).
python3 "$(dirname "$0")/../enable_search.py"

echo "==> done"
