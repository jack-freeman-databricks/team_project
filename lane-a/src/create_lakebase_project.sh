#!/usr/bin/env bash
# One-time prerequisite: create the Lakebase (Autoscaling Postgres) project for lane A.
# DABs have no resource type for a Lakebase project, so this can't live in the bundle.
# Auto-creates the `production` branch + `primary` read-write endpoint (scale-to-zero).
#
# Usage: ./create_lakebase_project.sh [profile]   (default profile: tech-summit)
set -euo pipefail

PROFILE="${1:-tech-summit}"
PROJECT="ironbark-ops"

databricks postgres create-project "$PROJECT" \
  --json '{"spec": {"display_name": "Ironbark Ops (lane A)"}}' \
  --profile "$PROFILE"

echo "--- branches ---"
databricks postgres list-branches "projects/$PROJECT" --profile "$PROFILE"
echo "--- endpoints ---"
databricks postgres list-endpoints "projects/$PROJECT/branches/production" --profile "$PROFILE"
echo "--- databases ---"
databricks postgres list-databases "projects/$PROJECT/branches/production" --profile "$PROFILE"

echo "Done. Next: databricks bundle run a_lakebase_setup -t dev --profile $PROFILE"
