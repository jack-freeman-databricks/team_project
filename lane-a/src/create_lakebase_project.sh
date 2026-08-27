#!/usr/bin/env bash
# One-time prerequisite: create the Lakebase (Autoscaling Postgres) project for lane A.
# DABs have no resource type for a Lakebase project, so this can't live in the bundle.
# Auto-creates the `production` branch + `primary` read-write endpoint (scale-to-zero).
#
# Usage: ./create_lakebase_project.sh [profile]   (default profile: tech-summit)
set -euo pipefail

# Profile must target the workspace that owns ironbark-ops. On this machine that is
# `tech-summit` (fevm-serverless-stable); lane B refers to the same workspace as `ironbark`.
PROFILE="${1:-tech-summit}"
PROJECT="ironbark-ops"

# Idempotent: lane B already created ironbark-ops for the Build 1 submission, and
# create-project errors on an existing project. Skip the create if it already exists.
if databricks postgres list-projects --profile "$PROFILE" -o json 2>/dev/null | grep -q "\"$PROJECT\""; then
  echo "Project $PROJECT already exists — skipping create (owned by lane B)."
else
  databricks postgres create-project "$PROJECT" \
    --json '{"spec": {"display_name": "Ironbark Ops (lane A)"}}' \
    --profile "$PROFILE"
fi

echo "--- branches ---"
databricks postgres list-branches "projects/$PROJECT" --profile "$PROFILE"
echo "--- endpoints ---"
databricks postgres list-endpoints "projects/$PROJECT/branches/production" --profile "$PROFILE"
echo "--- databases ---"
databricks postgres list-databases "projects/$PROJECT/branches/production" --profile "$PROFILE"

echo "Done. Next: databricks bundle run a_lakebase_setup -t dev --profile $PROFILE"
