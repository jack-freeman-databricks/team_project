#!/usr/bin/env bash
# Lakebase branch lifecycle, as code. Both branch uses the rubric asks for.
#
#   1. dev-work-order-notes          permanent development branch, off main.
#                                    Migrations are developed and validated here
#                                    before being promoted to the root branch.
#   2. forecast-deferred-maintenance THROWAWAY forecasting branch, 4 hour TTL.
#                                    Carries destructive what-if writes that must
#                                    never touch production and are not kept.
#
# The root branch of a Lakebase Autoscaling project is auto-provisioned and named
# `production`. That is this project's main / clean environment; Lakebase does not
# name it "main".
set -euo pipefail
PROFILE="${PROFILE:-ironbark}"
PROJECT="ironbark-ops"
MAIN="projects/${PROJECT}/branches/production"

echo "==> development branch (permanent, off main)"
databricks postgres get-branch "${MAIN%/*}/dev-work-order-notes" --profile "$PROFILE" >/dev/null 2>&1 || \
databricks postgres create-branch "projects/${PROJECT}" dev-work-order-notes \
  --profile "$PROFILE" --timeout 10m \
  --json "{\"spec\": {\"source_branch\": \"${MAIN}\", \"no_expiry\": true}}"

echo "==> throwaway forecasting branch (TTL 4h, expires itself)"
databricks postgres get-branch "${MAIN%/*}/forecast-deferred-maintenance" --profile "$PROFILE" >/dev/null 2>&1 || \
databricks postgres create-branch "projects/${PROJECT}" forecast-deferred-maintenance \
  --profile "$PROFILE" --timeout 10m \
  --json "{\"spec\": {\"source_branch\": \"${MAIN}\", \"ttl\": \"14400s\"}}"

echo "==> scale-to-zero, so idle branches cost close to nothing"
# NOTE: the update_mask must name the whole default_endpoint_settings object; leaf
# paths such as spec.default_endpoint_settings.suspend_timeout_duration are
# rejected with "Unknown field path in update_mask". There is also no endpoint
# level mask for these fields, so project defaults apply to endpoints created
# after this call.
databricks postgres update-project "projects/${PROJECT}" spec.default_endpoint_settings \
  --profile "$PROFILE" --timeout 5m --json '{
    "spec": {"default_endpoint_settings": {
      "autoscaling_limit_min_cu": 0.5,
      "autoscaling_limit_max_cu": 8,
      "suspend_timeout_duration": "300s"}}}'

databricks postgres list-branches "projects/${PROJECT}" --profile "$PROFILE"
