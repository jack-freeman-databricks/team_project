# Build 1: Lakebase — evidence map

Lakebase instance: `projects/ironbark-ops` (Postgres 17.11), workspace
`fevm-serverless-stable-1s8h43`. Scenario: Ironbark Resources, an iron ore
crushing plant. All artifacts here are real system output.

| Requirement | Evidence |
|---|---|
| Lakebase instance in code + connectivity check | `lakebase_instance.txt`, `as_code/main.tf`, `as_code/provision.sh` |
| Governed UC table synced into Lakebase, returns rows | `synced_table.sql`, `synced_table_result.json` (13 rows) |
| Operational schema modelled: related tables and keys | `schema_state.json` — live capture: 4 tables, 4 PKs, **2 FKs**, 4 CHECKs, 17 indexes |
| Writable Postgres tables distinct from read-only synced | `writable_vs_synced.json` — 4 writable tables vs `dim_tag_online` (151 rows) |
| Reverse Lakehouse Sync into UC Delta | `reverse_sync_sample.json` |
| Sync defined as code, not UI-only | `as_code/main.tf` (Terraform), `as_code/bundle.yml` (DAB), `as_code/provision.sh` |
| SCD Type 2 history + system metadata columns | `reverse_sync_sample.json` — paired preimage/postimage sharing `_pg_lsn`/`_pg_xid` |
| Development branch off main, creation in code | `branch.txt`, `as_code/create_branches.sh`, `branch_state.json` |
| Branch changes committed as versioned artifacts | `migrations/002_*.sql`, `migrations/003_*.sql` |
| Main stays clean until promotion | `agent_change/promotion_evidence.txt`, `git_history.txt` |
| **Both** branch uses: iteration + throwaway forecasting | `branch_state.json` (`dev-work-order-notes` permanent, `forecast-deferred-maintenance` TTL 4h) + `forecast_branch_result.json` |
| Scale-to-zero configured | `branch_state.json` — forecasting endpoint at 0.5 CU floor, 300s idle suspend |
| Coding agent's change as diff/migration | `agent_change/` |
| Agent change validated by query + result | `agent_change/validation_result.json` (branch **and** main) |
| Change promoted via merge or PR | `git_history.txt`, `pull_request.txt` |
| Layered build evident in commit history | `git_history.txt` |
| Lakebase Search (hybrid vector + full text) | `migrations/003_lakebase_search.sql`, `search_query.txt` |
| Search returns relevant records for NL query | `search_result.json` (5 results, both signals contributing) |
| Business question answered from synced data | `core_question.txt`, `core_query.sql`, `core_query_result.json` (12 rows) |

## Two things stated plainly rather than glossed

**Terraform scope.** The Databricks Terraform provider has no resource type for
Lakebase Autoscaling projects, branches, synced tables or CDF configs; its
`databricks_database_instance` targets the retired Provisioned tier. `main.tf`
therefore declares the topology with `terraform_data` provisioners driving the
`databricks postgres` CLI, with Terraform enforcing dependency order. Same for
DABs, which has no native resource either. Nothing here pretends a resource type
exists that does not.

**Scale-to-zero coverage.** Project `default_endpoint_settings` apply to endpoints
created after they are set, and there is no endpoint-level `update_mask` for those
fields, so the two pre-existing endpoints still carry 1 CU / 86400s. The
forecasting branch endpoint, created afterwards, shows the intended 0.5 CU floor
and 300s suspend. `branch_state.json` records both, unedited.

**Root branch naming.** Lakebase auto-provisions the root branch as `production`,
not `main`. That is this project's clean environment; both development and
forecasting branches were created off it.
