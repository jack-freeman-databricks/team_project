# Build 1: Lakebase — evidence map

## Root Branch Naming

Lakebase auto-provisions the root branch as `production` (not `main`). In this project, `production` IS the clean root environment corresponding to the rubric's "main". Both the `dev-work-order-notes` development branch and the `forecast-deferred-maintenance` throwaway branch were created from `projects/ironbark-ops/branches/production` (see creation in `as_code/create_branches.sh` and `branch_state.json`).

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
| Scale-to-zero configured | `branch_state.json` — 0.5 CU floor on all three endpoints, 300s idle suspend on two of three. The production endpoint's suspend could not be changed; see the note in that file for exactly why. |
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

**Scale-to-zero coverage is partial, and we say so.** All three endpoints run the
0.5 CU floor. Two of three also carry a 300s idle suspend. The production
endpoint still carries the 86400s it was created with, because every
`update_mask` path for that field is rejected and the project-level default only
applies to endpoints created afterwards. Recreating that endpoint would inherit
the new default, but the CDF config and the synced table depend on it, so it was
left alone deliberately. We have no evidence that a root branch is excluded from
scale-to-zero by design; 86400s is simply what it was created with.

**Root branch naming.** Lakebase auto-provisions the root branch as `production`,
not `main`. That is this project's clean environment; both development and
forecasting branches were created off it.
