# Ironbark Lakebase, declared with Terraform.
#
# HONEST SCOPE NOTE. The Databricks Terraform provider has no resource type for
# Lakebase Autoscaling projects, branches, synced tables, or Change Data Feed
# configurations at the time of writing. The provider's lakebase-adjacent
# resource (databricks_database_instance) targets the retired Provisioned tier,
# which no longer exists and must not be created.
#
# Rather than declare resources that do not exist, the unsupported objects are
# driven through the `databricks postgres` CLI from terraform_data provisioners.
# That keeps the whole topology declared, version controlled, planned and applied
# as code, with dependency ordering enforced by Terraform, which is what "defined
# as code, not UI-only" asks for. Swap each block for a native resource when the
# provider gains one.

terraform {
  required_version = ">= 1.5"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.50"
    }
  }
}

variable "profile"    { type = string, default = "ironbark" }
variable "project"    { type = string, default = "ironbark-ops" }
variable "uc_catalog" { type = string, default = "jack_freeman_catalog" }
variable "uc_schema"  { type = string, default = "tech_summit_scada_build" }
variable "pg_schema"  { type = string, default = "plant" }

provider "databricks" {
  profile = var.profile
}

locals {
  main_branch = "projects/${var.project}/branches/production"
  database    = "projects/${var.project}/branches/production/databases/databricks-postgres"
  cli         = "databricks --profile ${var.profile}"
}

# 1. Lakebase Autoscaling project: Postgres 17, native login on (required for
#    executor-side foreach sinks), scale-to-zero after 5 minutes idle.
resource "terraform_data" "project" {
  input = { project = var.project }
  provisioner "local-exec" {
    command = <<-EOT
      ${local.cli} postgres get-project projects/${var.project} >/dev/null 2>&1 || \
      ${local.cli} postgres create-project ${var.project} --json '{
        "spec": {"display_name":"${var.project}","pg_version":17,
                 "enable_pg_native_login":true,
                 "default_endpoint_settings":{"autoscaling_limit_min_cu":0.5,
                                              "autoscaling_limit_max_cu":8,
                                              "suspend_timeout_duration":"300s"}}}'
    EOT
  }
}

# 2. Operational schema and migrations, idempotent so re-apply is safe.
resource "terraform_data" "migrations" {
  depends_on = [terraform_data.project]
  triggers_replace = [filesha256("${path.module}/../migrations/002_add_work_order_note.sql"),
                      filesha256("${path.module}/../migrations/003_lakebase_search.sql")]
  provisioner "local-exec" {
    command = "python3 ${path.module}/apply_migrations.py production"
  }
}

# 3. Branches: a permanent development branch and a throwaway forecasting branch.
resource "terraform_data" "branches" {
  depends_on = [terraform_data.migrations]
  provisioner "local-exec" {
    command = "PROFILE=${var.profile} bash ${path.module}/create_branches.sh"
  }
}

# 4. Forward sync: governed UC table into Lakebase, read-only in Postgres.
#    TRIGGERED scheduling requires Delta CDF on the source table.
resource "terraform_data" "synced_table" {
  depends_on = [terraform_data.migrations]
  provisioner "local-exec" {
    command = <<-EOT
      ${local.cli} postgres get-synced-table \
        synced_tables/${var.uc_catalog}.${var.uc_schema}.dim_tag_online >/dev/null 2>&1 || \
      ${local.cli} postgres create-synced-table \
        ${var.uc_catalog}.${var.uc_schema}.dim_tag_online --timeout 15m --json '{
          "spec": {"source_table_full_name":"${var.uc_catalog}.${var.uc_schema}.dim_tag",
                   "primary_key_columns":["tag_id"],
                   "scheduling_policy":"TRIGGERED",
                   "branch":"${local.main_branch}",
                   "postgres_database":"databricks_postgres",
                   "create_database_objects_if_missing":true,
                   "new_pipeline_spec":{"storage_catalog":"${var.uc_catalog}",
                                        "storage_schema":"${var.uc_schema}"}}}'
    EOT
  }
}

# 5. Reverse sync: writable Postgres schema into UC Delta as SCD Type 2.
#    Schema level, so every current and future table in plant joins the feed.
resource "terraform_data" "reverse_cdf" {
  depends_on = [terraform_data.migrations]
  provisioner "local-exec" {
    command = <<-EOT
      ${local.cli} postgres get-cdf-config \
        ${local.database}/cdf-configs/ironbark_plant_cdf >/dev/null 2>&1 || \
      ${local.cli} postgres create-cdf-config ${local.database} \
        ${var.uc_catalog} ${var.uc_schema} ${var.pg_schema} \
        --cdf-config-id ironbark_plant_cdf --timeout 15m
    EOT
  }
}

# 6. Lakebase Search: pgvector before lakebase_vector, then the BM25 and ANN
#    indexes. Requires the Lakebase Search preview enabled for the workspace.
resource "terraform_data" "search" {
  depends_on = [terraform_data.migrations]
  provisioner "local-exec" {
    command = "python3 ${path.module}/enable_search.py"
  }
}

output "lakebase_project" { value = "projects/${var.project}" }
output "synced_table"     { value = "${var.uc_catalog}.${var.uc_schema}.dim_tag_online" }
output "cdf_config"       { value = "${local.database}/cdf-configs/ironbark_plant_cdf" }
