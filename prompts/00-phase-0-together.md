# Phase 0: all three of us, one screen, ~45 minutes

Paste this into one Claude Code session while the other two watch. Nobody starts
their own lane until this is done, because everything downstream depends on the
schema agreed here.

---

We are building a real-time iron ore crushing plant monitoring solution on
Databricks in one day, three people working in parallel. Read `contract/naming.md`,
`contract/ddl_uc.sql` and `contract/ddl_lakebase.sql` in this repo first, in that
order. They are the agreed data contract.

Your job in this session is to stand up the shared foundation, nothing more. Do not
build any pipeline, model or app.

1. Confirm the Databricks profile we are all using and check that the workspace has
   what we need: Lakeflow pipelines, Lakebase (`databricks postgres`), Databricks
   Apps, Model Serving, and **classic compute on DBR 18 or later** with dedicated or
   standard access mode. That last one gates the native Lakebase streaming sink, which
   does not run on serverless. Report anything missing before going further, because
   it changes lane A's plan.
2. **Check whether a workspace admin has enabled the "Lakebase Change Data Feed"
   preview** on the workspace Previews page. Only an admin can, and nothing in the
   middle of the architecture works until they have. If it is off, ask for it now
   rather than at 3pm.
3. Check that the catalog we are about to create will not use default storage, and
   that its managed storage is not reachable only through a private endpoint. Lakebase
   CDF supports neither, and finding this out late would cost us the demo.
4. Run `python3 contract/gen_reference_data.py` and confirm it reports that all unit
   nodes close and the wet and dry sides reconcile.
5. Upload the four reference CSVs to the landing volume, then run section A of
   `contract/ddl_uc.sql`. Substitute our three usernames into the GRANT statements.
   Do not run sections B or C: section B is a column contract for tables that
   Lakeflow creates and owns, and section C is created by Lakebase CDF.
6. Verify: `dim_tag` has 121 rows, `dim_flowsheet_node` 13, `dim_flowsheet_arc` 15,
   `dim_rule` 10. Confirm every `measure_tag_id` in `dim_flowsheet_arc` that is not
   null exists in `dim_tag`, and that every `accumulation_tag_id` in
   `dim_flowsheet_node` does too. A broken reference here silently breaks the mass
   balance later.
7. Create the Lakebase project `ironbark-ops` on Postgres 16, 17 or 18 and run
   `contract/ddl_lakebase.sql` against it. Confirm both tables report
   `replica_identity = full`.
8. Print a short summary of what exists now and what each lane should verify before
   starting.

Ask before creating anything not named in the contract. If you think the contract is
wrong, say so and stop, do not work around it.
