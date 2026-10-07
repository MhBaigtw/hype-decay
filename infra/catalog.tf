# ---------------------------------------------------------------------------
# Glue Data Catalog database for the curated tables. Task 4.
#
# The database is Terraform's; the TABLES are not. The Iceberg tables are
# created by Athena DDL (scripts/iceberg_build.py), because Iceberg table
# metadata -- snapshots, manifests, the current-metadata pointer -- is written
# by the engine on every commit, and a Terraform-managed table definition would
# drift on every insert. Terraform owns the container; the engine owns what is
# in it.
#
# Cost: the Data Catalog is free for the first million objects and the first
# million requests a month, so this is $0.00.
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "curated" {
  name        = replace(var.project, "-", "_") # Athena identifiers cannot contain "-"
  description = "hype-decay curated tables: page_hour and page_daily (Iceberg). Task 4."

  tags = { Task = "task-4-catalog" }
}

output "glue_database" {
  description = "Glue Data Catalog database holding the curated tables."
  value       = aws_glue_catalog_database.curated.name
}
