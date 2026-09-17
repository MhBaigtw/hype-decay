# CLAUDE.md

Read this every session. Read `SPEC.md` before touching transform logic.
Read `TASKS.md` to find the current task. Do not skip ahead.

## What this is

A data pipeline that measures how fast public attention decays after a spike.
Portfolio project for a data engineering internship. The point is to
demonstrate AWS competence, so architecture quality matters more than
analytical novelty.

Owner is a CS student, not a professional cloud engineer. Explain AWS
concepts when you introduce them. Do not assume prior AWS knowledge.

## Hard rules

**Never run `terraform apply` without showing the plan and getting explicit
approval.** Not once. Not for "just a bucket."

**Never create a resource with an idle hourly cost.** The total budget for
this project is under $30. These are forbidden unless the owner explicitly
approves them in writing:

- MWAA (Managed Airflow) — roughly $350/month
- Redshift, RDS, OpenSearch, ElastiCache, Neptune
- EMR clusters (EMR Serverless is fine, clusters are not)
- NAT Gateway — use VPC endpoints or public subnets instead
- Kinesis Data Streams — Firehose only, and not before v3
- SageMaker anything

**Allowed services:** S3, Glue Data Catalog, Glue Spark jobs, Athena, Lambda,
Step Functions, EventBridge Scheduler, DynamoDB (on-demand only), SNS,
CloudWatch, IAM, API Gateway HTTP API, AWS Budgets. ECS Fargate and Kinesis
Firehose become allowed at v3 and not before.

**One small compute instance is permitted, for the backfill only.** This is
the single exception to the idle-cost rule above. A 24 to 70 hour transfer must
not depend on a laptop staying awake, and running it from the home connection
puts that IP under the Wikimedia rate limits. Conditions, all required:

- smallest instance type that keeps the transfer saturated
- time-boxed, with the box stated before launch
- terminated on completion, not stopped
- a documented shutdown check, and the termination recorded in `NOTES.md`
- nothing else with an idle hourly cost, before or after

**Region is `us-east-1`.** Everything. No exceptions.

**Every Athena query must filter on a partition column.** The workgroup has a
per-query scan limit configured; if a query trips it, fix the query, do not
raise the limit.

**Glue Spark jobs:** maximum 10 DPU, maximum 30 minute timeout, and always
`--enable-auto-scaling`. A runaway Glue job is the most likely way this
project overruns its budget.

**Never commit credentials.** No AWS keys in code, in Terraform, in `.env`
files, or in test fixtures. Use the IAM role and the local AWS profile.

## Data source constraints — these are not negotiable

**Wikimedia rate limits.** Three connections per IP, enforced, with blocks for
clients that evade it. A real User-Agent with a contact address is required by
their policy. **Do not build a Lambda fan-out that downloads from
dumps.wikimedia.org.** The pattern is: slow sequential resumable transfer into
S3, then parallel processing *from* S3.

**The pageviews filename timestamp is the END of the capture window.** A file
named `...-180000.gz` covers 17:00–18:00 UTC. Every stored row must carry
`hour_start`, computed as filename hour minus one. Getting this wrong shifts
every result by an hour and silently corrupts the entire output.

**Desktop and mobile are separate domain codes.** `en` and `en.m` must be
summed into one project. Reading only `en` undercounts by roughly half.

**Bot traffic does not decay like human traffic.** The `pageviews` dataset is
already agent-filtered to `user`. Task 0 confirmed that against the Wikimedia
REST API to the exact view: desktop 4,024,554, and mobile 5,966,626 =
5,742,163 mobile-web + 224,463 mobile-app. There is nothing to filter out, and
no `agent_type` column to filter on.

The real risk is automated traffic that Wikimedia MISCLASSIFIES as `user`.
Worked example: `.xyz` took 24,025 views in a single hour on 2026-09-10 and is
not caught by the namespace exclusions in `SPEC.md`; the REST daily split for
that title is 121k `user` against 262k `automated`. Inside our data the
user-classified share is indistinguishable from human traffic, so this is a
documented limitation, not something to fix.

Add an optional flag for spikes whose hourly profile is suspiciously flat.
Human attention has a diurnal shape and crawlers do not. The flag is advisory:
it annotates a spike, it never drops one.

## Working agreement

- Small commits, conventional commit messages, one task per branch.
- Write the test before the transform logic. dbt tests count.
- When a task's acceptance criteria are met, stop and report. Do not start the
  next task.
- If a requirement in `SPEC.md` turns out to be wrong or impossible against
  real data, stop and say so. Do not silently reinterpret it.
- If something costs money that `TASKS.md` did not anticipate, stop and ask.

## Teaching requirement

The owner will review this code and be asked about it in interviews. After
each task, write a short entry in `NOTES.md` covering: what was built, which
AWS service does what, one design decision and the alternative that was
rejected, and one thing that would break under 10x load. Keep each entry under
200 words. This is not optional and it is not a nice-to-have.

## Layout

```
scripts/     one-off and recon scripts (recon.py lives here)
infra/       terraform, one module per concern
ingest/      downloaders and lambda handlers
transform/   dbt project (dbt-athena)
api/         read API lambda
web/         frontend
NOTES.md     teaching log, one entry per task
```
