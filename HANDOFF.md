# HANDOFF

## Kickoff prompt for Claude Code

Paste this as the first message in a Claude Code session opened at the repo
root.

---

Read `CLAUDE.md`, `SPEC.md` and `TASKS.md` in full before you do anything
else. They are the contract for this project. `CLAUDE.md` contains hard
constraints including a forbidden-services list and a cost ceiling; treat
every rule in it as non-negotiable.

Then execute **Task 0 only**.

Task 0 is recon. It does not touch AWS. Do not create any AWS resource, do not
write Terraform, and do not begin Task 1 under any circumstances.

For Task 0:

1. Run `python3 scripts/recon.py --selftest` and confirm the parser checks
   pass.
2. Ask me for a contact email address, then set `USER_AGENT` in
   `scripts/recon.py` using it. Wikimedia's policy requires a real contact
   address and blocks clients that ignore it. Do not invent an address.
3. Run `python3 scripts/recon.py` live and capture the full output.
4. If either source returns a 404, the URL path construction in the script is
   the likely cause. Fix it, explain what was wrong, and re-run.
5. Write the Task 0 entry in `NOTES.md` recording: measured hourly file size,
   extrapolated 2-year raw volume, estimated backfill hours at 3 concurrent
   connections, mobile traffic share, whether `pageview_complete` is
   tractable, and the median GDELT mention lag.
6. Make a recommendation: `pageviews` or `pageview_complete`, with the reason
   stated in one paragraph.

Then stop and report. Include the raw script output in your report.

If anything in `SPEC.md` turns out to be wrong or impossible against the real
data, say so explicitly rather than working around it quietly.

---

## Review loop

After each Claude Code task, the output goes back to the mentor chat for
review before the next task starts.

When reporting back, include:

- the full Claude Code response, not a summary of it
- the actual diff for anything touching: `hour_start` arithmetic, the
  `en` / `en.m` union, spike thresholds, or the half-life calculation
- the complete `terraform plan` output for any task that creates resources
- the `NOTES.md` entry written for that task
- current AWS spend, once Task 1 is complete

The spec-critical logic is listed explicitly because those four things are
where a silent error produces plausible-looking output that is entirely wrong,
and no test will catch it unless the test was written knowing the trap.
