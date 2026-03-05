# LiveOps Demo Script (12–15 minutes)

## One-line goal
"After the release, revenue dropped → within 5 minutes we auto-identify root causes, give actionable recommendations, and generate an incident report."

---

## Scene 1 (1 min) — Overview anomaly appears
- **Action**: Open Lakeview dashboard (use query 1 from 04_dashboard_queries as data source)
- **Show**: Revenue / Payment Success Rate line charts at 5-minute granularity
- **Script**: "From 10:05, payment success rate dropped and revenue was down about 8%."

---

## Scene 2 (2 min) — One-click anomaly detection (auto high-light)
- **Action**: Same dashboard or second page: show anomaly table/chart (query 2: anomaly windows; query 3: estimated loss, affected DAU)
- **Script**: "The system automatically marks the anomaly window 10:05–10:40 and shows estimated loss and affected DAU share."

---

## Scene 3 (4 min) — GenAI Incident Detective (main beat)
- **Action**: Open 06_rca_agent; in the dialog enter (or click preset button):
  - "Explain why revenue dropped after 10:00 today. Give me top 3 root causes, evidence (dimensions/numbers/compare window), and recommended actions."
- **Show**: Let the audience see the Agent "at work" — run 1) anomaly window 2) dimension breakdown 3) error_code / provider
- **Output**: Root-cause card format
  - **Root cause #1**: iOS + app_version 1.2.7 + SG pay success rate 2%→18% (evidence: table, compare window)
  - **Root cause #2**: error_code=TOKEN_EXPIRED spike (evidence: top error codes)
  - **Root cause #3**: payment_provider=Stripe on iOS new version failure rate anomaly (evidence: provider slice)
  - **Recommended actions**: Rollback / canary off payment entry / force token refresh / contact provider
- **Script**: "Answers must cite data evidence; read-only, Unity Catalog governed."

---

## Scene 4 (2 min) — Root-cause drill-down (click to explore)
- **Action**: Open 05_drill_down; set time window to 10:00–11:00 (or pass from RCA)
- **Show**: Dimension distribution + time trend + affected users + sample failure logs (error_code, message summary)

---

## Scene 5 (2 min) — Auto-generate Incident report (close the loop)
- **Action**: Open 07_incident_report; run "Generate Postmortem Draft"
- **Show**: Slack/Confluence format: what happened, impact, root causes, mitigation, follow-up, owner, timeline
- **Closing line**: "Before: 2 hours in a war room. Now the platform runs the analysis and generates the report."

---

## Fallbacks
- Preset buttons: "Explain anomaly", "Top drivers", "Generate report"
- Pre-compute gold tables so queries stay fast
- Have an offline recording ready (same script)
