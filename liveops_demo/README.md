# LiveOps Demo: Real-time Monitoring + Anomaly Alerting + GenAI Root Cause Analysis

**One-line goal**: After a release, revenue drops → within 5 minutes automatically identify root causes + actionable recommendations + generate incident report.

## Demo Script (12–15 minutes)

| Scene | Duration | Content |
|-------|----------|---------|
| 1 | 1 min | Overview anomaly: dashboard shows Revenue / Payment Success Rate; from 10:05, pay success rate drops, revenue down ~8% |
| 2 | 2 min | One-click anomaly detection: anomaly table/chart marks 10:05–10:40, shows estimated loss and affected DAU share |
| 3 | 4 min | **GenAI Incident Detective**: Ask "Explain revenue drop after 10:00 today…" → Agent outputs top 3 root-cause cards + evidence + actions |
| 4 | 2 min | Root-cause drill-down: click cause #1 to open drill-down view (dimension breakdown + trend + sample logs) |
| 5 | 2 min | Auto-generate Incident report: Generate Postmortem Draft → Slack/Confluence format |

## Data Assets (Unity Catalog)

- `main.liveops_demo.silver_game_events` — behavioral events
- `main.liveops_demo.silver_payments` — payment outcomes
- `main.liveops_demo.gold_kpi_5m` — 5-minute KPI aggregates
- `main.liveops_demo.gold_anomaly` — anomaly detection results

## Run Order

1. **01_setup_schema** — Create schema (and tables if using main catalog)
2. **02_seed_silver_data** — Generate 3 days of mock data with incident at 10:05
3. **03_build_gold_kpi_anomaly** — Build gold_kpi_5m and gold_anomaly
4. **04_dashboard_queries** — SQL for Lakeview (trends + anomaly markers)
5. **05_drill_down** — Root-cause drill-down: version / region / device / error code
6. **06_rca_agent** — GenAI RCA Agent: root-cause cards + evidence
7. **07_incident_report** — Generate postmortem draft

## Anomaly Detection Logic

- Rolling baseline: same time-of-day mean/median over past 7 days
- Threshold: `pay_success_rate < baseline - 3*std` or `z_score < -3`
- Output anomaly windows and impact_estimated

## Fallbacks

- Pre-compute gold tables to avoid slow queries during the demo
- Preset buttons for key questions: "Explain anomaly", "Top drivers", "Generate report"
- Prepare an offline recording using the same script
