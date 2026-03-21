# Runbook: Incident Response

## Severity Levels

| Level | Example | Response Time | Escalation |
|-------|---------|--------------|-----------|
| P0 — Critical | Food safety violation, payment data exposure | Immediate | PagerDuty + On-call + CTO |
| P1 — High | Daily pipeline missed SLA, Snowflake warehouse down | 30 min | PagerDuty + On-call |
| P2 — Medium | Data quality check failure, stale dashboard | 2 hours | Slack #data-incidents |
| P3 — Low | Non-critical job retry, minor schema drift | Next business day | Jira ticket |

---

## P0: Food Safety Temperature Violation

**Trigger**: Fridge/freezer > 40°F for > 2 hours

### Immediate actions (within 5 minutes)
1. PagerDuty alert auto-fires — confirm receipt
2. Equipment is automatically locked in POS (no new orders using affected equipment)
3. Contact location manager directly: `aws ssm get-parameter --name /restaurant/location-contacts/{location_id}`
4. Check Iceberg time-travel to determine when violation started:
   ```sql
   SELECT MIN(timestamp), MAX(temperature_f)
   FROM bronze.kitchen_telemetry
     BEFORE(TIMESTAMP => '<incident_start>'::TIMESTAMPTZ)
   WHERE location_id = '<location>'
     AND equipment_id = '<equipment>'
     AND temperature_f > 40;
   ```
5. Preserve affected Iceberg snapshot for health authority audit (do NOT expire)

### Recovery
- Equipment repair confirmed → unlock in POS via API
- Log incident in `compliance.food_safety_incidents` table
- Notify health department if duration > 4 hours (local regulations apply)

---

## P1: Daily Pipeline Missed SLA (> 09:00 AM)

**Trigger**: Airflow SLA miss alert for `restaurant_daily_operations`

### Triage steps
1. Check Airflow UI for failed task: `https://airflow.restaurant.internal`
2. Identify failure point (bronze / silver / gold):
   ```bash
   airflow tasks list restaurant_daily_operations --tree
   airflow tasks logs restaurant_daily_operations <task_id> <run_id>
   ```
3. Check downstream impact:
   - Kitchen prep lists not generated → manually push previous day's list
   - Snowflake Gold tables stale → notify BI team

### Common fixes

**POS API timeout** (bronze ingest fails):
```bash
# Trigger manual backfill for specific date
airflow dags backfill restaurant_daily_operations \
  --start-date 2026-03-21 --end-date 2026-03-21 \
  --reset-dagruns
```

**Iceberg compaction stuck**:
```bash
python iceberg/maintenance/compaction_job.py \
  --table bronze.kitchen_telemetry
```

**Snowflake warehouse suspended unexpectedly**:
```sql
ALTER WAREHOUSE DATA_ENGINEERING_WH RESUME;
ALTER WAREHOUSE DATA_ENGINEERING_WH SET AUTO_SUSPEND = 600;
```

---

## P2: Data Quality Failure

**Trigger**: Great Expectations or Soda check fails

1. Identify failing expectation suite from Slack `#data-incidents`
2. Query the raw data to understand scope:
   ```sql
   -- Example: duplicate order_ids
   SELECT order_id, COUNT(*) as n
   FROM bronze.pos_transactions
   WHERE DATE(order_time) = CURRENT_DATE()
   GROUP BY 1 HAVING n > 1;
   ```
3. If data is corrupt: use Iceberg time-travel to restore last clean snapshot:
   ```python
   from pyiceberg.catalog import load_catalog
   catalog = load_catalog("glue")
   table = catalog.load_table("bronze.pos_transactions")
   # Roll back to snapshot before bad data
   table.rollback_to_snapshot(snapshot_id=<last_good_snapshot_id>)
   ```
4. Re-run pipeline from failed task

---

## Contacts

| Role | Contact | PagerDuty |
|------|---------|-----------|
| Data Engineering On-Call | Rotates weekly | `@data-oncall` |
| Snowflake Admin | `snowflake-admin@restaurant.com` | — |
| AWS Account Owner | `cloud-team@restaurant.com` | `@cloud-oncall` |
| Food Safety Officer | `food-safety@restaurant.com` | `@food-safety-oncall` |
