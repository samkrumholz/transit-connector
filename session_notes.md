# Transit Collector — Session Notes

## 2026-02-23

### Implemented
All files created for the GitHub Actions-based transit collector.

**Files written:**
- `identify_trips.R` — downloads MARC static GTFS, finds Penn Line trips with both BAL and WAS stops, writes `ref/marc_trips.csv` and `ref/marc_stops.csv`
- `collect.R` — runs every 10 min via GitHub Actions; exits immediately outside AM/PM windows; collects MARC GTFS-RT and Amtrak NER from Amtraker v3; appends to `data/YYYY-MM-DD.csv`
- `backfill_amtrak.R` — one-time local script to scrape ASMAD (juckins.net) for 12 months of historical NER data; outputs to `data/` in the same schema
- `.github/workflows/collect.yml` — triggers every 10 min; runs collect.R; commits data/
- `.github/workflows/refresh_gtfs.yml` — triggers Sundays 6 AM UTC; runs identify_trips.R; commits ref/
- `.gitignore`

**Collection windows (UTC):**
- AM: 10:00–13:50 (6–8:50 AM ET)
- PM: 19:00–23:30 (3–7:30 PM ET)
- Mon–Fri only

**CSV schema:** `service, route_id, trip_id, trip_date, stop_id, stop_name, sched_dep, pred_dep, sched_arr, pred_arr, collected_at`

### Known issue in collect.R
The `%||%` null-coalescing operator is defined at the bottom of the file but used earlier. R sources the full file before executing so this works, but if the function is refactored into a sourced helper it would need to move. Not a problem in current structure.

### collect.R MARC parsing note
`tidytransit::read_gtfs_rt()` / `gtfs_rt_entities()` return varies by version. If parsing fails the first way, there's a fallback to `read_gtfs_rt()` with URL. If both fail, the error is caught and printed without crashing.

### ASMAD backfill status
Not yet run. ASMAD URL and query parameter format need verification against live page before running. Script prints suggested URL if no data is returned.

### Next steps for user
1. Create private GitHub repo `transit-collector` under account `samkrumholz`
2. Settings → Actions → General → Workflow permissions → Read and write permissions
3. `git init` in `C:/Users/krumh/Claude/transit_collector/`, add remote, push
4. Actions tab → Refresh GTFS → Run workflow (manual trigger to populate ref/)
5. Verify `ref/marc_trips.csv` looks right
6. Actions tab → Collect → Run workflow (test manual run)
7. Check `data/` for a CSV with rows; confirm both marc and amtrak service rows appear
8. (Optional) Run `backfill_amtrak.R` locally after verifying ASMAD URL
