# Transit Collector — Project Notes

## Purpose
Collect actual departure/arrival times for MARC Penn Line and Amtrak NER between
Baltimore Penn Station (BAL) and Washington Union Station (WAS). No historical data
exists for MARC, so continuous collection is the only option.

## Services in scope

| Service | Direction | Window |
|---|---|---|
| MARC Penn Line | Baltimore → DC | Depart BAL 6:00–8:45 AM ET |
| Amtrak NER | Baltimore → DC | Depart BAL 6:00–8:45 AM ET |
| MARC Penn Line | DC → Baltimore | Depart WAS 3:15–6:30 PM ET |
| Amtrak NER | DC → Baltimore | Depart WAS 3:15–6:30 PM ET |

MTA buses deferred — require Swiftly API key.

## Architecture: GitHub Actions (not Task Scheduler)
Collection runs in GitHub Actions every 10 minutes. No requirement that user's machine
be on. Repo: `samkrumholz/transit-collector` (private).

## Data sources

### MARC Penn Line (GTFS-RT, no auth)
- Trip updates: `https://mdotmta-gtfs-rt.s3.amazonaws.com/MARC+RT/marc-tu.pb`
- Static GTFS: `https://feeds.mta.maryland.gov/gtfs/marc`
- Feed is ~15 bytes when empty (no trains). Must check size before parsing.
- Parsed with `tidytransit` in R.

### Amtrak NER (Amtraker v3, no auth)
- Live: `https://api-v3.amtraker.com/v3/trains`
- Unofficial wrapper around Amtrak's internal feed.
- Station codes: BAL, WAS.
- Historical backfill: ASMAD (juckins.net) via `backfill_amtrak.R`.

## Project files

| File | Purpose |
|---|---|
| `identify_trips.R` | One-time + weekly: downloads MARC static GTFS, writes ref/ files |
| `collect.R` | Runs every 10 min via Actions; appends to data/YYYY-MM-DD.csv |
| `backfill_amtrak.R` | One-time local run: scrapes ASMAD for 12 months of NER history |
| `.github/workflows/collect.yml` | Actions workflow: every 10 min, runs collect.R |
| `.github/workflows/refresh_gtfs.yml` | Actions workflow: Sundays, runs identify_trips.R |
| `ref/marc_trips.csv` | Penn Line trip ID reference (from static GTFS) |
| `ref/marc_stops.csv` | Penn Line stop reference |
| `data/YYYY-MM-DD.csv` | Daily observation files |

## CSV schema
`service, route_id, trip_id, trip_date, stop_id, stop_name, sched_dep, pred_dep, sched_arr, pred_arr, collected_at`

All times: ISO-8601 UTC strings. `trip_date` = ET service date (YYYY-MM-DD).
`pred_dep` / `pred_arr` = best available predicted or actual time.

## Collection windows (UTC, Mon–Fri only)
- AM: 10:00–13:50 (= 6:00–8:50 AM ET)
- PM: 19:00–23:30 (= 3:00–7:30 PM ET)

## Setup steps (one-time)
1. Create private GitHub repo `transit-collector` (account: samkrumholz)
2. Settings → Actions → General → Workflow permissions → Read and write permissions
3. Push this folder to that repo
4. Actions → Refresh GTFS → Run workflow (populates ref/)
5. Actions → Collect → Run workflow (test manual collection)

## Status
- [x] All files written (2026-02-23)
- [ ] Repo created and files pushed
- [ ] Refresh GTFS workflow run successfully
- [ ] First successful automated collection
- [ ] backfill_amtrak.R run and ASMAD data verified

## Gotchas
- MARC feed is 15 bytes when empty — collect.R checks size before parsing.
- Amtraker v3 is unofficial — failures are caught and don't crash the run.
- ref/marc_trips.csv must exist before first collect.R run.
- ASMAD backfill: verify query parameters against live page before running at scale.
- GitHub cron has ~1 min jitter; fine for 10-min polling.
