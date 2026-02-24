# backfill_amtrak.R
# Scrape ASMAD (juckins.net) for historical NER departures/arrivals at BAL and WAS.
# Run once locally. Output goes to data/ alongside live-collected files.
# Be polite: 1-second pause between requests.

suppressPackageStartupMessages({
  library(httr2)
  library(rvest)
  library(dplyr)
  library(readr)
  library(lubridate)
})

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
# NER trains that serve BAL and WAS in our windows.
# Southbound AM (BAL→WAS): trains depart BAL ~6:00–8:45 AM ET
# Northbound PM (WAS→BAL): trains depart WAS ~3:15–6:30 PM ET
# Train numbers change with schedule; these are typical but verify on Amtraker.
#
# To find current train numbers: check https://api-v3.amtraker.com/v3/trains
# and look for NER trains with BAL and WAS in stations list.
# Edit TRAIN_NUMS below after checking.

TRAIN_NUMS <- c(
  # Southbound AM (BAL→WAS)
  "66", "80", "82", "84", "86", "88", "90", "92", "94", "160",
  # Northbound PM (WAS→BAL)
  "67", "81", "83", "85", "87", "89", "91", "93", "95", "161",
  # Northeast Regional numbers rotate; expand this list as needed
  "125", "127", "129", "131", "133", "135", "137", "139", "141", "143",
  "126", "128", "130", "132", "134", "136", "138", "140", "142", "144"
)

STATIONS   <- c("BAL", "WAS")
DATE_START <- Sys.Date() - 365   # 12 months back
DATE_END   <- Sys.Date() - 1     # yesterday (today not yet complete)
ASMAD_URL  <- "https://juckins.net/amtrak_status/archive/html/history.php"
OUT_DIR    <- "data"

col_names <- c("service", "route_id", "trip_id", "trip_date",
               "stop_id", "stop_name", "sched_dep", "pred_dep",
               "sched_arr", "pred_arr", "collected_at")

dir.create(OUT_DIR, showWarnings = FALSE)

# ---------------------------------------------------------------------------
# HELPER: scrape one train × station combination
# ---------------------------------------------------------------------------
scrape_asmad <- function(train_num, station, date_start, date_end) {
  resp <- tryCatch(
    request(ASMAD_URL) |>
      req_url_query(
        train_num  = train_num,
        station    = station,
        date_start = format(date_start, "%m/%d/%Y"),
        date_end   = format(date_end,   "%m/%d/%Y"),
        df         = "1"   # date format flag (may vary; inspect page if needed)
      ) |>
      req_timeout(30) |>
      req_perform(),
    error = function(e) NULL
  )

  if (is.null(resp)) return(NULL)
  if (resp_status(resp) != 200) return(NULL)

  html <- resp_body_string(resp) |> read_html()
  tables <- html_table(html, fill = TRUE)

  if (length(tables) == 0) return(NULL)

  # ASMAD typically returns one table with columns like:
  # Date | Sched Dep | Act Dep | Sched Arr | Act Arr | ...
  # Column names vary; find the largest table
  tbl <- tables[[which.max(sapply(tables, nrow))]]

  if (nrow(tbl) < 2) return(NULL)

  # Normalize column names
  names(tbl) <- tolower(gsub("[^a-z0-9]", "_", names(tbl)))

  tbl
}

# ---------------------------------------------------------------------------
# HELPER: parse time string "HH:MM" on a given date into ISO-8601 UTC
# ASMAD times are Eastern; convert to UTC.
# ---------------------------------------------------------------------------
parse_et_time <- function(date_str, time_str) {
  if (is.na(time_str) || !grepl("\\d{1,2}:\\d{2}", time_str)) return(NA_character_)
  dt <- tryCatch(
    as.POSIXct(paste(date_str, time_str), format = "%Y-%m-%d %H:%M",
               tz = "America/New_York"),
    error = function(e) NA
  )
  if (is.na(dt)) return(NA_character_)
  format(with_tz(dt, "UTC"), "%Y-%m-%dT%H:%M:%SZ")
}

# ---------------------------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------------------------
cat("ASMAD backfill:", DATE_START, "to", DATE_END, "\n")
cat("Trains:", paste(TRAIN_NUMS, collapse = ", "), "\n\n")

all_backfill <- list()

for (train_num in TRAIN_NUMS) {
  for (station in STATIONS) {
    cat("Train", train_num, "@ station", station, "... ")
    Sys.sleep(1)  # be polite

    tbl <- scrape_asmad(train_num, station, DATE_START, DATE_END)

    if (is.null(tbl)) {
      cat("no data\n")
      next
    }

    # Try to identify date and time columns by pattern
    # Typical ASMAD columns (may differ — inspect and adjust):
    # "date", "sch_dep", "act_dep", "sch_arr", "act_arr"
    col_map <- list(
      date     = grep("date",        names(tbl), value = TRUE)[1],
      sched_dep = grep("sch.*dep|sched.*dep", names(tbl), value = TRUE)[1],
      act_dep   = grep("act.*dep|actual.*dep", names(tbl), value = TRUE)[1],
      sched_arr = grep("sch.*arr|sched.*arr", names(tbl), value = TRUE)[1],
      act_arr   = grep("act.*arr|actual.*arr", names(tbl), value = TRUE)[1]
    )

    # If we can't find a date column, skip
    if (is.na(col_map$date)) {
      cat("can't parse columns:", paste(names(tbl), collapse = ", "), "\n")
      next
    }

    stop_name_val <- switch(station,
      BAL = "Baltimore Penn Station",
      WAS = "Washington Union Station",
      station
    )

    rows <- lapply(seq_len(nrow(tbl)), function(i) {
      row   <- tbl[i, ]
      date_val <- as.character(row[[col_map$date]])

      # Parse date — ASMAD may use MM/DD/YYYY
      trip_date <- tryCatch({
        d <- as.Date(date_val, format = "%m/%d/%Y")
        if (is.na(d)) d <- as.Date(date_val)
        format(d, "%Y-%m-%d")
      }, error = function(e) NA_character_)

      if (is.na(trip_date)) return(NULL)

      tibble(
        service      = "amtrak",
        route_id     = "NER",
        trip_id      = as.character(train_num),
        trip_date    = trip_date,
        stop_id      = station,
        stop_name    = stop_name_val,
        sched_dep    = if (!is.na(col_map$sched_dep))
                         parse_et_time(trip_date, as.character(row[[col_map$sched_dep]]))
                       else NA_character_,
        pred_dep     = if (!is.na(col_map$act_dep))
                         parse_et_time(trip_date, as.character(row[[col_map$act_dep]]))
                       else NA_character_,
        sched_arr    = if (!is.na(col_map$sched_arr))
                         parse_et_time(trip_date, as.character(row[[col_map$sched_arr]]))
                       else NA_character_,
        pred_arr     = if (!is.na(col_map$act_arr))
                         parse_et_time(trip_date, as.character(row[[col_map$act_arr]]))
                       else NA_character_,
        collected_at = NA_character_
      )
    })

    valid_rows <- Filter(Negate(is.null), rows)
    if (length(valid_rows) > 0) {
      n <- length(valid_rows)
      all_backfill[[length(all_backfill) + 1]] <- bind_rows(valid_rows)
      cat(n, "rows\n")
    } else {
      cat("0 parseable rows\n")
    }
  }
}

if (length(all_backfill) == 0) {
  cat("\nNo backfill data retrieved. Check ASMAD URL and column names.\n")
  cat("Try opening the URL in a browser:\n")
  cat(ASMAD_URL, "?train_num=66&station=BAL\n")
  quit(save = "no")
}

# ---------------------------------------------------------------------------
# WRITE OUTPUT — one CSV per service date (append to existing data/ files)
# ---------------------------------------------------------------------------
combined <- bind_rows(all_backfill)

# Ensure column order
for (col in col_names) {
  if (!col %in% names(combined)) combined[[col]] <- NA_character_
}
combined <- combined[, col_names]

cat("\nTotal backfill rows:", nrow(combined), "\n")

dates <- unique(combined$trip_date)
dates <- dates[!is.na(dates)]
cat("Covering", length(dates), "service dates\n")

for (d in sort(dates)) {
  day_rows  <- combined |> filter(trip_date == d)
  out_file  <- file.path(OUT_DIR, paste0(d, ".csv"))
  file_exists <- file.exists(out_file)

  # Avoid writing duplicate rows (check for trip_id × stop_id × trip_date)
  if (file_exists) {
    existing <- read_csv(out_file, show_col_types = FALSE)
    new_rows <- anti_join(day_rows, existing,
                          by = c("trip_id", "stop_id", "trip_date", "service"))
    if (nrow(new_rows) == 0) next
    write_csv(new_rows, out_file, append = TRUE, col_names = FALSE)
    cat("Appended", nrow(new_rows), "rows to", out_file, "\n")
  } else {
    write_csv(day_rows, out_file)
    cat("Wrote", nrow(day_rows), "rows to", out_file, "\n")
  }
}

cat("Backfill complete.\n")
