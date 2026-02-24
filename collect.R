# collect.R
# Runs every 10 min via GitHub Actions. Exits immediately outside collection windows.
# Appends observations to data/YYYY-MM-DD.csv and commits via workflow.

suppressPackageStartupMessages({
  library(httr2)
  library(readr)
  library(dplyr)
  library(tidytransit)
  library(jsonlite)
  library(lubridate)
})

# Null-coalescing operator
`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0 && !is.na(a[[1]])) a else b

# ---------------------------------------------------------------------------
# 1. WINDOW CHECK
# ---------------------------------------------------------------------------
now  <- as.POSIXct(Sys.time(), tz = "UTC")
wday <- as.integer(format(now, "%u"))   # 1=Mon … 7=Sun
hhmm <- as.integer(format(now, "%H")) * 100L + as.integer(format(now, "%M"))

in_window <- wday <= 5L &&
  ((hhmm >= 1000L && hhmm < 1400L) ||
   (hhmm >= 1900L && hhmm <= 2330L))

if (!in_window) {
  cat("Outside collection window (", format(now, "%a %H:%M UTC"), "). Exiting.\n")
  quit(save = "no")
}

cat("Collection run:", format(now, "%Y-%m-%d %H:%M UTC"), "\n")

# Service date = current Eastern time date
et_now       <- with_tz(now, "America/New_York")
service_date <- format(et_now, "%Y-%m-%d")
collected_at <- format(now, "%Y-%m-%dT%H:%M:%SZ")

# Output file
dir.create("data", showWarnings = FALSE)
out_file <- paste0("data/", service_date, ".csv")

col_names <- c("service", "route_id", "trip_id", "trip_date",
               "stop_id", "stop_name", "sched_dep", "pred_dep",
               "sched_arr", "pred_arr", "collected_at")

# Accumulate rows across services
all_rows <- list()

# ---------------------------------------------------------------------------
# 2. MARC COLLECTION
# ---------------------------------------------------------------------------
marc_url <- "https://mdotmta-gtfs-rt.s3.amazonaws.com/MARC+RT/marc-tu.pb"

tryCatch({
  cat("Fetching MARC GTFS-RT feed...\n")

  resp <- request(marc_url) |>
    req_timeout(30) |>
    req_perform()

  raw_bytes <- resp_body_raw(resp)

  if (length(raw_bytes) <= 20L) {
    cat("MARC feed is empty (", length(raw_bytes), "bytes). Skipping.\n")
  } else {
    # Load trip reference
    if (!file.exists("ref/marc_trips.csv")) {
      warning("ref/marc_trips.csv not found — run identify_trips.R first. Skipping MARC.")
    } else {
      marc_trips <- read_csv("ref/marc_trips.csv", show_col_types = FALSE)
      marc_stops <- read_csv("ref/marc_stops.csv", show_col_types = FALSE)

      # Write protobuf to temp file then parse
      tmp_pb <- tempfile(fileext = ".pb")
      writeBin(raw_bytes, tmp_pb)

      feed <- tryCatch(
        gtfs_rt_entities(tmp_pb, entity_type = "tu"),
        error = function(e) {
          # Fallback: try passing URL directly
          tidytransit::read_gtfs_rt(marc_url, entity_type = "tu")
        }
      )

      # feed is a list; trip updates are in $trip_update
      # stop_time_update has: trip_id, stop_id, stop_sequence,
      #   departure_time, arrival_time, departure_delay, arrival_delay
      stu <- tryCatch(
        feed$stop_time_update,
        error = function(e) NULL
      )

      if (is.null(stu) || nrow(stu) == 0) {
        cat("MARC: no stop_time_update records parsed.\n")
      } else {
        # Filter to Penn Line trips
        stu_penn <- stu |>
          filter(trip_id %in% marc_trips$trip_id)

        # Identify BAL and WAS stop_ids from ref
        bal_ids <- marc_stops |>
          filter(grepl("Baltimore", stop_name, ignore.case = TRUE)) |>
          pull(stop_id)

        was_ids <- marc_stops |>
          filter(grepl("Union Station", stop_name, ignore.case = TRUE) &
                   grepl("Washington", stop_name, ignore.case = TRUE)) |>
          pull(stop_id)

        target_stops <- c(bal_ids, was_ids)

        stu_target <- stu_penn |>
          filter(stop_id %in% target_stops) |>
          left_join(marc_stops |> select(stop_id, stop_name), by = "stop_id") |>
          left_join(marc_trips |> select(trip_id, route_id), by = "trip_id")

        if (nrow(stu_target) > 0) {
          # Coerce times to ISO-8601; GTFS-RT times are Unix seconds
          iso_utc <- function(x) {
            if (is.null(x) || all(is.na(x))) return(rep(NA_character_, length(x)))
            as.character(format(as.POSIXct(as.numeric(x), origin = "1970-01-01", tz = "UTC"),
                                "%Y-%m-%dT%H:%M:%SZ"))
          }

          marc_rows <- stu_target |>
            transmute(
              service      = "marc",
              route_id     = coalesce(route_id, NA_character_),
              trip_id      = trip_id,
              trip_date    = service_date,
              stop_id      = stop_id,
              stop_name    = stop_name,
              sched_dep    = iso_utc(departure_time),
              pred_dep     = iso_utc(if ("departure_delay" %in% names(stu_target) &&
                                           !is.null(stu_target$departure_delay))
                                        departure_time + departure_delay
                                     else departure_time),
              sched_arr    = iso_utc(arrival_time),
              pred_arr     = iso_utc(if ("arrival_delay" %in% names(stu_target) &&
                                           !is.null(stu_target$arrival_delay))
                                        arrival_time + arrival_delay
                                     else arrival_time),
              collected_at = collected_at
            )

          all_rows[["marc"]] <- marc_rows
          cat("MARC: captured", nrow(marc_rows), "stop observations.\n")
        } else {
          cat("MARC: no Penn Line BAL/WAS stops in feed right now.\n")
        }
      }
    }
  }
}, error = function(e) {
  cat("MARC collection error:", conditionMessage(e), "\n")
})

# ---------------------------------------------------------------------------
# 3. AMTRAK COLLECTION
# ---------------------------------------------------------------------------
amtrak_url <- "https://api-v3.amtraker.com/v3/trains"

tryCatch({
  cat("Fetching Amtrak data from Amtraker v3...\n")

  resp <- request(amtrak_url) |>
    req_timeout(30) |>
    req_headers(Accept = "application/json") |>
    req_perform()

  trains_json <- resp_body_json(resp, simplifyVector = FALSE)

  # trains_json is a named list: train_number -> list of train objects
  amtrak_rows <- list()

  for (train_num in names(trains_json)) {
    train_list <- trains_json[[train_num]]
    if (!is.list(train_list)) next

    for (train in train_list) {
      # Check if NER
      route_name <- train$routeName %||% train$route_name %||% ""
      if (!grepl("Northeast Regional|NER", route_name, ignore.case = TRUE)) next

      stations <- train$stations
      if (is.null(stations) || length(stations) == 0) next

      # Find BAL and WAS stations
      bal_st <- Filter(function(s) s$code == "BAL", stations)
      was_st <- Filter(function(s) s$code == "WAS", stations)

      if (length(bal_st) == 0 || length(was_st) == 0) next

      for (st in c(bal_st, was_st)) {
        stop_code <- st$code
        stop_name_val <- switch(stop_code,
          BAL = "Baltimore Penn Station",
          WAS = "Washington Union Station",
          stop_code
        )

        row <- tibble(
          service      = "amtrak",
          route_id     = "NER",
          trip_id      = as.character(train_num),
          trip_date    = service_date,
          stop_id      = stop_code,
          stop_name    = stop_name_val,
          sched_dep    = as.character(st$schDep %||% NA),
          pred_dep     = as.character(st$dep    %||% NA),
          sched_arr    = as.character(st$schArr %||% NA),
          pred_arr     = as.character(st$arr    %||% NA),
          collected_at = collected_at
        )
        amtrak_rows[[length(amtrak_rows) + 1]] <- row
      }
    }
  }

  if (length(amtrak_rows) > 0) {
    amtrak_tbl <- bind_rows(amtrak_rows)
    all_rows[["amtrak"]] <- amtrak_tbl
    cat("Amtrak: captured", nrow(amtrak_tbl), "stop observations.\n")
  } else {
    cat("Amtrak: no NER trains with both BAL and WAS found.\n")
  }

}, error = function(e) {
  cat("Amtrak collection error:", conditionMessage(e), "\n")
})

# ---------------------------------------------------------------------------
# 4. WRITE OUTPUT
# ---------------------------------------------------------------------------
if (length(all_rows) == 0) {
  cat("No data collected this run.\n")
  quit(save = "no")
}

combined <- bind_rows(all_rows)

# Ensure column order matches schema
for (col in col_names) {
  if (!col %in% names(combined)) combined[[col]] <- NA_character_
}
combined <- combined[, col_names]

# Append to daily file (write header only if file doesn't exist)
file_exists <- file.exists(out_file)
write_csv(combined, out_file, append = file_exists, col_names = !file_exists)

cat("Wrote", nrow(combined), "rows to", out_file,
    if (file_exists) "(appended)" else "(new file)", "\n")
cat("Done.\n")
