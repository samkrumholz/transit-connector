# collect.R
# Runs every ~10-25 min via GitHub Actions cron (jitter is normal).
# Each invocation polls 3 times, 3 minutes apart, to compensate for jitter.
# Exits immediately if outside collection windows.

suppressPackageStartupMessages({
  library(httr2)
  library(readr)
  library(dplyr)
  library(jsonlite)
  library(lubridate)
})

`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0 && !is.na(a[[1]])) a else b

col_names <- c("service", "route_id", "trip_id", "trip_date",
               "stop_id", "stop_name", "sched_dep", "pred_dep",
               "sched_arr", "pred_arr", "collected_at")

dir.create("data", showWarnings = FALSE)

# ---------------------------------------------------------------------------
# WINDOW CHECK
# ---------------------------------------------------------------------------
in_window <- function(t = Sys.time()) {
  t    <- as.POSIXct(t, tz = "UTC")
  wday <- as.integer(format(t, "%u"))
  hhmm <- as.integer(format(t, "%H")) * 100L + as.integer(format(t, "%M"))
  wday <= 5L &&
    ((hhmm >= 1000L && hhmm < 1400L) ||
     (hhmm >= 1900L && hhmm <= 2330L))
}

if (!in_window()) {
  t <- as.POSIXct(Sys.time(), tz = "UTC")
  cat("Outside collection window (", format(t, "%a %H:%M UTC"), "). Exiting.\n")
  quit(save = "no")
}

# ---------------------------------------------------------------------------
# COLLECTION FUNCTION — called once per poll iteration
# ---------------------------------------------------------------------------
collect_once <- function() {
  now          <- as.POSIXct(Sys.time(), tz = "UTC")
  et_now       <- with_tz(now, "America/New_York")
  service_date <- format(et_now, "%Y-%m-%d")
  collected_at <- format(now, "%Y-%m-%dT%H:%M:%SZ")
  out_file     <- paste0("data/", service_date, ".csv")
  all_rows     <- list()

  cat("\n--- Poll at", format(now, "%H:%M:%S UTC"), "---\n")

  # --- MARC (parsed via Python gtfs-realtime-bindings) ---
  tryCatch({
    if (!file.exists("ref/marc_trips.csv")) {
      cat("MARC: ref/marc_trips.csv missing — skipping.\n")
    } else {
      marc_trips <- read_csv("ref/marc_trips.csv", show_col_types = FALSE)
      marc_stops <- read_csv("ref/marc_stops.csv", show_col_types = FALSE)

      py_out <- system2("python3", "parse_gtfs_rt.py", stdout = TRUE, stderr = FALSE)

      if (length(py_out) <= 1L) {
        cat("MARC: feed empty or parse produced no rows.\n")
      } else {
        stu <- read_csv(paste(py_out, collapse = "\n"), show_col_types = FALSE,
                        col_types = cols(.default = col_character()))

        bal_ids <- marc_stops |>
          filter(grepl("Penn Station", stop_name, ignore.case = TRUE)) |>
          pull(stop_id) |> as.character()
        was_ids <- marc_stops |>
          filter(grepl("Union Station", stop_name, ignore.case = TRUE) &
                   grepl("Washington",  stop_name, ignore.case = TRUE)) |>
          pull(stop_id) |> as.character()

        iso_utc <- function(x) {
          ifelse(is.na(x) | x == "",
                 NA_character_,
                 format(as.POSIXct(as.numeric(x), origin = "1970-01-01", tz = "UTC"),
                        "%Y-%m-%dT%H:%M:%SZ"))
        }

        stu_target <- stu |>
          select(-route_id) |>
          filter(trip_id %in% marc_trips$trip_id,
                 stop_id %in% c(bal_ids, was_ids)) |>
          left_join(marc_stops |> select(stop_id, stop_name) |>
                      mutate(stop_id = as.character(stop_id)), by = "stop_id") |>
          left_join(marc_trips |> select(trip_id, route_id),  by = "trip_id")

        if (nrow(stu_target) > 0) {
          marc_rows <- stu_target |>
            transmute(
              service      = "marc",
              route_id     = as.character(route_id),
              trip_id      = trip_id,
              trip_date    = service_date,
              stop_id      = stop_id,
              stop_name    = stop_name,
              sched_dep    = iso_utc(dep_time),
              pred_dep     = iso_utc(ifelse(!is.na(dep_delay) & dep_delay != "",
                                            as.character(as.numeric(dep_time) +
                                                           as.numeric(dep_delay)),
                                            dep_time)),
              sched_arr    = iso_utc(arr_time),
              pred_arr     = iso_utc(ifelse(!is.na(arr_delay) & arr_delay != "",
                                            as.character(as.numeric(arr_time) +
                                                           as.numeric(arr_delay)),
                                            arr_time)),
              collected_at = collected_at
            )
          all_rows[["marc"]] <- marc_rows
          cat("MARC:", nrow(marc_rows), "observations.\n")
        } else {
          cat("MARC: no Penn Line BAL/WAS stops in feed.\n")
        }
      }
    }
  }, error = function(e) cat("MARC error:", conditionMessage(e), "\n"))

  # --- AMTRAK ---
  # Convert Amtrak ISO-8601 strings (with ET offset) to UTC Z format
  to_utc <- function(x) {
    if (is.null(x) || length(x) == 0) return(NA_character_)
    x <- as.character(x)
    if (is.na(x) || x == "") return(NA_character_)
    tryCatch(
      format(lubridate::as_datetime(x), "%Y-%m-%dT%H:%M:%SZ"),
      error = function(e) x
    )
  }

  tryCatch({
    resp        <- request("https://api-v3.amtraker.com/v3/trains") |>
      req_timeout(30) |>
      req_headers(Accept = "application/json") |>
      req_perform()
    trains_json <- resp_body_json(resp, simplifyVector = FALSE)
    amtrak_rows <- list()

    for (train_num in names(trains_json)) {
      for (train in trains_json[[train_num]]) {
        route_name <- train$routeName %||% train$route_name %||% ""
        if (!grepl("Northeast Regional|NER", route_name, ignore.case = TRUE)) next
        stations <- train$stations
        if (is.null(stations)) next
        bal_st <- Filter(function(s) s$code == "BAL", stations)
        was_st <- Filter(function(s) s$code == "WAS", stations)
        if (length(bal_st) == 0 || length(was_st) == 0) next

        for (st in c(bal_st, was_st)) {
          amtrak_rows[[length(amtrak_rows) + 1]] <- tibble(
            service      = "amtrak",
            route_id     = "NER",
            trip_id      = as.character(train_num),
            trip_date    = service_date,
            stop_id      = st$code,
            stop_name    = switch(st$code,
                             BAL = "Baltimore Penn Station",
                             WAS = "Washington Union Station",
                             st$code),
            sched_dep    = to_utc(st$schDep %||% NA),
            pred_dep     = to_utc(st$dep    %||% NA),
            sched_arr    = to_utc(st$schArr %||% NA),
            pred_arr     = to_utc(st$arr    %||% NA),
            collected_at = collected_at
          )
        }
      }
    }

    if (length(amtrak_rows) > 0) {
      all_rows[["amtrak"]] <- bind_rows(amtrak_rows)
      cat("Amtrak:", nrow(all_rows[["amtrak"]]), "observations.\n")
    } else {
      cat("Amtrak: no NER trains with BAL+WAS.\n")
    }
  }, error = function(e) cat("Amtrak error:", conditionMessage(e), "\n"))

  # --- WRITE ---
  if (length(all_rows) == 0) {
    cat("No data this poll.\n")
    return(invisible(NULL))
  }

  combined <- bind_rows(all_rows)
  for (col in col_names) {
    if (!col %in% names(combined)) combined[[col]] <- NA_character_
  }
  combined <- combined[, col_names]

  file_exists <- file.exists(out_file)
  write_csv(combined, out_file, append = file_exists, col_names = !file_exists)
  cat("Wrote", nrow(combined), "rows to", out_file,
      if (file_exists) "(appended)" else "(new file)", "\n")
}

# ---------------------------------------------------------------------------
# POLL LOOP — 3 iterations, 3 minutes apart
# ---------------------------------------------------------------------------
N_POLLS      <- 3L
SLEEP_SEC    <- 180L   # 3 minutes

for (i in seq_len(N_POLLS)) {
  if (!in_window()) {
    cat("Window closed after poll", i - 1L, "— stopping.\n")
    break
  }
  collect_once()
  if (i < N_POLLS) {
    cat("Sleeping", SLEEP_SEC / 60, "min before next poll...\n")
    Sys.sleep(SLEEP_SEC)
  }
}

cat("\nDone.\n")
