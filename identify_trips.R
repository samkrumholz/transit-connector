# identify_trips.R
# One-time / periodic script to build Penn Line trip reference from static GTFS.
# Run via: Rscript identify_trips.R
# Output: ref/marc_trips.csv, ref/marc_stops.csv

library(httr2)
library(readr)
library(dplyr)

cat("Downloading MARC static GTFS...\n")
gtfs_url <- "https://feeds.mta.maryland.gov/gtfs/marc"
tmp_zip  <- tempfile(fileext = ".zip")
tmp_dir  <- tempdir()

req_result <- tryCatch(
  request(gtfs_url) |> req_perform() |> resp_body_raw(),
  error = function(e) stop("Failed to download GTFS: ", conditionMessage(e))
)
writeBin(req_result, tmp_zip)
unzip(tmp_zip, exdir = tmp_dir, overwrite = TRUE)

cat("Parsing GTFS files...\n")
routes     <- read_csv(file.path(tmp_dir, "routes.txt"),     show_col_types = FALSE)
trips_tbl  <- read_csv(file.path(tmp_dir, "trips.txt"),      show_col_types = FALSE)
stop_times <- read_csv(file.path(tmp_dir, "stop_times.txt"), show_col_types = FALSE)
stops      <- read_csv(file.path(tmp_dir, "stops.txt"),      show_col_types = FALSE)

# --- Penn Line route ---
penn_route <- routes |>
  filter(grepl("Penn", route_long_name, ignore.case = TRUE))

if (nrow(penn_route) == 0) stop("No Penn Line route found. Check routes.txt.")
cat("Penn Line route_id(s):", paste(penn_route$route_id, collapse = ", "), "\n")

penn_trips <- trips_tbl |>
  filter(route_id %in% penn_route$route_id)

# --- Identify BAL and WAS stop_ids ---
# Baltimore Penn Station and Washington Union Station
bal_stops <- stops |>
  filter(grepl("Baltimore", stop_name, ignore.case = TRUE) &
           grepl("Penn", stop_name, ignore.case = TRUE))

was_stops <- stops |>
  filter(grepl("Washington", stop_name, ignore.case = TRUE) |
           grepl("Union Station", stop_name, ignore.case = TRUE))

cat("Baltimore stop candidates:\n"); print(bal_stops[, c("stop_id", "stop_name")])
cat("Washington stop candidates:\n"); print(was_stops[, c("stop_id", "stop_name")])

# Keep trips that serve both BAL and WAS stops
penn_stop_times <- stop_times |>
  filter(trip_id %in% penn_trips$trip_id)

trips_with_bal <- penn_stop_times |>
  filter(stop_id %in% bal_stops$stop_id) |>
  pull(trip_id) |> unique()

trips_with_was <- penn_stop_times |>
  filter(stop_id %in% was_stops$stop_id) |>
  pull(trip_id) |> unique()

valid_trips <- intersect(trips_with_bal, trips_with_was)
cat(length(valid_trips), "Penn Line trips serve both BAL and WAS.\n")

# --- Write ref/marc_trips.csv ---
marc_trips <- penn_trips |>
  filter(trip_id %in% valid_trips) |>
  select(trip_id, route_id, direction_id, trip_headsign)

dir.create("ref", showWarnings = FALSE)
write_csv(marc_trips, "ref/marc_trips.csv")
cat("Wrote ref/marc_trips.csv (", nrow(marc_trips), "rows)\n")

# --- Write ref/marc_stops.csv ---
# All stops that appear in Penn Line trips
all_penn_stop_ids <- penn_stop_times |>
  filter(trip_id %in% valid_trips) |>
  pull(stop_id) |> unique()

marc_stops <- stops |>
  filter(stop_id %in% all_penn_stop_ids) |>
  select(stop_id, stop_name, stop_lat, stop_lon)

write_csv(marc_stops, "ref/marc_stops.csv")
cat("Wrote ref/marc_stops.csv (", nrow(marc_stops), "rows)\n")

# --- Print BAL/WAS stop_ids for reference ---
cat("\nBAL stop_ids used:", paste(bal_stops$stop_id, collapse = ", "), "\n")
cat("WAS stop_ids used:", paste(was_stops$stop_id, collapse = ", "), "\n")
cat("Done.\n")
