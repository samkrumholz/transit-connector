#!/usr/bin/env python3
"""
Download and parse MARC GTFS-RT trip updates feed.
Prints CSV rows to stdout: trip_id, route_id, stop_id,
  arr_time, arr_delay, dep_time, dep_delay
"""
import sys
import csv
import urllib.request
from google.transit import gtfs_realtime_pb2

URL = "https://mdotmta-gtfs-rt.s3.amazonaws.com/MARC+RT/marc-tu.pb"

try:
    resp = urllib.request.urlopen(URL, timeout=30)
    data = resp.read()
except Exception as e:
    print(f"fetch error: {e}", file=sys.stderr)
    sys.exit(0)

if len(data) <= 20:
    print(f"feed empty ({len(data)} bytes)", file=sys.stderr)
    sys.exit(0)

feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(data)

writer = csv.writer(sys.stdout)
writer.writerow(["trip_id", "route_id", "stop_id",
                 "arr_time", "arr_delay", "dep_time", "dep_delay"])

for entity in feed.entity:
    if not entity.HasField("trip_update"):
        continue
    tu       = entity.trip_update
    trip_id  = tu.trip.trip_id
    route_id = tu.trip.route_id
    for stu in tu.stop_time_update:
        has_arr = stu.HasField("arrival")
        has_dep = stu.HasField("departure")
        writer.writerow([
            trip_id, route_id, stu.stop_id,
            stu.arrival.time    if has_arr else "",
            stu.arrival.delay   if has_arr else "",
            stu.departure.time  if has_dep else "",
            stu.departure.delay if has_dep else "",
        ])
