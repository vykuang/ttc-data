# trip_id in GTFS

## Definition
A `trip_id` is one scheduled run of a route for a service pattern (for example, route 10 eastbound departing at 08:15 on weekdays).

It is not:
- a specific physical vehicle
- a unique daily runtime instance in static GTFS

From `trips.txt`:
- many `trip_id` values can share the same `route_id` and `service_id`
- each `trip_id` represents a distinct scheduled departure
- grain: one row in `trips` = one scheduled departure

## Why `stop_times` Uses `trip_id` (Not `route_id`)
Each departure has different times at each stop, so stop times must be tied to the departure (`trip_id`), not only the route (`route_id`).

Hierarchy:

```text
route_id
 └── trip_id
      └── stop_sequence + arrival_time / departure_time
```

If keyed only by `route_id`, departures would be collapsed and per-departure timing would be lost.

## Joining Scheduled and Actual Times
Canonical join key:
- (`trip_id`, `stop_sequence`)

Why:
- `trip_id` identifies the scheduled run
- `stop_sequence` identifies the stop within that run

Data sources:
- GTFS-RT trip updates: `trip_id` + `stop_time_update[].arrival.time` (Unix timestamp, actual/predicted)
- Static GTFS `stop_times`: `arrival_time` (wall-clock string)

## Time Conversion Caveat
`stop_times.arrival_time` is a wall-clock value (for example, `08:15:00`), not a Unix timestamp. To compare it with GTFS-RT timestamps, anchor it to the trip service date.

Also handle post-midnight times such as `25:30:00` (1:30 AM on the next calendar day).

This is the standard GTFS + GTFS-RT approach for schedule-adherence calculations.