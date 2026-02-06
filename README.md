# TTC Data

## Objective

- Showcase the Toronto open data catalogue 
- Improve access to the public data
- Practicing new and old tools (polars, duckdb, airflow)

## Architecture

- source: open data
- orchestration: airflow
- DWH: duckdb
- transformation: polars
- viz: streamlit
- storage: parquet and S3
    - backend for raw API content as well as cleaned layers
- IaC: Terraform
    - S3
    - lambda
    - ECR

## API primer

[real time TTC data in textproto](https://gtfsrt.ttc.ca/)

### Trips API

Endpoint: https://gtfsrt.ttc.ca/trips/update?format=binary

Example entity

```json
entity {
  id: "1318"
  trip_update {
    trip {
      trip_id: "24821010"
      schedule_relationship: SCHEDULED
      route_id: "133"
    }
    stop_time_update {
      stop_sequence: 1
      departure {
        time: 1769375013
      }
      stop_id: "2792"
      schedule_relationship: SCHEDULED
    }
    stop_time_update {
      stop_sequence: 2
      arrival {
        time: 1769375103
      }
      stop_id: "2775"
      schedule_relationship: SCHEDULED
    }
    # nth stop_sequence later...
    vehicle {
      id: "8893"
    }
    timestamp: 1769374791
  }
}
```

- Each trip taken by a vehicle is its own entity, with a unique `trip_id`
- Each route may simultaneously have multiple trips 
- `vehicle_id` to be cross-ref'd with `vehicles/position` endpoint

### Vehicle position API

Endpoint: https://gtfsrt.ttc.ca/vehicles/position?format=binary

```json
entity {
  id: "2"
  vehicle {
    trip {
      trip_id: "100266010"
      schedule_relationship: SCHEDULED
      route_id: "165"
    }
    position {
      latitude: 43.727837
      longitude: -79.47905
      bearing: 70.0
      speed: 6.7056
    }
    current_stop_sequence: 37
    current_status: INCOMING_AT
    timestamp: 1769374993
    stop_id: "2460"
    vehicle {
      id: "3639"
    }
    occupancy_status: FEW_SEATS_AVAILABLE
  }
}
```

- `trip_id` cross-ref'd with tripsUpdate
- details `route_id`, which maps to the actual bus/streetcar/subway route
- `current_stop_sequence` and `current_status` indicates next stop, along with `stop_id`
- `route_id` and `stop_id` should be able to be cross-ref'd with some static dimension dataset

## Data modeling

Goal is to store data from each trip for historical logging. Even though data is presented as entire `trips`, the grain of the `trips` table will be **each stop on each trip**. By increasing the grain fineness, the complexity of varying number of stops is sidestepped. If we kept the grain at the entire trip, a `struct` may have been necessary to capture the different stops, which *smuggles operational complexity into the presentation layer*. In addition:

- can't join on `stops`
- can't filter on `stops`
- can't aggregate

The `stop_arrival_fact` schema:

- trip_id - from source
- vehicle_id
- route_id
- stop_id
- scheduled_arrival_time
- actual_arrival_time
- arrival_delay_seconds
- dwell_time_seconds

From [GTFS TTC routes and schedules dataset](https://open.toronto.ca/dataset/merged-gtfs-ttc-routes-and-schedules/), these dimensions are provided

- routes
- stops
- trips - not sure what it pertains to, contains duplicates with same `trip_id`
- shapes
