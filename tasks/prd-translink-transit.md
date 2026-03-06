# PRD: Translink Transit Data Pipeline & Dashboard

## 1. Introduction / Overview

This project builds a data pipeline and dashboard for the Translink transit system in British Columbia, covering all transit modes (Bus, SkyTrain, SeaBus, West Coast Express). It ingests real-time data from the Translink Open API and static schedule data from GTFS feeds, stores it historically, and exposes both a real-time and historical view of transit routes, delays, and arrival times.

The architecture mirrors the existing TTC data project: Airflow for orchestration, DuckDB as the data warehouse, Polars for transformation, Streamlit for visualization, and S3/Parquet for storage.

**Problem it solves:** Translink's raw API data is difficult to consume directly. This project makes the data accessible, queryable, and visualizable — enabling commuters and analysts to understand transit performance across the full Translink network.

---

## 2. Goals

1. Ingest real-time vehicle positions and trip updates from the Translink Open API on a scheduled cadence.
2. Ingest static GTFS data (routes, stops, schedules, shapes) to serve as dimension tables.
3. Store raw and cleaned data in S3 as Parquet files.
4. Build a DuckDB warehouse with a fact table at the stop-level grain (one row per stop per trip).
5. Provide a Streamlit dashboard showing both real-time transit status and historical performance.
6. Cover all Translink modes: Bus, SkyTrain, SeaBus, and West Coast Express.

---

## 3. User Stories

- **As a commuter**, I want to see live delays and current positions for my route so I can plan my departure time.
- **As an analyst**, I want to query historical arrival data by route, stop, and time range to understand transit reliability trends.
- **As a developer**, I want a working data pipeline that ingests Translink data automatically so the dataset stays fresh without manual intervention.
- **As a portfolio reviewer**, I want to see a functional end-to-end data engineering project using modern tools (Airflow, DuckDB, Polars, Streamlit).

---

## 4. Functional Requirements

### Data Ingestion
1. The system must poll the Translink Open API for real-time vehicle positions on a configurable schedule (e.g., every 60 seconds or per Airflow DAG schedule).
2. The system must poll the Translink Open API for real-time trip updates on the same schedule.
3. The system must ingest static GTFS data (routes, stops, trips, shapes, stop_times) from the Translink GTFS feed on a daily or weekly schedule.
4. The Translink API key must be read from environment variables (`.env`) and must never be hardcoded.
5. Raw API responses must be saved to S3 in Parquet format before any transformation occurs.

### Data Transformation
6. The system must transform raw trip update data into a `stop_arrival_fact` table with one row per stop per trip.
7. The `stop_arrival_fact` table must include at minimum: `trip_id`, `vehicle_id`, `route_id`, `stop_id`, `scheduled_arrival_time`, `actual_arrival_time`, `arrival_delay_seconds`.
8. The system must join fact data to GTFS dimension tables to resolve human-readable route names, stop names, and transit mode.
9. Transformations must be implemented using Polars.

### Storage
10. Cleaned/transformed data must be written to S3 as Parquet files, partitioned by date.
11. DuckDB must read from S3 Parquet files and serve as the query layer for the dashboard.

### Orchestration
12. All pipeline steps (ingest → transform → load) must be orchestrated as Airflow DAGs using `DockerOperator` only.
13. Each Docker container used by Airflow must be self-contained with its own dependencies and credentials.

### Dashboard
14. The Streamlit dashboard must include a **real-time view** showing current vehicle positions and delays by route.
15. The Streamlit dashboard must include a **historical view** allowing filtering by route, stop, transit mode, and date range.
16. The dashboard must display: route name, transit mode, stop name, scheduled vs. actual arrival time, and delay in minutes.
17. All four transit modes (Bus, SkyTrain, SeaBus, West Coast Express) must be selectable in the dashboard.

---

## 5. Non-Goals (Out of Scope)

- Trip planning or journey routing (e.g., "how do I get from A to B") — this is a data/analytics tool, not a navigation app.
- Push notifications or alerts for individual users.
- Mobile app or native UI — Streamlit web only.
- Fare or ticketing data.
- Accessibility features beyond what Streamlit provides out of the box.
- Infrastructure automation beyond what already exists in the TTC project (Terraform for S3, Lambda, ECR).

---

## 6. Design Considerations

- Follow the same data modeling pattern as the TTC project: grain of fact table = **one row per stop per trip**.
- The `route_id` and `stop_id` from the Translink API should be cross-referenced with GTFS static dimension tables to resolve names and modes.
- Dashboard layout should mirror the TTC Streamlit app structure where applicable, for consistency.
- Transit mode should be derivable from the GTFS `routes.txt` `route_type` field:
  - `0` = Tram/Streetcar (SkyTrain light rail)
  - `1` = Subway/Metro (SkyTrain heavy rail)
  - `2` = Rail (West Coast Express)
  - `3` = Bus
  - `4` = Ferry (SeaBus)

---

## 7. Technical Considerations

- **API Auth:** Translink Open API requires an API key. Store in `.env` and inject into Docker containers at runtime via environment variables. Do not commit the key to version control.
- **GTFS Static Source:** Download from the [Translink GTFS feed](https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources/gtfs). Re-ingest on a weekly schedule to catch schedule changes.
- **Translink vs TTC API format:** Confirm whether Translink's real-time feed uses GTFS-RT binary format or a JSON REST API — the ingestion layer must handle whichever format the Translink Open API returns.
- **DuckDB + S3:** Use DuckDB's `httpfs` extension to query Parquet files directly from S3.
- **Docker images:** Each pipeline step (ingest, transform, load) should be a separate Docker image, following the pattern established in the TTC project.
- **IaC:** Reuse or extend existing Terraform config for S3 buckets, ECR repositories, and Lambda if needed.

---

## 8. Success Metrics

1. The Airflow pipeline runs end-to-end without manual intervention and ingests real Translink data.
2. The `stop_arrival_fact` table is populated with data from at least one full day of Translink operations across all four transit modes.
3. The Streamlit dashboard loads and displays real-time and historical data without errors.
4. A query for "average delay by route" over any 7-day window executes successfully against DuckDB.

---

## 9. Open Questions

1. Does the Translink Open API return GTFS-RT binary format or a REST/JSON format? This affects how the ingestion layer is written.
2. What is the polling frequency limit imposed by the Translink API (rate limiting)? This determines the Airflow DAG schedule interval.
3. Should historical data be retained indefinitely, or is there a retention window (e.g., 90 days rolling)?
4. Should the SeaBus and West Coast Express be treated as separate datasets or unified with bus/SkyTrain in the same pipeline?
5. Are there any existing Terraform resources from the TTC project that can be directly reused for this project, or should Translink infrastructure be isolated in a separate workspace?
