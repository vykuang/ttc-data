# Code architecture

How does *Clean Architecture* influence our TTC dashboard?

- dependency rules take precedence in all design; dependencies must point inward toward domain (use cases, entites); interface adapters depend on application depend on domain, never the reverse.
- SRP guides component boundaries
    - each adapter handles one type of transform
- interface segregation principle: interfaces are focused; small, purpose-specific over monolithic
## layers

- domain logic: transformation of the ttc data
    - pivoting the stop updates in trips
    - enriching with stop scheduled times
    - polars as the interface contract
- application: stitching the domain modules, acts as orchestrator
    - calls interface adapters
    - Request models validates data for domain
    - Response models transform domain objects to be suitable for interface layer
    - Ports interact with external services (file systems, GTFS API)
        - defines the abstract base classes, eg `BaseDataModel`
        - only what application layer requires; implementation detail (S3, local, or otherwise) is abstracted away
    - optional services as `kwargs` dict for extending domain logic that may not be mandatory, eg tracking, logging
- interface adapters: responsible for data I/O; changes to external interfaces won't affect application orchestration or domain logic, and allows multiple interfaces to be used interchangeably
    - inbound (controllers):
        - convert external requests into application-specific formats
        - validates application requirements
        - conversions
    - outbound (presenters, view model):
        - presenters transform application results for external
        - view models bridge presenters and views; only primitive types, simple data structures. easy for views (eg dashboard) to consume formatted output
        - separates core logic and external interfaces
- frameworks-specific adapters (infrastructure)
    - local FS
    - S3
    - API retrieval
    - parquets

## Domain

What are the core concepts, workflows, and rules? After listing these out, not as convinced it maps neatly to an ETL pipeline architecture. Perhaps at the app layer, when it's retrieving the transformed and FE'd gold layer for further manipulation based on user requests?

### Entities

- Trip: journey of a vehicle from one terminal to the other at a specific scheduled time, on a particular day; collection of legs
- leg: journey of a vehicle from one stop to the other on a scheduled trip on a particular day
- Vehicle: specific vehicle on a specific trip
- route: what trips follow; collection of stops
- stops: geographic location that make up a route

### value objects

immutable objects defined by attribute instead of identity

- vehicle position
- vehicle distance
- scheduled arrival time 
- actual arrival time
- leg duration

### domain services

stateless operations not naturally belonging to any specific entity/value objects. Things I want the app to show?

- sort trips based on total delay

## Application layer

- thin layer that stitches together the pipeline
- error handling and data validation
- hides infra details from domain, and domain from interfaces
- lays out the contract for what the interface adapters must agree to via abstract base classes (dependency injection)
## interface adapter usage in orchestrator

```py
# src/orchestrator.py
from dataclasses import dataclass
from src.core.data_model import LocalDataModel, S3DataModel, BaseDataModel

@dataclass
class IngestConfig:
    backend: str              # "local" | "s3"
    source_path: str          # e.g. "data/raw/trip.json" or "s3://bucket/raw/trip.json"
    parquet_out: str          # e.g. "data/dims/trip.parquet" or "s3://bucket/dims/trip.parquet"

def get_storage_backend(name: str) -> BaseDataModel:
    if name == "local":
        return LocalDataModel()
    if name == "s3":
        return S3DataModel()
    raise ValueError(f"Unsupported backend: {name}")

def run_ingestion(cfg: IngestConfig) -> str:
    storage = get_storage_backend(cfg.backend)

    raw = storage.read_source_file(cfg.source_path)      # same call for local/s3
    df = transform(raw)                                  # your transform logic
    out_uri = storage.write_parquet(df, cfg.parquet_out) # same call for local/s3

    return out_uri
```