"""
Domain entities
Trip, Leg, Vehicle, Route, Stop, etc
uses only python primitives
"""
from dataclasses import dataclass

@dataclass(frozen=True)
class Trip:
    trip_id: str
    route_id: str
    vehicle_id: str