"""
Data transfer Objects
"""
from dataclasses import dataclass

@dataclass(frozen=True)
class IngestRequest:
    pass

@dataclass(frozen=True)
class IngestResponse:
    pass
