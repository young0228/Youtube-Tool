"""Database package for YouTube Topic Radar.

This package contains simple helpers to initialize and access the local
DuckDB database used by the MVP pipeline.
"""

from .init_db import initialize_database
from .repository import DuckDBRepository, VideoFeatureRecord, VideoRawRecord

__all__ = [
    "initialize_database",
    "DuckDBRepository",
    "VideoRawRecord",
    "VideoFeatureRecord",
]
