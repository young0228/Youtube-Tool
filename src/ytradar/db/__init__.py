"""Database package for YouTube Topic Radar.

This package contains simple helpers to initialize and access the local
DuckDB database used by the MVP pipeline.
"""

from .init_db import initialize_database

__all__ = ["initialize_database"]
