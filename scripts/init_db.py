"""Initialize local DuckDB schema for YouTube Topic Radar."""

from __future__ import annotations

import os
from pathlib import Path

from ytradar.db import initialize_database


if __name__ == "__main__":
    db_path = Path(os.getenv("YTRADAR_DB_PATH", "data/radar.duckdb"))
    resolved = initialize_database(db_path)
    print(f"Initialized DuckDB schema: {resolved}")
