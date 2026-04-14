"""Database schema initialization for YouTube Topic Radar.

The MVP stores only metadata/text-derived signals.
No video or audio assets are downloaded or stored.
"""

from __future__ import annotations

from pathlib import Path

import duckdb


def _create_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all required tables and indexes for the MVP.

    The schema is intentionally small and explicit for solo-operator
    local usage.
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS channels (
            channel_id TEXT PRIMARY KEY,
            channel_key TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            channel_group TEXT NOT NULL,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS videos_raw (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            channel_name TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            published_at TIMESTAMP NOT NULL,
            duration TEXT,
            fetched_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            view_count BIGINT,
            like_count BIGINT,
            comment_count BIGINT,
            live_broadcast_flag TEXT,
            url TEXT NOT NULL,
            tags_json TEXT,
            raw_payload_json TEXT NOT NULL,
            language_hint TEXT,
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS video_features (
            video_id TEXT PRIMARY KEY,
            extracted_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            normalized_text TEXT,
            hours_since_publish DOUBLE,
            views_per_hour DOUBLE,
            engagement_rate DOUBLE,
            comment_velocity DOUBLE,
            keyword_score DOUBLE,
            risk_score DOUBLE,
            is_short_guess BOOLEAN,
            keywords_json TEXT,
            trend_score DOUBLE,
            FOREIGN KEY (video_id) REFERENCES videos_raw(video_id)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS topic_candidates (
            candidate_id BIGINT PRIMARY KEY,
            candidate_type TEXT NOT NULL,
            cluster_key TEXT NOT NULL,
            candidate_title TEXT NOT NULL,
            rationale TEXT,
            score DOUBLE NOT NULL,
            supporting_video_ids_json TEXT,
            generated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        );
        """
    )

    # Practical indexes for common local analysis paths.
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_videos_raw_channel_published
        ON videos_raw (channel_id, published_at);
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_videos_raw_published
        ON videos_raw (published_at);
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_topic_candidates_type_score
        ON topic_candidates (candidate_type, score);
        """
    )


def initialize_database(db_path: str | Path) -> Path:
    """Initialize the DuckDB database file and schema.

    Args:
        db_path: Filesystem path to the DuckDB file.

    Returns:
        The resolved database path.
    """

    resolved_path = Path(db_path).expanduser().resolve()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(resolved_path))
    try:
        _create_schema(conn)
    finally:
        conn.close()

    return resolved_path


if __name__ == "__main__":
    # Simple direct execution option:
    # python -m ytradar.db.init_db
    default_db_path = Path("data/radar.duckdb")
    path = initialize_database(default_db_path)
    print(f"Database initialized at: {path}")
