"""DuckDB persistence helpers for metadata collection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb


@dataclass(slots=True)
class VideoRawRecord:
    """Normalized raw metadata record stored in `videos_raw`."""

    video_id: str
    channel_id: str
    channel_name: str
    title: str
    description: str | None
    published_at: datetime
    duration: str | None
    view_count: int | None
    like_count: int | None
    comment_count: int | None
    live_broadcast_flag: str | None
    url: str
    tags_json: str | None
    raw_payload_json: str
    fetched_at: datetime


@dataclass(slots=True)
class VideoFeatureRecord:
    """Deterministic engineered feature record for one video."""

    video_id: str
    normalized_text: str
    hours_since_publish: float
    views_per_hour: float
    engagement_rate: float
    comment_velocity: float
    keyword_score: float
    risk_score: float
    trend_score: float
    is_short_guess: bool
    keywords_json: str | None
    extracted_at: datetime


@dataclass(slots=True)
class TopicCandidateRecord:
    """Clustered topic candidate ready for persistence."""

    topic_cluster_id: str
    representative_label: str
    date_kst: str
    source_video_count: int
    source_channel_count: int
    source_channels_json: str | None
    average_trend_score: float
    top_video_id: str
    recommended_format: str
    status: str
    generated_at: datetime


class DuckDBRepository:
    """Simple repository for channels/videos tables."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(Path(db_path).expanduser().resolve())

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def upsert_channels(self, channels: list[tuple[str, str, str, str]]) -> None:
        """Upsert channels into the channels table.

        Args:
            channels: List of tuples:
                (channel_id, channel_key, display_name, channel_group)
        """

        if not channels:
            return

        sql = """
        INSERT INTO channels (channel_id, channel_key, display_name, channel_group)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(channel_id) DO UPDATE SET
            channel_key = excluded.channel_key,
            display_name = excluded.display_name,
            channel_group = excluded.channel_group,
            updated_at = current_timestamp;
        """

        conn = self._connect()
        try:
            conn.executemany(sql, channels)
        finally:
            conn.close()

    def upsert_videos_raw(self, videos: list[VideoRawRecord]) -> None:
        """Upsert normalized raw metadata records into `videos_raw`."""

        if not videos:
            return

        sql = """
        INSERT INTO videos_raw (
            video_id,
            channel_id,
            channel_name,
            title,
            description,
            published_at,
            duration,
            view_count,
            like_count,
            comment_count,
            live_broadcast_flag,
            url,
            tags_json,
            raw_payload_json,
            fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            channel_id = excluded.channel_id,
            channel_name = excluded.channel_name,
            title = excluded.title,
            description = excluded.description,
            published_at = excluded.published_at,
            duration = excluded.duration,
            view_count = excluded.view_count,
            like_count = excluded.like_count,
            comment_count = excluded.comment_count,
            live_broadcast_flag = excluded.live_broadcast_flag,
            url = excluded.url,
            tags_json = excluded.tags_json,
            raw_payload_json = excluded.raw_payload_json,
            fetched_at = excluded.fetched_at;
        """

        conn = self._connect()
        try:
            rows = [
                (
                    v.video_id,
                    v.channel_id,
                    v.channel_name,
                    v.title,
                    v.description,
                    v.published_at,
                    v.duration,
                    v.view_count,
                    v.like_count,
                    v.comment_count,
                    v.live_broadcast_flag,
                    v.url,
                    v.tags_json,
                    v.raw_payload_json,
                    v.fetched_at,
                )
                for v in videos
            ]
            conn.executemany(sql, rows)
        finally:
            conn.close()

    def fetch_videos_raw(self) -> list[dict]:
        """Fetch metadata rows required for feature engineering."""

        sql = """
        SELECT
            video_id,
            title,
            description,
            duration,
            published_at,
            view_count,
            like_count,
            comment_count
        FROM videos_raw;
        """
        conn = self._connect()
        try:
            rows = conn.execute(sql).fetchall()
            columns = [col[0] for col in conn.description]
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()

    def upsert_video_features(self, features: list[VideoFeatureRecord]) -> None:
        """Upsert engineered features into `video_features`."""

        if not features:
            return

        sql = """
        INSERT INTO video_features (
            video_id,
            normalized_text,
            hours_since_publish,
            views_per_hour,
            engagement_rate,
            comment_velocity,
            keyword_score,
            risk_score,
            trend_score,
            is_short_guess,
            keywords_json,
            extracted_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            normalized_text = excluded.normalized_text,
            hours_since_publish = excluded.hours_since_publish,
            views_per_hour = excluded.views_per_hour,
            engagement_rate = excluded.engagement_rate,
            comment_velocity = excluded.comment_velocity,
            keyword_score = excluded.keyword_score,
            risk_score = excluded.risk_score,
            trend_score = excluded.trend_score,
            is_short_guess = excluded.is_short_guess,
            keywords_json = excluded.keywords_json,
            extracted_at = excluded.extracted_at;
        """

        conn = self._connect()
        try:
            rows = [
                (
                    f.video_id,
                    f.normalized_text,
                    f.hours_since_publish,
                    f.views_per_hour,
                    f.engagement_rate,
                    f.comment_velocity,
                    f.keyword_score,
                    f.risk_score,
                    f.trend_score,
                    f.is_short_guess,
                    f.keywords_json,
                    f.extracted_at,
                )
                for f in features
            ]
            conn.executemany(sql, rows)
        finally:
            conn.close()

    def fetch_rows_for_clustering(self) -> list[dict]:
        """Fetch rows needed for deterministic topic clustering."""

        sql = """
        SELECT
            vr.video_id,
            vr.channel_id,
            vr.title,
            vf.normalized_text,
            vf.keywords_json,
            vf.trend_score
        FROM videos_raw vr
        INNER JOIN video_features vf ON vr.video_id = vf.video_id
        WHERE vf.trend_score IS NOT NULL;
        """
        conn = self._connect()
        try:
            rows = conn.execute(sql).fetchall()
            columns = [col[0] for col in conn.description]
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()

    def upsert_topic_candidates(self, candidates: list[TopicCandidateRecord]) -> None:
        """Upsert clustered topic candidates."""

        if not candidates:
            return

        sql = """
        INSERT INTO topic_candidates (
            topic_cluster_id,
            representative_label,
            date_kst,
            source_video_count,
            source_channel_count,
            source_channels_json,
            average_trend_score,
            top_video_id,
            recommended_format,
            status,
            generated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(topic_cluster_id) DO UPDATE SET
            representative_label = excluded.representative_label,
            date_kst = excluded.date_kst,
            source_video_count = excluded.source_video_count,
            source_channel_count = excluded.source_channel_count,
            source_channels_json = excluded.source_channels_json,
            average_trend_score = excluded.average_trend_score,
            top_video_id = excluded.top_video_id,
            recommended_format = excluded.recommended_format,
            status = excluded.status,
            generated_at = excluded.generated_at;
        """
        conn = self._connect()
        try:
            rows = [
                (
                    c.topic_cluster_id,
                    c.representative_label,
                    c.date_kst,
                    c.source_video_count,
                    c.source_channel_count,
                    c.source_channels_json,
                    c.average_trend_score,
                    c.top_video_id,
                    c.recommended_format,
                    c.status,
                    c.generated_at,
                )
                for c in candidates
            ]
            conn.executemany(sql, rows)
        finally:
            conn.close()

    def fetch_topic_candidates_report_rows(self) -> list[dict]:
        """Fetch denormalized topic candidate rows for reporting/export."""

        sql = """
        SELECT
            tc.topic_cluster_id,
            tc.representative_label,
            tc.date_kst,
            tc.source_video_count,
            tc.source_channel_count,
            tc.source_channels_json,
            tc.average_trend_score,
            tc.top_video_id,
            vr.url AS top_video_url,
            tc.recommended_format,
            tc.status,
            tc.generated_at
        FROM topic_candidates tc
        LEFT JOIN videos_raw vr ON tc.top_video_id = vr.video_id
        ORDER BY tc.average_trend_score DESC;
        """
        conn = self._connect()
        try:
            rows = conn.execute(sql).fetchall()
            columns = [col[0] for col in conn.description]
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()

    def fetch_channel_name_map(self) -> dict[str, str]:
        """Fetch channel id -> display name mapping for report labels."""

        sql = "SELECT channel_id, display_name FROM channels;"
        conn = self._connect()
        try:
            rows = conn.execute(sql).fetchall()
            return {str(channel_id): str(display_name) for channel_id, display_name in rows}
        finally:
            conn.close()
