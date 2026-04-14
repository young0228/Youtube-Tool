"""YouTube metadata collector for fixed channels.

This module fetches recent video metadata only. It does not download video/audio.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

from ytradar.config.channels import ChannelConfig
from ytradar.db.repository import VideoRawRecord


@dataclass(slots=True)
class CollectorSettings:
    """Runtime controls for metadata collection."""

    lookback_days: int = 7
    max_retries: int = 3
    retry_backoff_seconds: float = 1.5


class YouTubeMetadataCollector:
    """Collect recent YouTube metadata for configured channels."""

    def __init__(self, api_key: str, settings: CollectorSettings | None = None) -> None:
        self.settings = settings or CollectorSettings()
        self.client = build("youtube", "v3", developerKey=api_key, cache_discovery=False)

    def collect_recent_videos(self, channels: list[ChannelConfig]) -> list[VideoRawRecord]:
        """Collect recent video metadata for each configured channel."""

        cutoff = datetime.now(timezone.utc) - timedelta(days=self.settings.lookback_days)
        all_video_ids: set[str] = set()

        for channel in channels:
            playlist_id = self._get_uploads_playlist_id(channel.channel_id)
            if not playlist_id:
                continue

            video_ids = self._collect_recent_video_ids_from_playlist(playlist_id, cutoff)
            all_video_ids.update(video_ids)

        if not all_video_ids:
            return []

        return self._fetch_video_details(sorted(all_video_ids))

    def _execute_with_retry(self, request: Any) -> dict[str, Any]:
        """Execute a YouTube API request with simple exponential backoff."""

        last_error: Exception | None = None
        for attempt in range(1, self.settings.max_retries + 1):
            try:
                return request.execute()
            except HttpError as exc:  # pragma: no cover (network/API dependent)
                last_error = exc
                if attempt == self.settings.max_retries:
                    raise
                sleep_seconds = self.settings.retry_backoff_seconds * (2 ** (attempt - 1))
                time.sleep(sleep_seconds)

        raise RuntimeError(f"Request failed after retries: {last_error}")

    def _get_uploads_playlist_id(self, channel_id: str) -> str | None:
        """Get a channel's uploads playlist id (quota-efficient path)."""

        request = self.client.channels().list(
            part="contentDetails",
            id=channel_id,
            maxResults=1,
        )
        response = self._execute_with_retry(request)
        items = response.get("items", [])
        if not items:
            return None
        return items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")

    def _collect_recent_video_ids_from_playlist(
        self,
        playlist_id: str,
        cutoff: datetime,
    ) -> list[str]:
        """Collect video IDs newer than `cutoff` from uploads playlist."""

        video_ids: list[str] = []
        next_page_token: str | None = None

        while True:
            request = self.client.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            )
            response = self._execute_with_retry(request)
            items = response.get("items", [])
            should_stop = False

            for item in items:
                snippet = item.get("snippet", {})
                published_at_raw = snippet.get("publishedAt")
                video_id = snippet.get("resourceId", {}).get("videoId")
                if not published_at_raw or not video_id:
                    continue

                published_at = _parse_youtube_datetime(published_at_raw)
                if published_at < cutoff:
                    should_stop = True
                    break

                video_ids.append(video_id)

            if should_stop:
                break

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        return video_ids

    def _fetch_video_details(self, video_ids: list[str]) -> list[VideoRawRecord]:
        """Fetch details/statistics in chunks for collected video IDs."""

        results: list[VideoRawRecord] = []
        fetched_at = datetime.now(timezone.utc)

        for chunk in _chunked(video_ids, size=50):
            request = self.client.videos().list(
                part="snippet,contentDetails,statistics,liveStreamingDetails",
                id=",".join(chunk),
                maxResults=50,
            )
            response = self._execute_with_retry(request)

            for item in response.get("items", []):
                record = _normalize_video_item(item, fetched_at=fetched_at)
                if record is not None:
                    results.append(record)

        return results


def _normalize_video_item(item: dict[str, Any], fetched_at: datetime) -> VideoRawRecord | None:
    """Normalize one YouTube `videos.list` item into a DB-ready record."""

    snippet = item.get("snippet", {})
    stats = item.get("statistics", {})
    content_details = item.get("contentDetails", {})

    video_id = item.get("id")
    channel_id = snippet.get("channelId")
    if not video_id or not channel_id:
        return None

    tags = snippet.get("tags")
    return VideoRawRecord(
        video_id=video_id,
        channel_id=channel_id,
        channel_name=snippet.get("channelTitle", ""),
        title=snippet.get("title", ""),
        description=snippet.get("description"),
        published_at=_parse_youtube_datetime(snippet["publishedAt"]),
        duration=content_details.get("duration"),
        view_count=_safe_int(stats.get("viewCount")),
        like_count=_safe_int(stats.get("likeCount")),
        comment_count=_safe_int(stats.get("commentCount")),
        live_broadcast_flag=snippet.get("liveBroadcastContent"),
        url=f"https://www.youtube.com/watch?v={video_id}",
        tags_json=json.dumps(tags, ensure_ascii=False) if tags else None,
        raw_payload_json=json.dumps(item, ensure_ascii=False),
        fetched_at=fetched_at,
    )


def _parse_youtube_datetime(value: str) -> datetime:
    """Parse YouTube RFC3339 timestamp to timezone-aware UTC datetime."""

    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _safe_int(value: Any) -> int | None:
    """Safely parse integer-like API values."""

    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _chunked(values: list[str], size: int) -> list[list[str]]:
    """Split list into fixed-size chunks."""

    return [values[i : i + size] for i in range(0, len(values), size)]
