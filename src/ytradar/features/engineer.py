"""Deterministic feature engineering from `videos_raw` into `video_features`."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ytradar.config.features import FeatureConfig
from ytradar.db import VideoFeatureRecord


_TEXT_CLEAN_RE = re.compile(r"[^\w\s가-힣]")
_DURATION_PART_RE = re.compile(r"(\d+)([HMS])")


@dataclass(slots=True)
class FeatureInputRow:
    """Input projection read from `videos_raw`."""

    video_id: str
    title: str
    description: str | None
    duration: str | None
    published_at: datetime
    view_count: int | None
    like_count: int | None
    comment_count: int | None


class FeatureEngineer:
    """Compute deterministic, explainable features for videos."""

    def __init__(self, config: FeatureConfig) -> None:
        self.config = config

    def build_features(self, rows: list[dict[str, Any]]) -> list[VideoFeatureRecord]:
        """Build feature records from raw DB rows."""

        now = datetime.now(timezone.utc)
        parsed_rows = [self._to_input_row(row) for row in rows]

        # Dataset-level maxima keep values in a stable 0..1 range.
        max_views_per_hour = max((self._views_per_hour_raw(r, now) for r in parsed_rows), default=1.0)
        max_comment_velocity = max((self._comment_velocity_raw(r, now) for r in parsed_rows), default=1.0)

        features: list[VideoFeatureRecord] = []
        for row in parsed_rows:
            normalized_text = _normalize_text(row.title, row.description)
            hours_since_publish = _hours_since(row.published_at, now)
            views_per_hour_raw = self._views_per_hour_raw(row, now)
            comment_velocity_raw = self._comment_velocity_raw(row, now)

            views_per_hour = _bounded_ratio(views_per_hour_raw, max_views_per_hour)
            engagement_rate = _bounded_ratio(_engagement_rate_raw(row), 0.20)
            comment_velocity = _bounded_ratio(comment_velocity_raw, max_comment_velocity)
            keyword_score, matched_keywords = _keyword_score(normalized_text, self.config.keyword_terms)
            risk_score, _ = _keyword_score(normalized_text, self.config.risk_terms)
            is_short_guess = _is_short_guess(row.duration)

            trend_score = _trend_score(
                views_per_hour=views_per_hour,
                engagement_rate=engagement_rate,
                comment_velocity=comment_velocity,
                keyword_score=keyword_score,
                risk_score=risk_score,
                weights=self.config,
            )

            features.append(
                VideoFeatureRecord(
                    video_id=row.video_id,
                    normalized_text=normalized_text,
                    hours_since_publish=hours_since_publish,
                    views_per_hour=views_per_hour_raw,
                    engagement_rate=_engagement_rate_raw(row),
                    comment_velocity=comment_velocity_raw,
                    keyword_score=keyword_score,
                    risk_score=risk_score,
                    trend_score=trend_score,
                    is_short_guess=is_short_guess,
                    keywords_json=json.dumps(matched_keywords, ensure_ascii=False),
                    extracted_at=now,
                )
            )

        return features

    @staticmethod
    def _to_input_row(row: dict[str, Any]) -> FeatureInputRow:
        """Convert DB row dict into typed input dataclass."""

        published_at = row.get("published_at")
        if isinstance(published_at, str):
            published_at = datetime.fromisoformat(published_at.replace("Z", "+00:00"))

        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)

        return FeatureInputRow(
            video_id=str(row["video_id"]),
            title=str(row.get("title") or ""),
            description=row.get("description"),
            duration=row.get("duration"),
            published_at=published_at,
            view_count=_to_int_or_none(row.get("view_count")),
            like_count=_to_int_or_none(row.get("like_count")),
            comment_count=_to_int_or_none(row.get("comment_count")),
        )

    @staticmethod
    def _views_per_hour_raw(row: FeatureInputRow, now: datetime) -> float:
        """Raw views/hour value."""

        views = float(row.view_count or 0)
        hours = max(_hours_since(row.published_at, now), 1.0)
        return views / hours

    @staticmethod
    def _comment_velocity_raw(row: FeatureInputRow, now: datetime) -> float:
        """Raw comments/hour value."""

        comments = float(row.comment_count or 0)
        hours = max(_hours_since(row.published_at, now), 1.0)
        return comments / hours


def _normalize_text(title: str, description: str | None) -> str:
    """Normalize text for deterministic matching/clustering."""

    merged = f"{title} {description or ''}".lower()
    stripped = _TEXT_CLEAN_RE.sub(" ", merged)
    return " ".join(stripped.split())


def _hours_since(published_at: datetime, now: datetime) -> float:
    """Hours since publish (minimum 0)."""

    delta = now - published_at.astimezone(timezone.utc)
    return max(delta.total_seconds() / 3600.0, 0.0)


def _engagement_rate_raw(row: FeatureInputRow) -> float:
    """(likes + comments) / max(views, 1)."""

    likes = float(row.like_count or 0)
    comments = float(row.comment_count or 0)
    views = max(float(row.view_count or 0), 1.0)
    return (likes + comments) / views


def _keyword_score(text: str, terms: list[str]) -> tuple[float, list[str]]:
    """Simple keyword hit ratio and matched term list."""

    if not terms:
        return 0.0, []

    matched = [term for term in terms if term and term.lower() in text]
    score = len(matched) / len(terms)
    return min(score, 1.0), matched


def _parse_duration_seconds(duration_iso8601: str | None) -> int | None:
    """Parse YouTube ISO8601 duration like PT1H2M3S to seconds."""

    if not duration_iso8601 or not duration_iso8601.startswith("PT"):
        return None

    total = 0
    for value, unit in _DURATION_PART_RE.findall(duration_iso8601):
        qty = int(value)
        if unit == "H":
            total += qty * 3600
        elif unit == "M":
            total += qty * 60
        elif unit == "S":
            total += qty
    return total


def _is_short_guess(duration_iso8601: str | None) -> bool:
    """Heuristic short guess from duration metadata (<=60s)."""

    seconds = _parse_duration_seconds(duration_iso8601)
    return bool(seconds is not None and seconds <= 60)


def _trend_score(
    *,
    views_per_hour: float,
    engagement_rate: float,
    comment_velocity: float,
    keyword_score: float,
    risk_score: float,
    weights: FeatureConfig,
) -> float:
    """Weighted trend score with a small risk penalty."""

    w = weights.weights
    raw_score = (
        (views_per_hour * w.views_per_hour)
        + (engagement_rate * w.engagement_rate)
        + (comment_velocity * w.comment_velocity)
        + (keyword_score * w.keyword_score)
        - (risk_score * w.risk_penalty)
    )
    return max(raw_score, 0.0)


def _bounded_ratio(value: float, upper_bound: float) -> float:
    """Bound a ratio to [0, 1] for explainable normalization."""

    safe_upper = max(upper_bound, 1e-9)
    return min(max(value / safe_upper, 0.0), 1.0)


def _to_int_or_none(value: Any) -> int | None:
    """Convert number-like values to int when possible."""

    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
