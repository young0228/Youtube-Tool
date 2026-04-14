"""Deterministic topic clustering from engineered video features."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ytradar.config.clustering import ClusteringConfig
from ytradar.db import TopicCandidateRecord

_TOKEN_RE = re.compile(r"[\w가-힣]+")


@dataclass(slots=True)
class ClusterInputRow:
    """Input row used by clustering logic."""

    video_id: str
    channel_id: str
    title: str
    normalized_text: str
    keywords: list[str]
    trend_score: float


@dataclass(slots=True)
class WorkingCluster:
    """In-memory cluster under construction."""

    rows: list[ClusterInputRow]
    token_counter: Counter[str]
    keyword_counter: Counter[str]


class DeterministicTopicClusterer:
    """Simple explainable topic clustering with overlap heuristics."""

    def __init__(self, config: ClusteringConfig) -> None:
        self.config = config

    def build_candidates(self, rows: list[dict]) -> list[TopicCandidateRecord]:
        """Cluster rows and return candidate records for persistence."""

        parsed = sorted(
            (self._parse_row(row) for row in rows),
            key=lambda r: (r.video_id, r.channel_id),
        )
        if not parsed:
            return []

        clusters: list[WorkingCluster] = []
        for row in parsed:
            placed = False
            for cluster in clusters:
                if self._belongs_to_cluster(row, cluster):
                    self._add_to_cluster(row, cluster)
                    placed = True
                    break
            if not placed:
                clusters.append(self._new_cluster(row))

        now_kst = datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat()
        generated_at = datetime.now(timezone.utc)

        candidates: list[TopicCandidateRecord] = []
        for idx, cluster in enumerate(clusters, start=1):
            source_video_count = len(cluster.rows)
            source_channel_count = len({r.channel_id for r in cluster.rows})
            avg_trend = sum(r.trend_score for r in cluster.rows) / max(source_video_count, 1)
            top_video = max(cluster.rows, key=lambda r: r.trend_score)

            representative_label = self._representative_label(cluster)
            cluster_id = f"{now_kst}-cluster-{idx:03d}"

            candidates.append(
                TopicCandidateRecord(
                    topic_cluster_id=cluster_id,
                    representative_label=representative_label,
                    date_kst=now_kst,
                    source_video_count=source_video_count,
                    source_channel_count=source_channel_count,
                    source_channels_json=json.dumps(
                        sorted({r.channel_id for r in cluster.rows}),
                        ensure_ascii=False,
                    ),
                    average_trend_score=avg_trend,
                    top_video_id=top_video.video_id,
                    recommended_format=_recommend_format(
                        source_video_count=source_video_count,
                        source_channel_count=source_channel_count,
                        average_trend_score=avg_trend,
                        min_cluster_size_for_longform=self.config.min_cluster_size_for_longform,
                        min_cluster_size_for_both=self.config.min_cluster_size_for_both,
                    ),
                    status="new",
                    generated_at=generated_at,
                )
            )

        return candidates

    def _parse_row(self, row: dict) -> ClusterInputRow:
        """Convert DB row to typed clustering row."""

        return ClusterInputRow(
            video_id=str(row["video_id"]),
            channel_id=str(row["channel_id"]),
            title=str(row.get("title") or ""),
            normalized_text=str(row.get("normalized_text") or ""),
            keywords=_parse_keywords_json(row.get("keywords_json")),
            trend_score=float(row.get("trend_score") or 0.0),
        )

    def _new_cluster(self, row: ClusterInputRow) -> WorkingCluster:
        """Create a new working cluster initialized with one row."""

        title_tokens = _extract_tokens(row.title, self.config.stopwords, self.config.min_token_length)
        keyword_tokens = [k.lower() for k in row.keywords]
        return WorkingCluster(
            rows=[row],
            token_counter=Counter(title_tokens),
            keyword_counter=Counter(keyword_tokens),
        )

    def _add_to_cluster(self, row: ClusterInputRow, cluster: WorkingCluster) -> None:
        """Append row to cluster and update aggregate token/keyword counters."""

        cluster.rows.append(row)
        cluster.token_counter.update(
            _extract_tokens(row.title, self.config.stopwords, self.config.min_token_length)
        )
        cluster.keyword_counter.update(k.lower() for k in row.keywords)

    def _belongs_to_cluster(self, row: ClusterInputRow, cluster: WorkingCluster) -> bool:
        """Check whether row should join cluster via overlap heuristics."""

        row_tokens = set(_extract_tokens(row.title, self.config.stopwords, self.config.min_token_length))
        cluster_tokens = set(cluster.token_counter.keys())
        title_jaccard = _jaccard(row_tokens, cluster_tokens)

        row_keywords = {k.lower() for k in row.keywords}
        cluster_keywords = set(cluster.keyword_counter.keys())
        keyword_overlap = _overlap_ratio(row_keywords, cluster_keywords)

        if keyword_overlap >= self.config.min_keyword_overlap:
            return True

        return title_jaccard >= self.config.min_title_jaccard

    def _representative_label(self, cluster: WorkingCluster) -> str:
        """Build human-readable label from common keywords/tokens."""

        top_keywords = [k for k, _ in cluster.keyword_counter.most_common(2)]
        if top_keywords:
            return " / ".join(top_keywords)

        top_tokens = [t for t, _ in cluster.token_counter.most_common(3)]
        if top_tokens:
            return " ".join(top_tokens)

        return "misc_topic"


def _extract_tokens(text: str, stopwords: list[str], min_len: int) -> list[str]:
    """Extract lowercase title tokens for simple matching."""

    stopword_set = {s.lower() for s in stopwords}
    tokens = [tok.lower() for tok in _TOKEN_RE.findall(text)]
    return [tok for tok in tokens if len(tok) >= min_len and tok not in stopword_set]


def _parse_keywords_json(value: str | None) -> list[str]:
    """Parse keyword list JSON safely."""

    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return [str(v) for v in parsed if isinstance(v, str)]


def _jaccard(a: set[str], b: set[str]) -> float:
    """Jaccard similarity for two token sets."""

    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _overlap_ratio(a: set[str], b: set[str]) -> float:
    """Directional overlap score using smaller set as denominator."""

    if not a or not b:
        return 0.0
    return len(a & b) / max(min(len(a), len(b)), 1)


def _recommend_format(
    *,
    source_video_count: int,
    source_channel_count: int,
    average_trend_score: float,
    min_cluster_size_for_longform: int,
    min_cluster_size_for_both: int,
) -> str:
    """Map cluster strength to shortform/longform recommendation."""

    if source_video_count >= min_cluster_size_for_both and source_channel_count >= 2:
        return "both"
    if source_video_count >= min_cluster_size_for_longform and average_trend_score >= 0.20:
        return "longform"
    return "shortform"
