"""Reporting helpers for topic candidate editorial review."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

from ytradar.db.repository import DuckDBRepository


@dataclass(slots=True)
class ReportRow:
    """Denormalized report row for one topic candidate."""

    topic_cluster_id: str
    representative_label: str
    date_kst: str
    source_channels: str
    source_video_count: int
    source_channel_count: int
    average_trend_score: float
    top_video_id: str
    top_video_url: str
    recommended_format: str
    status: str


class TopicReportService:
    """Generate console/CSV/Markdown reports from topic candidates."""

    def __init__(self, repository: DuckDBRepository) -> None:
        self.repository = repository

    def load_report_rows(self) -> list[ReportRow]:
        """Load report rows with channel names resolved."""

        raw_rows = self.repository.fetch_topic_candidates_report_rows()
        channel_map = self.repository.fetch_channel_name_map()

        rows: list[ReportRow] = []
        for row in raw_rows:
            source_channels = _resolve_source_channels(
                row.get("source_channels_json"),
                channel_map,
            )
            rows.append(
                ReportRow(
                    topic_cluster_id=str(row["topic_cluster_id"]),
                    representative_label=str(row["representative_label"]),
                    date_kst=str(row["date_kst"]),
                    source_channels=source_channels,
                    source_video_count=int(row["source_video_count"]),
                    source_channel_count=int(row["source_channel_count"]),
                    average_trend_score=float(row["average_trend_score"]),
                    top_video_id=str(row["top_video_id"]),
                    top_video_url=str(row.get("top_video_url") or ""),
                    recommended_format=str(row["recommended_format"]),
                    status=str(row["status"]),
                )
            )

        return rows

    def render_console_report(self, rows: list[ReportRow], top_n: int = 20) -> str:
        """Render a ranked plain-text report for terminal output."""

        short_rows = [r for r in rows if r.recommended_format in ("shortform", "both")][:top_n]
        long_rows = [r for r in rows if r.recommended_format in ("longform", "both")][:top_n]

        lines: list[str] = []
        lines.append("=== SHORTFORM CANDIDATES ===")
        lines.extend(_format_ranked_rows(short_rows))
        lines.append("")
        lines.append("=== LONGFORM CANDIDATES ===")
        lines.extend(_format_ranked_rows(long_rows))
        return "\n".join(lines)

    def export_csv(self, rows: list[ReportRow], output_path: str | Path) -> Path:
        """Export all rows to CSV for spreadsheet/editorial workflows."""

        path = Path(output_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = [
            "topic_cluster_id",
            "representative_label",
            "date_kst",
            "source_channels",
            "source_video_count",
            "source_channel_count",
            "average_trend_score",
            "top_video_id",
            "top_video_url",
            "recommended_format",
            "status",
        ]

        with path.open("w", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row.__dict__)

        return path

    def export_markdown(self, rows: list[ReportRow], output_path: str | Path, top_n: int = 20) -> Path:
        """Export a compact markdown editorial report."""

        path = Path(output_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)

        short_rows = [r for r in rows if r.recommended_format in ("shortform", "both")][:top_n]
        long_rows = [r for r in rows if r.recommended_format in ("longform", "both")][:top_n]

        content = []
        content.append("# Topic Radar Report")
        content.append("")
        content.append("## Shortform Candidates")
        content.extend(_format_markdown_rows(short_rows))
        content.append("")
        content.append("## Longform Candidates")
        content.extend(_format_markdown_rows(long_rows))

        path.write_text("\n".join(content), encoding="utf-8")
        return path


def _resolve_source_channels(source_channels_json: str | None, channel_map: dict[str, str]) -> str:
    """Resolve source channel ids into display names for reporting."""

    if not source_channels_json:
        return ""

    try:
        channel_ids = json.loads(source_channels_json)
    except json.JSONDecodeError:
        return ""

    if not isinstance(channel_ids, list):
        return ""

    names = [channel_map.get(str(channel_id), str(channel_id)) for channel_id in channel_ids]
    return ", ".join(sorted(names))


def _format_ranked_rows(rows: list[ReportRow]) -> list[str]:
    """Format rows for console output."""

    if not rows:
        return ["(none)"]

    lines: list[str] = []
    for idx, row in enumerate(rows, start=1):
        lines.append(
            (
                f"{idx:02d}. [{row.representative_label}] "
                f"videos={row.source_video_count}, channels={row.source_channel_count}, "
                f"avg_trend={row.average_trend_score:.4f}, status={row.status}"
            )
        )
        lines.append(f"    channels: {row.source_channels}")
        lines.append(f"    top_video: {row.top_video_url or row.top_video_id}")
    return lines


def _format_markdown_rows(rows: list[ReportRow]) -> list[str]:
    """Format rows for markdown output."""

    if not rows:
        return ["- (none)"]

    lines: list[str] = []
    for row in rows:
        lines.append(
            (
                f"- **{row.representative_label}** "
                f"(videos={row.source_video_count}, channels={row.source_channel_count}, "
                f"avg_trend={row.average_trend_score:.4f}, status={row.status})"
            )
        )
        lines.append(f"  - source_channels: {row.source_channels}")
        lines.append(f"  - top_video: {row.top_video_url or row.top_video_id}")
    return lines
