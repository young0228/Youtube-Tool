"""Project CLI entrypoint for YouTube Topic Radar MVP."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from ytradar.clustering.topic_cluster import DeterministicTopicClusterer
from ytradar.collectors.youtube_collector import CollectorSettings, YouTubeMetadataCollector
from ytradar.config.channels import load_channels_config
from ytradar.config.clustering import load_clustering_config
from ytradar.config.features import load_feature_config
from ytradar.db import initialize_database
from ytradar.db.repository import DuckDBRepository
from ytradar.features.engineer import FeatureEngineer
from ytradar.reporting.topic_report import TopicReportService


def _db_path() -> Path:
    return Path(os.getenv("YTRADAR_DB_PATH", "data/radar.duckdb"))


def _channels_config_path() -> Path:
    return Path(os.getenv("YTRADAR_CHANNELS_CONFIG", "configs/channels.yaml"))


def _features_config_path() -> Path:
    return Path(os.getenv("YTRADAR_FEATURES_CONFIG", "configs/features.yaml"))


def _clustering_config_path() -> Path:
    return Path(os.getenv("YTRADAR_CLUSTERING_CONFIG", "configs/clustering.yaml"))


def init_db_command(_: argparse.Namespace) -> None:
    """Initialize DuckDB schema."""

    path = initialize_database(_db_path())
    print(f"Initialized database: {path}")


def sync_channels_command(_: argparse.Namespace) -> None:
    """Upsert configured channels into `channels` table."""

    initialize_database(_db_path())
    repository = DuckDBRepository(_db_path())

    channels = load_channels_config(_channels_config_path())
    rows = [(c.channel_id, c.channel_key, c.display_name, c.group) for c in channels]
    repository.upsert_channels(rows)
    print(f"Synced channels: {len(rows)}")


def collect_videos_command(args: argparse.Namespace) -> None:
    """Fetch metadata for recent videos and upsert into `videos_raw`."""

    api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Missing YOUTUBE_API_KEY environment variable")

    initialize_database(_db_path())

    channels = load_channels_config(_channels_config_path())
    if not channels:
        print("No active channels configured. Nothing to collect.")
        return

    repository = DuckDBRepository(_db_path())
    repository.upsert_channels([(c.channel_id, c.channel_key, c.display_name, c.group) for c in channels])

    collector = YouTubeMetadataCollector(
        api_key=api_key,
        settings=CollectorSettings(lookback_days=args.days),
    )
    videos = collector.collect_recent_videos(channels)
    repository.upsert_videos_raw(videos)
    print(f"Collected videos: {len(videos)}")


def compute_features_command(_: argparse.Namespace) -> None:
    """Compute deterministic features from videos_raw."""

    initialize_database(_db_path())
    repository = DuckDBRepository(_db_path())

    rows = repository.fetch_videos_raw()
    if not rows:
        print("No videos_raw rows found. Run collect-videos first.")
        return

    config = load_feature_config(_features_config_path())
    features = FeatureEngineer(config).build_features(rows)
    repository.upsert_video_features(features)
    print(f"Computed features: {len(features)}")


def build_topics_command(_: argparse.Namespace) -> None:
    """Build deterministic topic candidates from engineered features."""

    initialize_database(_db_path())
    repository = DuckDBRepository(_db_path())

    rows = repository.fetch_rows_for_clustering()
    if not rows:
        print("No feature rows found. Run compute-features first.")
        return

    config = load_clustering_config(_clustering_config_path())
    candidates = DeterministicTopicClusterer(config).build_candidates(rows)
    repository.upsert_topic_candidates(candidates)
    print(f"Built topics: {len(candidates)}")


def export_report_command(args: argparse.Namespace) -> None:
    """Export report from topic_candidates to console/CSV/optional markdown."""

    repository = DuckDBRepository(_db_path())
    report_service = TopicReportService(repository)

    rows = report_service.load_report_rows()
    if not rows:
        print("No topic_candidates rows found. Run build-topics first.")
        return

    print(report_service.render_console_report(rows, top_n=args.top_n))
    csv_path = report_service.export_csv(rows, args.csv_path)
    print(f"CSV export: {csv_path}")

    if args.md_path:
        md_path = report_service.export_markdown(rows, args.md_path, top_n=args.top_n)
        print(f"Markdown export: {md_path}")


def run_all_command(args: argparse.Namespace) -> None:
    """Run pipeline end-to-end in recommended order."""

    init_db_command(args)
    sync_channels_command(args)
    collect_videos_command(args)
    compute_features_command(args)
    build_topics_command(args)
    export_report_command(args)


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser with minimal command set."""

    parser = argparse.ArgumentParser(description="YouTube Topic Radar CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_db = subparsers.add_parser("init-db", help="Initialize local DuckDB schema")
    init_db.set_defaults(func=init_db_command)

    sync = subparsers.add_parser("sync-channels", help="Sync configured channels into database")
    sync.set_defaults(func=sync_channels_command)

    collect = subparsers.add_parser("collect-videos", help="Collect recent video metadata only")
    collect.add_argument("--days", type=int, default=7, help="Lookback window in days")
    collect.set_defaults(func=collect_videos_command)

    compute = subparsers.add_parser("compute-features", help="Compute deterministic video features")
    compute.set_defaults(func=compute_features_command)

    topics = subparsers.add_parser("build-topics", help="Build deterministic topic clusters")
    topics.set_defaults(func=build_topics_command)

    report = subparsers.add_parser("export-report", help="Print ranked report and export files")
    report.add_argument("--top-n", type=int, default=20, help="Top candidates per section")
    report.add_argument(
        "--csv-path",
        default="data/exports/topic_candidates.csv",
        help="CSV output path",
    )
    report.add_argument(
        "--md-path",
        default="",
        help="Optional markdown output path",
    )
    report.set_defaults(func=export_report_command)

    run_all = subparsers.add_parser("run-all", help="Run entire pipeline end-to-end")
    run_all.add_argument("--days", type=int, default=7, help="Lookback window in days")
    run_all.add_argument("--top-n", type=int, default=20, help="Top candidates per section")
    run_all.add_argument(
        "--csv-path",
        default="data/exports/topic_candidates.csv",
        help="CSV output path",
    )
    run_all.add_argument(
        "--md-path",
        default="",
        help="Optional markdown output path",
    )
    run_all.set_defaults(func=run_all_command)

    return parser


def main() -> None:
    """CLI main entrypoint."""

    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
