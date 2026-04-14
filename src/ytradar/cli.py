"""CLI entry points for YouTube Topic Radar MVP."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from ytradar.clustering.topic_cluster import DeterministicTopicClusterer
from ytradar.config.channels import load_channels_config
from ytradar.config.clustering import load_clustering_config
from ytradar.config.features import load_feature_config
from ytradar.db import initialize_database
from ytradar.db.repository import DuckDBRepository
from ytradar.features.engineer import FeatureEngineer


def collect_metadata_command(args: argparse.Namespace) -> None:
    """Run metadata collection and save rows into `videos_raw`."""
    from ytradar.collectors.youtube_collector import (
        CollectorSettings,
        YouTubeMetadataCollector,
    )

    api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Missing YOUTUBE_API_KEY environment variable")

    db_path = Path(os.getenv("YTRADAR_DB_PATH", "data/radar.duckdb"))
    channels_config = Path(os.getenv("YTRADAR_CHANNELS_CONFIG", "configs/channels.yaml"))

    initialize_database(db_path)

    channels = load_channels_config(channels_config)
    if not channels:
        print("No active channels configured. Nothing to fetch.")
        return

    collector = YouTubeMetadataCollector(
        api_key=api_key,
        settings=CollectorSettings(lookback_days=args.days),
    )
    repository = DuckDBRepository(db_path)

    repository.upsert_channels(
        [
            (c.channel_id, c.channel_key, c.display_name, c.group)
            for c in channels
        ]
    )

    videos = collector.collect_recent_videos(channels)
    repository.upsert_videos_raw(videos)

    print(f"Fetched and stored {len(videos)} video metadata rows.")


def engineer_features_command(args: argparse.Namespace) -> None:
    """Compute deterministic features from videos_raw into video_features."""

    db_path = Path(os.getenv("YTRADAR_DB_PATH", "data/radar.duckdb"))
    features_config = Path(os.getenv("YTRADAR_FEATURES_CONFIG", "configs/features.yaml"))

    initialize_database(db_path)

    repository = DuckDBRepository(db_path)
    rows = repository.fetch_videos_raw()
    if not rows:
        print("No videos_raw rows found. Run collect-metadata first.")
        return

    config = load_feature_config(features_config)
    engineer = FeatureEngineer(config)
    features = engineer.build_features(rows)
    repository.upsert_video_features(features)
    print(f"Engineered and stored {len(features)} video feature rows.")


def cluster_topics_command(args: argparse.Namespace) -> None:
    """Build deterministic topic clusters and store topic candidates."""

    db_path = Path(os.getenv("YTRADAR_DB_PATH", "data/radar.duckdb"))
    clustering_config = Path(os.getenv("YTRADAR_CLUSTERING_CONFIG", "configs/clustering.yaml"))

    initialize_database(db_path)

    repository = DuckDBRepository(db_path)
    rows = repository.fetch_rows_for_clustering()
    if not rows:
        print("No rows available for clustering. Run engineer-features first.")
        return

    config = load_clustering_config(clustering_config)
    clusterer = DeterministicTopicClusterer(config)
    candidates = clusterer.build_candidates(rows)
    repository.upsert_topic_candidates(candidates)
    print(f"Clustered and stored {len(candidates)} topic candidates.")


def build_parser() -> argparse.ArgumentParser:
    """Build the root parser."""

    parser = argparse.ArgumentParser(description="YouTube Topic Radar CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser(
        "collect-metadata",
        help="Fetch recent YouTube metadata for configured channels",
    )
    collect_parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Lookback window in days for recent videos",
    )
    collect_parser.set_defaults(func=collect_metadata_command)

    features_parser = subparsers.add_parser(
        "engineer-features",
        help="Compute deterministic features from videos_raw into video_features",
    )
    features_parser.set_defaults(func=engineer_features_command)

    cluster_parser = subparsers.add_parser(
        "cluster-topics",
        help="Cluster videos into deterministic topic candidates",
    )
    cluster_parser.set_defaults(func=cluster_topics_command)

    return parser


def main() -> None:
    """CLI main."""

    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
