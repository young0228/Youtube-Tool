"""Clustering config loader for deterministic topic grouping."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(slots=True)
class ClusteringConfig:
    """Config for deterministic token/keyword overlap clustering."""

    min_title_jaccard: float
    min_keyword_overlap: float
    min_token_length: int
    stopwords: list[str]
    min_cluster_size_for_longform: int
    min_cluster_size_for_both: int



def load_clustering_config(path: str | Path) -> ClusteringConfig:
    """Load clustering config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return ClusteringConfig(
        min_title_jaccard=float(payload.get("min_title_jaccard", 0.30)),
        min_keyword_overlap=float(payload.get("min_keyword_overlap", 0.50)),
        min_token_length=int(payload.get("min_token_length", 2)),
        stopwords=list(payload.get("stopwords", [])),
        min_cluster_size_for_longform=int(payload.get("min_cluster_size_for_longform", 3)),
        min_cluster_size_for_both=int(payload.get("min_cluster_size_for_both", 5)),
    )
