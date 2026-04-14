"""Feature/scoring config loader."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(slots=True)
class FeatureWeights:
    views_per_hour: float
    engagement_rate: float
    comment_velocity: float
    keyword_score: float
    risk_penalty: float


@dataclass(slots=True)
class FeatureConfig:
    """Config object for deterministic feature engineering."""

    keyword_terms: list[str]
    risk_terms: list[str]
    weights: FeatureWeights



def load_feature_config(path: str | Path) -> FeatureConfig:
    """Load feature engineering config from YAML."""

    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    weights = payload.get("weights", {})

    return FeatureConfig(
        keyword_terms=list(payload.get("keyword_terms", [])),
        risk_terms=list(payload.get("risk_terms", [])),
        weights=FeatureWeights(
            views_per_hour=float(weights.get("views_per_hour", 0.40)),
            engagement_rate=float(weights.get("engagement_rate", 0.30)),
            comment_velocity=float(weights.get("comment_velocity", 0.20)),
            keyword_score=float(weights.get("keyword_score", 0.15)),
            risk_penalty=float(weights.get("risk_penalty", 0.05)),
        ),
    )
