"""Channel configuration loader.

Keeps the monitored YouTube channel list outside of code so non-engineers can
edit channels without touching Python modules.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(slots=True)
class ChannelConfig:
    """One configured channel entry."""

    channel_id: str
    channel_key: str
    display_name: str
    group: str
    active: bool = True



def load_channels_config(path: str | Path) -> list[ChannelConfig]:
    """Load active channels from YAML config.

    Args:
        path: Path to the YAML file containing `channels` list.

    Returns:
        List of active channels.
    """

    config_path = Path(path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    channels_data = payload.get("channels", [])

    channels: list[ChannelConfig] = []
    for item in channels_data:
        channel = ChannelConfig(
            channel_id=str(item["youtube_channel_id"]),
            channel_key=str(item["channel_key"]),
            display_name=str(item["display_name"]),
            group=str(item.get("group", "unknown")),
            active=bool(item.get("active", True)),
        )
        if channel.active:
            channels.append(channel)

    return channels
