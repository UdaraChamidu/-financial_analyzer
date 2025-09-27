"""Configuration loader."""
from pathlib import Path
from typing import Any, Dict
import yaml

def load_config(path: str | Path = "config.yaml") -> Dict[str, Any]:
    """Load YAML config file.

    Args:
        path: Path to YAML config.

    Returns:
        Configuration dictionary.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {p}")
    with p.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)
