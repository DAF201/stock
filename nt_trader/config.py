from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict


def load_config(config_path: str | None) -> Dict[str, Any]:
    """Load JSON config if present; return {} otherwise.

    Falls back to ./config.json if path is None and file exists.
    """
    path: Path | None = None
    if config_path:
        path = Path(config_path)
    else:
        default = Path("config.json")
        if default.exists():
            path = default
    if not path or not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def merge_config_into_args(args, parser, cfg: Dict[str, Any]):
    """Apply config values to argparse namespace when the user did not pass an explicit CLI flag.

    Rule: For each known option dest, if the corresponding --flag (or an alias) is not found in argv,
    and config contains a value for that dest, assign it to args.
    """
    import sys

    argv = set(sys.argv[1:])
    # Build map from dest -> possible option strings
    mapping = {}
    for action in parser._actions:
        if not getattr(action, "option_strings", None):
            continue
        mapping[action.dest] = set(action.option_strings)

    for key, value in cfg.items():
        dest = key.replace("-", "_")
        if dest not in mapping:
            # Unknown key in config; ignore
            continue
        option_strings = mapping[dest]
        used = any(opt in argv for opt in option_strings)
        if not used:
            setattr(args, dest, value)

    return args

