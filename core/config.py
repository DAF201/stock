import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


class Config:
    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = data

    @property
    def openai_project_key(self) -> Optional[str]:
        return (
            os.getenv("OPENAI_API_KEY")
            or self._data.get("openai", {}).get("wave")
        )

    @property
    def alpaca(self) -> Dict[str, Any]:
        return self._data.get("ALPACA", {})

    @property
    def finnhub_key(self) -> Optional[str]:
        return os.getenv("FINNHUB_API_KEY") or self._data.get("FINNHUB", {}).get("api_key")

    @property
    def sec_user_agent(self) -> Optional[str]:
        return self._data.get("SEC", {}).get("user_agent")


def load_config(key_path: Optional[str] = None) -> Config:
    """Load configuration from `wave/key.json` by default, allowing overrides via env.

    Environment variables take precedence for sensitive keys.
    """
    if key_path is None:
        # Expect key.json at package root: wave/key.json
        key_path = str(Path(__file__).resolve().parent.parent / "key.json")
    try:
        with open(key_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}
    return Config(data)
