from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from scripts.utils.logger import get_logger

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
logger = get_logger("build_events")


def _read_json(path: Path) -> Any:
    if not path.exists():
        logger.info("Arquivo %s não encontrado, prosseguindo.", path)
        return None
    if path.stat().st_size == 0:
        logger.info("Arquivo %s está vazio, prosseguindo.", path)
        return None
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def build_events() -> Dict[str, List[Dict[str, Any]]]:
    email_events = _read_json(DATA_DIR / "events_email.json") or {}
    deliveries = _read_json(DATA_DIR / "deliveries_raw.json") or {}
    processes = _read_json(DATA_DIR / "api_processes.json") or {}

    events: Dict[str, List[Dict[str, Any]]] = {
        "emails": email_events.get("messages", []),
        "deliveries": deliveries.get("payload", {}).get("items", []),
        "processes": processes.get("payload", {}).get("items", []),
    }
    return events


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    events = build_events()
    output_path = DATA_DIR / "events.json"
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(events, fh, ensure_ascii=False, indent=2)
    logger.info("Eventos consolidados em %s", output_path)


if __name__ == "__main__":
    main()
