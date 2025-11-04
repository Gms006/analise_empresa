from __future__ import annotations

# carregar .env da raiz do projeto
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=True)

# logger robusto
try:
    from scripts.utils.logger import get_logger

    logger = get_logger("fetch_api")
except Exception:  # pragma: no cover - fallback only during bootstrap
    import logging
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logger = logging.getLogger("fetch_api")

import json
from datetime import datetime, timezone

from scripts.acessorias_client import AcessoriasClient

CONFIG_PATH = Path(__file__).resolve().parents[1] / "scripts" / "config.json"
DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def _load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Configuração não encontrada: {CONFIG_PATH}")

    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _write_output(payload: dict) -> None:
    _ensure_data_dir()
    output_path = DATA_DIR / "api_processes.json"
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    logger.info("Resposta salva em %s", output_path)


def main() -> None:
    config = _load_config()
    acessorias_cfg = config.get("acessorias", {})
    base_url = (acessorias_cfg.get("base_url") or "https://api.acessorias.com").rstrip("/")
    page_size = int(acessorias_cfg.get("page_size") or 20)
    statuses = acessorias_cfg.get("statuses") or []

    client = AcessoriasClient(base_url, page_size=page_size)
    now_utc = datetime.now(timezone.utc)
    timestamp = now_utc.isoformat().replace("+00:00", "Z")
    logger.info("Iniciando coleta às %s", timestamp)

    payload = client.list_processes(statuses=statuses)
    _write_output({
        "fetched_at": timestamp,
        "payload": payload,
    })


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - logging guard
        logger.error("fetch_api falhou: %s", exc, exc_info=True)
        raise
