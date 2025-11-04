from __future__ import annotations

# carregar .env da raiz do projeto
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=True)

import json
from datetime import datetime, timezone

from scripts.acessorias_client import AcessoriasClient
from scripts.utils.logger import get_logger

CONFIG_PATH = Path(__file__).resolve().parents[1] / "scripts" / "config.json"
DATA_DIR = Path(__file__).resolve().parents[1] / "data"

logger = get_logger("fetch_companies")


def _load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _write_output(payload: dict) -> None:
    _ensure_data_dir()
    output_path = DATA_DIR / "companies.json"
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    logger.info("Empresas salvas em %s", output_path)


def main() -> None:
    config = _load_config()
    acessorias_cfg = config.get("acessorias", {})

    base_url = (acessorias_cfg.get("base_url") or "https://api.acessorias.com").rstrip("/")
    page_size = int(acessorias_cfg.get("page_size") or 20)

    client = AcessoriasClient(base_url, page_size=page_size)
    now_utc = datetime.now(timezone.utc)
    timestamp = now_utc.isoformat().replace("+00:00", "Z")
    logger.info("Consultando empresas às %s", timestamp)

    payload = client._request("companies/ListAll/", params={"Pagina": 1})
    _write_output({
        "fetched_at": timestamp,
        "payload": payload,
    })


if __name__ == "__main__":
    main()
