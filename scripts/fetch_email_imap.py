from __future__ import annotations

# carregar .env da raiz do projeto
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=True)

import imaplib
import json
import os
from datetime import datetime, timedelta, timezone
from email import message_from_bytes
from email.header import decode_header, make_header
from email.message import Message

from scripts.utils.logger import get_logger

CONFIG_PATH = Path(__file__).resolve().parents[1] / "scripts" / "config.json"
DATA_DIR = Path(__file__).resolve().parents[1] / "data"

logger = get_logger("fetch_email_imap")


def _load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _write_output(payload: dict) -> None:
    _ensure_data_dir()
    output_path = DATA_DIR / "events_email.json"
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    logger.info("Eventos de e-mail salvos em %s", output_path)


def _read_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"{name} ausente no .env (ou vazio).")
    return value


def get_text_message(msg: Message) -> str:
    parts: list[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/plain" and part.get_content_disposition() != "attachment":
                payload = part.get_payload(decode=True) or b""
                parts.append(payload.decode(part.get_content_charset() or "utf-8", errors="replace"))
    else:
        payload = msg.get_payload(decode=True) or b""
        parts.append(payload.decode(msg.get_content_charset() or "utf-8", errors="replace"))
    return "\n".join(part.strip() for part in parts if part)


def _decode_subject(msg: Message) -> str:
    header = msg.get("Subject", "")
    if not header:
        return ""
    decoded = make_header(decode_header(header))
    return str(decoded)


def main() -> None:
    config = _load_config()
    imap_cfg = config.get("imap", {})

    host = _read_env("IMAP_HOST")
    user = _read_env("IMAP_USER")
    password = _read_env("IMAP_PASSWORD")

    now_utc = datetime.now(timezone.utc)
    timestamp = now_utc.isoformat().replace("+00:00", "Z")
    search_days = int(imap_cfg.get("search_days") or 30)
    since_date = (now_utc - timedelta(days=search_days)).strftime("%d-%b-%Y")

    logger.info("Conectando ao IMAP %s como %s", host, user)
    with imaplib.IMAP4_SSL(host) as client:
        client.login(user, password)
        client.select("INBOX")
        status, data = client.search(None, "SINCE", since_date)
        if status != "OK":
            raise RuntimeError(f"Busca IMAP falhou: {status}")

        messages: list[dict] = []
        for num in data[0].split():
            status, msg_data = client.fetch(num, "(RFC822)")
            if status != "OK" or not msg_data:
                logger.warning("Falha ao buscar mensagem %s: %s", num, status)
                continue
            raw_email = msg_data[0][1]
            email_message = message_from_bytes(raw_email)
            assert isinstance(email_message, Message)
            messages.append(
                {
                    "uid": num.decode(),
                    "subject": _decode_subject(email_message),
                    "from": email_message.get("From"),
                    "to": email_message.get("To"),
                    "date": email_message.get("Date"),
                    "body": get_text_message(email_message),
                }
            )

    _write_output({
        "fetched_at": timestamp,
        "search_since": since_date,
        "messages": messages,
    })


if __name__ == "__main__":
    main()
