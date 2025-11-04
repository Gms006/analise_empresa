from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import requests
from dotenv import load_dotenv

from scripts.utils.logger import get_logger

# carregar .env da raiz do projeto
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=True)


class AcessoriasClient:
    """Small helper around the Acessórias API."""

    def __init__(self, base_url: str, *, page_size: int = 20) -> None:
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.session = requests.Session()
        self.logger = get_logger("acessorias_client")

    def _request(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        import os as _os

        token = (_os.getenv("ACESSORIAS_TOKEN") or "").strip()
        if not token:
            raise RuntimeError("ACESSORIAS_TOKEN ausente no .env (ou vazio).")

        url = f"{self.base_url}/{path.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        merged_params = {"Pagina": 1, "TamanhoPagina": self.page_size}
        if params:
            merged_params.update(params)

        self.logger.info("Requisitando %s", url)
        response = self.session.get(url, headers=headers, params=merged_params, timeout=60)
        if response.status_code in (401, 403):
            print("[ERROR] acessorias_client: auth error")
            raise RuntimeError(
                "Token inválido ou sem permissão na Acessórias (401/403). Verifique ACESSORIAS_TOKEN."
            )
        response.raise_for_status()
        return response.json()

    def list_processes(
        self,
        *,
        statuses: Optional[Iterable[str]] = None,
        page: int = 1,
    ) -> Dict[str, Any]:
        statuses_list = list(statuses) if statuses else []
        if statuses_list:
            endpoint = "processes/ListAll/"
            params: Dict[str, Any] = {"Pagina": page, "ProcStatus": ",".join(statuses_list)}
        else:
            endpoint = "processes/ListAll*/"
            params = {"Pagina": page}

        self.logger.info("Endpoint escolhido: %s", endpoint)
        return self._request(endpoint, params=params)

    def list_deliveries(self, identifier: str, *, page: int = 1) -> Dict[str, Any]:
        endpoint = f"processes/{identifier}/"
        params = {"Pagina": page}
        self.logger.info("Listando entregas: %s", endpoint)
        return self._request(endpoint, params=params)

