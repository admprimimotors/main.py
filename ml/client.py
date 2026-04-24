"""
ml/client.py
============
Cliente HTTP autenticado para la API de Mercado Libre.

Responsabilidades:
 - Obtener un access_token válido automáticamente (refresca si venció).
 - Reintentar automáticamente ante 401 (token expirado) y 429 (rate limit).
 - Loggear requests y errores de forma consistente.

Uso desde otros módulos:
    from ml import client
    data = client.get("/items/MLA123456789")
    data = client.get("/users/me")
"""

from __future__ import annotations

import time
from typing import Any, Optional

import requests

import config
from logger import get_logger
from ml.auth import obtener_token_valido

log = get_logger(__name__)

_BASE_URL = config.ML_API_BASE
_DEFAULT_TIMEOUT = 30  # segundos
_MAX_REINTENTOS = 3


def _request(
    method: str,
    path: str,
    *,
    params: Optional[dict] = None,
    data: Optional[dict] = None,
    json_body: Optional[dict] = None,
) -> Any:
    """
    Wrapper interno para todas las llamadas a la API de ML.

    - Agrega automáticamente el Authorization header con el token válido.
    - Reintenta ante 401 (renovando token) y 429 (rate limit con back-off).
    """
    url = path if path.startswith("http") else f"{_BASE_URL}{path}"

    for intento in range(1, _MAX_REINTENTOS + 1):
        token = obtener_token_valido()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        log.debug(f"{method} {url} intento={intento}")
        resp = requests.request(
            method,
            url,
            params=params,
            data=data,
            json=json_body,
            headers=headers,
            timeout=_DEFAULT_TIMEOUT,
        )

        # OK
        if 200 <= resp.status_code < 300:
            if resp.content:
                try:
                    return resp.json()
                except ValueError:
                    return resp.text
            return None

        # Token vencido — reintentar con token refrescado
        if resp.status_code == 401 and intento < _MAX_REINTENTOS:
            log.warning("401 recibido, refrescando token y reintentando")
            continue

        # Rate limit — esperar y reintentar
        if resp.status_code == 429 and intento < _MAX_REINTENTOS:
            espera = int(resp.headers.get("Retry-After", 5))
            log.warning(f"Rate limit (429). Esperando {espera}s antes de reintentar")
            time.sleep(espera)
            continue

        # Errores transitorios 5xx — reintento con back-off leve
        if 500 <= resp.status_code < 600 and intento < _MAX_REINTENTOS:
            espera = 2 ** intento
            log.warning(f"Error {resp.status_code}, esperando {espera}s")
            time.sleep(espera)
            continue

        # Error real
        log.error(f"Error {resp.status_code} en {method} {url}: {resp.text[:300]}")
        resp.raise_for_status()

    raise RuntimeError(f"Falló después de {_MAX_REINTENTOS} intentos: {method} {url}")


def get(path: str, *, params: Optional[dict] = None) -> Any:
    return _request("GET", path, params=params)


def post(path: str, *, json_body: Optional[dict] = None, params: Optional[dict] = None) -> Any:
    return _request("POST", path, params=params, json_body=json_body)


def put(path: str, *, json_body: Optional[dict] = None, params: Optional[dict] = None) -> Any:
    return _request("PUT", path, params=params, json_body=json_body)


def delete(path: str, *, params: Optional[dict] = None) -> Any:
    return _request("DELETE", path, params=params)


# ==========================================================
# Upload de archivos (fotos) — multipart/form-data
# ==========================================================
def post_multipart(path: str, files: dict, *, data: Optional[dict] = None) -> Any:
    """
    POST multipart con archivos (p.ej. /pictures/items/upload).

    Args:
        path   : endpoint ("/pictures/items/upload" o "/pictures")
        files  : dict con el formato de `requests`, p.ej.
                 {"file": ("nombre.png", open_bytes, "image/png")}
        data   : campos de texto adicionales opcionales.

    Devuelve el JSON de respuesta (dict con id de la foto).
    Reintenta 401 (token expirado), 429 (rate limit) y 5xx — como _request.
    """
    url = path if path.startswith("http") else f"{_BASE_URL}{path}"

    for intento in range(1, _MAX_REINTENTOS + 1):
        token = obtener_token_valido()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        log.debug(f"POST multipart {url} intento={intento}")
        resp = requests.post(
            url,
            headers=headers,
            files=files,
            data=data,
            timeout=_DEFAULT_TIMEOUT * 2,  # upload puede tardar más
        )

        if 200 <= resp.status_code < 300:
            if resp.content:
                try:
                    return resp.json()
                except ValueError:
                    return resp.text
            return None

        if resp.status_code == 401 and intento < _MAX_REINTENTOS:
            log.warning("401 en upload, refrescando token")
            continue
        if resp.status_code == 429 and intento < _MAX_REINTENTOS:
            espera = int(resp.headers.get("Retry-After", 5))
            log.warning(f"Rate limit (429) en upload. Esperando {espera}s")
            time.sleep(espera)
            continue
        if 500 <= resp.status_code < 600 and intento < _MAX_REINTENTOS:
            espera = 2 ** intento
            log.warning(f"Error {resp.status_code} en upload, esperando {espera}s")
            time.sleep(espera)
            continue

        log.error(f"Error {resp.status_code} en upload {url}: {resp.text[:300]}")
        resp.raise_for_status()

    raise RuntimeError(f"Falló upload después de {_MAX_REINTENTOS} intentos: {url}")
