"""
app/ml_client.py
================
Cliente de la API de Mercado Libre.

Solo lecturas por ahora — esta fase es read-only. Las escrituras (PUT a un item
para cambiar stock/precio) se activan en la próxima fase, gateadas por una env
var explícita.

Configuración:
  - ML_CLIENT_ID         (de tu app en developers.mercadolibre.com.ar)
  - ML_CLIENT_SECRET     (de tu app)
  - ML_REFRESH_TOKEN     (bootstrap inicial; después se rota y se persiste en DB)

Cómo funciona la auth:
  - ML usa OAuth2 con refresh_token rotativo
  - Cada llamada a /oauth/token devuelve un access_token nuevo Y un refresh_token nuevo
  - El refresh_token viejo queda invalidado
  - Por eso persistimos en la tabla `ml_tokens` (singleton row, id=1)
  - El access_token se cachea en memoria mientras no expira (~6 hs)

Funciones públicas:
  - is_configured()           → True si las 3 env vars están seteadas
  - get_access_token(db)      → devuelve un access token vigente (refresca si hace falta)
  - get_item(db, item_id)     → datos de un item de ML (price, stock, status, etc.)
  - get_user_info(db)         → /users/me — útil para verificar la auth
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import MLToken


ML_API_BASE = "https://api.mercadolibre.com"
ML_AUTH_URL = "https://api.mercadolibre.com/oauth/token"

# Cache simple en memoria del access_token. Funciona porque corremos con
# WEB_CONCURRENCY=1 en Render. Si escalamos a más workers, cada uno tiene
# su cache → más refrescos pero no rompe nada.
_access_token_cache: dict = {
    "token": None,
    "expires_at": 0,  # epoch seconds
}


class MLClientError(Exception):
    """Cualquier error hablando con ML — para que el caller pueda capturar limpio."""
    pass


# =============================================================
# Configuración
# =============================================================

def is_configured() -> bool:
    """¿Están las 3 env vars necesarias para auth?"""
    return all(
        os.environ.get(k)
        for k in ("ML_CLIENT_ID", "ML_CLIENT_SECRET", "ML_REFRESH_TOKEN")
    )


# =============================================================
# Manejo del refresh_token (DB con fallback a env)
# =============================================================

def _get_refresh_token(db: Session) -> str:
    """
    Lee el refresh_token desde la tabla ml_tokens (id=1).
    Si no hay row aún, fallback al env var (bootstrap inicial).
    """
    row = db.execute(select(MLToken).where(MLToken.id == 1)).scalar_one_or_none()
    if row and row.refresh_token:
        return row.refresh_token
    return (os.environ.get("ML_REFRESH_TOKEN") or "").strip()


def _save_refresh_token(db: Session, new_token: str) -> None:
    """Persiste el refresh_token nuevo (rotado) en DB."""
    row = db.execute(select(MLToken).where(MLToken.id == 1)).scalar_one_or_none()
    now = datetime.now(timezone.utc)
    if row:
        row.refresh_token = new_token
        row.last_refreshed_at = now
    else:
        db.add(MLToken(id=1, refresh_token=new_token, last_refreshed_at=now))
    db.commit()


# =============================================================
# Refresco del access_token
# =============================================================

def _refresh_access_token(db: Session) -> str:
    """
    Pide un access_token nuevo a ML usando el refresh_token actual.
    Persiste el refresh_token rotado y cachea el access_token nuevo.
    Devuelve el access_token.
    """
    client_id = os.environ.get("ML_CLIENT_ID")
    client_secret = os.environ.get("ML_CLIENT_SECRET")
    refresh_token = _get_refresh_token(db)

    if not (client_id and client_secret and refresh_token):
        raise MLClientError(
            "Faltan credenciales ML. Verificá ML_CLIENT_ID, ML_CLIENT_SECRET, ML_REFRESH_TOKEN."
        )

    try:
        response = requests.post(
            ML_AUTH_URL,
            data={
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
            },
            timeout=15,
        )
    except requests.RequestException as e:
        raise MLClientError(f"Error de red en auth ML: {e}") from e

    if not response.ok:
        # Si ML dice "invalid_grant" el refresh_token venció / fue revocado.
        # El usuario tiene que regenerar uno nuevo desde su sistema viejo.
        raise MLClientError(
            f"Auth ML falló ({response.status_code}): {response.text[:300]}"
        )

    data = response.json()
    new_access = data.get("access_token")
    new_refresh = data.get("refresh_token", refresh_token)
    expires_in = int(data.get("expires_in", 21600))

    if not new_access:
        raise MLClientError("Respuesta de auth ML no incluye access_token")

    # Persistimos el refresh_token (rotado o no — siempre escribimos para que
    # el bootstrap desde env quede capturado en DB y no dependamos más del env).
    _save_refresh_token(db, new_refresh)

    # Cacheamos el access_token con margen de 5 min antes de su expiración.
    _access_token_cache["token"] = new_access
    _access_token_cache["expires_at"] = time.time() + expires_in - 300

    return new_access


def get_access_token(db: Session) -> str:
    """Devuelve un access_token vigente. Refresca si hace falta."""
    cached = _access_token_cache["token"]
    expires = _access_token_cache["expires_at"]
    if cached and time.time() < expires:
        return cached
    return _refresh_access_token(db)


# =============================================================
# HTTP autenticado a la API
# =============================================================

def _get(db: Session, path: str, params: Optional[dict] = None) -> dict:
    """GET autenticado. Maneja 401 con un retry tras forzar refresh del token."""
    token = get_access_token(db)
    url = f"{ML_API_BASE}{path}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers, params=params or {}, timeout=20)
    except requests.RequestException as e:
        raise MLClientError(f"Error de red en GET {path}: {e}") from e

    if response.status_code == 401:
        # Token expirado/revocado mid-request: forzar refresh y reintentar una vez.
        _access_token_cache["expires_at"] = 0
        token = get_access_token(db)
        try:
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params=params or {},
                timeout=20,
            )
        except requests.RequestException as e:
            raise MLClientError(f"Error de red en GET {path} (retry): {e}") from e

    if response.status_code == 404:
        raise MLClientError(f"ML 404: el item/recurso '{path}' no existe en ML")

    if not response.ok:
        raise MLClientError(
            f"ML GET {path} → {response.status_code}: {response.text[:300]}"
        )

    return response.json()


# =============================================================
# Endpoints específicos
# =============================================================

def get_item(db: Session, item_id: str) -> dict:
    """
    Trae un item de ML por ID. Devuelve dict con todos los campos relevantes:
    price, available_quantity, status, permalink, title, etc.

    Lanza MLClientError si no existe (404) o si auth falla.
    """
    return _get(db, f"/items/{item_id}")


def get_user_info(db: Session) -> dict:
    """Datos del usuario autenticado — útil para verificar que la auth anda."""
    return _get(db, "/users/me")
