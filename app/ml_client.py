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


def is_write_enabled() -> bool:
    """
    ¿Está habilitado el sync de escritura a ML?
    Por default NO — se activa explícitamente con la env var
    ML_SYNC_WRITE_ENABLED=true en Render.

    Mientras esté en false, ningún PUT sale a ML aunque el código exista.
    """
    return (
        is_configured()
        and (os.environ.get("ML_SYNC_WRITE_ENABLED") or "").strip().lower()
        in ("true", "1", "yes", "on")
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


# Cache de categorías (clave = category_id ML, valor = dict con name, etc.)
# El proceso es de un solo worker, así que un dict en memoria sirve y vacía
# en cada redeploy (cosa OK, las categorías de ML no cambian seguido).
_category_cache: dict = {}


def get_category(db: Session, category_id: str) -> dict:
    """
    Trae datos de una categoría de ML por ID. Devuelve {} si no se encuentra
    o si hay algún error (no levanta — esto es para enriquecer placeholders,
    no para flujos críticos).
    """
    if not category_id:
        return {}
    if category_id in _category_cache:
        return _category_cache[category_id]
    try:
        info = _get(db, f"/categories/{category_id}")
    except MLClientError:
        info = {}
    _category_cache[category_id] = info
    return info


def get_listing_prices(
    db: Session,
    *,
    price: float,
    category_id: str,
    listing_type_id: str = "gold_special",
    site_id: str = "MLA",
) -> dict:
    """
    Trae el detalle de fees para una publicación dada price + categoría + tipo.
    Endpoint público de ML: /sites/{SITE}/listing_prices

    Respuesta incluye `sale_fee_amount` (la comisión que ML cobra), que dividido
    por price da la comisión efectiva en %.

    Devuelve {} si falla — no levanta para no romper el sync.
    """
    if not category_id or price is None or price <= 0:
        return {}
    try:
        return _get(
            db,
            f"/sites/{site_id}/listing_prices",
            params={
                "price": str(price),
                "category_id": category_id,
                "listing_type_id": listing_type_id,
            },
        )
    except MLClientError:
        return {}


# =============================================================
# Escrituras (write) — gateadas por is_write_enabled()
# =============================================================

def _put(db: Session, path: str, payload: dict) -> dict:
    """
    PUT autenticado. Maneja 401 con un retry tras forzar refresh del token.
    NO chequea is_write_enabled aquí — eso es responsabilidad del caller
    (las funciones públicas update_item_*).
    """
    token = get_access_token(db)
    url = f"{ML_API_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.put(url, headers=headers, json=payload, timeout=20)
    except requests.RequestException as e:
        raise MLClientError(f"Error de red en PUT {path}: {e}") from e

    if response.status_code == 401:
        _access_token_cache["expires_at"] = 0
        token = get_access_token(db)
        try:
            response = requests.put(
                url,
                headers={**headers, "Authorization": f"Bearer {token}"},
                json=payload,
                timeout=20,
            )
        except requests.RequestException as e:
            raise MLClientError(f"Error de red en PUT {path} (retry): {e}") from e

    if not response.ok:
        # ML suele devolver JSON con `message` y `cause` describiendo el problema.
        # Tomamos los primeros 300 chars del body para no inundar el flash.
        raise MLClientError(
            f"ML PUT {path} → {response.status_code}: {response.text[:300]}"
        )

    return response.json()


def update_item_stock(db: Session, item_id: str, available_quantity: int) -> dict:
    """
    PUT a /items/{id} con available_quantity nuevo.
    El caller debe haber chequeado is_write_enabled() antes de llamar.
    """
    return _put(db, f"/items/{item_id}", {"available_quantity": int(available_quantity)})


def update_item_price(db: Session, item_id: str, price) -> dict:
    """
    PUT a /items/{id} con price nuevo.
    Convertimos a float porque ML espera number, no Decimal.
    """
    return _put(db, f"/items/{item_id}", {"price": float(price)})


# =============================================================
# Descripción (endpoint separado)
# =============================================================

def get_item_description(db: Session, item_id: str) -> dict:
    """
    Trae la descripción del item. ML la expone en un endpoint separado
    de /items/{id} — devuelve {plain_text, last_updated, ...}.
    Devuelve {} si falla — no levanta para que el sync no se corte.
    """
    if not item_id:
        return {}
    try:
        return _get(db, f"/items/{item_id}/description")
    except MLClientError:
        return {}


def update_item_description(db: Session, item_id: str, plain_text: str) -> dict:
    """
    PUT a /items/{id}/description con texto nuevo.
    El caller debe haber chequeado is_write_enabled() antes.
    """
    return _put(db, f"/items/{item_id}/description", {"plain_text": plain_text})


def update_item_attributes(db: Session, item_id: str, attributes: list) -> dict:
    """
    PUT /items/{id} con un array de atributos parciales (solo los que cambian).
    Cada elemento debe ser {id: ..., value_name|value_id|value_struct: ...}.
    """
    return _put(db, f"/items/{item_id}", {"attributes": attributes})


# =============================================================
# Compatibilidades (vehículos compatibles)
# =============================================================

def get_item_compatibilities(db: Session, item_id: str) -> list:
    """
    GET /items/{id}/compatibilities → lista de compatibilidades vehiculares.
    Cada compat tiene: id (de ML), domain_id, attributes (con VEHICLE_BRAND,
    VEHICLE_MODEL, VEHICLE_YEAR, etc.).
    Devuelve [] si falla — no levanta para no cortar el sync.
    """
    if not item_id:
        return []
    try:
        resp = _get(db, f"/items/{item_id}/compatibilities")
    except MLClientError:
        return []
    # ML puede devolver {results: [...]} o el array directo según endpoint
    if isinstance(resp, dict):
        return list(resp.get("results") or [])
    if isinstance(resp, list):
        return resp
    return []
