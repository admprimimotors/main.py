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
    """
    GET autenticado. Reintenta 401 (refresh de token) y 429 (rate limit, con
    backoff usando Retry-After o exponencial). ML corta a ~100 req/min por app,
    así que sin esto las operaciones masivas fallan de a montones.
    """
    import time
    url = f"{ML_API_BASE}{path}"
    response = None
    for intento in range(4):
        token = get_access_token(db)
        try:
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params=params or {},
                timeout=20,
            )
        except requests.RequestException as e:
            raise MLClientError(f"Error de red en GET {path}: {e}") from e

        if response.status_code == 401 and intento < 3:
            # Token expirado/revocado mid-request: forzar refresh y reintentar.
            _access_token_cache["expires_at"] = 0
            continue
        if response.status_code == 429 and intento < 3:
            espera = int(response.headers.get("Retry-After") or (2 ** intento))
            time.sleep(min(max(espera, 1), 30))
            continue
        break

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
    import time
    url = f"{ML_API_BASE}{path}"
    response = None
    for intento in range(4):
        token = get_access_token(db)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        try:
            response = requests.put(url, headers=headers, json=payload, timeout=20)
        except requests.RequestException as e:
            raise MLClientError(f"Error de red en PUT {path}: {e}") from e

        if response.status_code == 401 and intento < 3:
            _access_token_cache["expires_at"] = 0
            continue
        if response.status_code == 429 and intento < 3:
            espera = int(response.headers.get("Retry-After") or (2 ** intento))
            time.sleep(min(max(espera, 1), 30))
            continue
        break

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


def update_item_status(db: Session, item_id: str, status: str) -> dict:
    """
    PUT /items/{id} con status nuevo.
    ML acepta: 'active', 'paused', 'closed' (closed es irreversible vía API).
    """
    return _put(db, f"/items/{item_id}", {"status": status})


def update_item_pictures(db: Session, item_id: str, picture_urls: list[str]) -> dict:
    """
    PUT /items/{id} con un array `pictures` nuevo. Cada URL se manda como
    {"source": url} y ML las descarga / vuelve a hostear. Reemplaza el set
    completo de fotos de la publicación.
    """
    pictures = [{"source": u} for u in picture_urls if u]
    return _put(db, f"/items/{item_id}", {"pictures": pictures})


def update_item_title(db: Session, item_id: str, title: str) -> dict:
    """
    PUT /items/{id} con title nuevo.

    Atención: para publicaciones en categorías con catálogo (ej: Camisas de
    Motor), ML rechaza este PUT — el título lo deriva del catálogo y no es
    editable. El caller debería detectar la categoría antes y skipear si es
    catálogo (o capturar el MLClientError y reportarlo como warning).
    """
    return _put(db, f"/items/{item_id}", {"title": (title or "").strip()[:60]})


def update_item_seller_sku(db: Session, item_id: str, sku: str) -> dict:
    """
    PUT /items/{id} con `seller_custom_field` = SKU del vendedor.

    Es el campo "SKU" de la publicación en ML (privado del vendedor). Mejora el
    matcheo con el catálogo y la organización interna. Solo metadata — NO toca
    precio ni stock. Aceptado universalmente (incluso en publicaciones de catálogo).
    """
    return _put(db, f"/items/{item_id}", {"seller_custom_field": str(sku).strip()[:64]})


def list_all_item_ids(db: Session) -> list:
    """
    Lista TODOS los item_ids del seller en ML (scan paginado). Read-only.
    Se usa para detectar publicaciones huérfanas (en ML pero no en la base local).
    """
    uid = get_user_id(db)
    if not uid:
        return []
    ids: list = []
    scroll = None
    while True:
        params = {"search_type": "scan", "limit": 100}
        if scroll:
            params["scroll_id"] = scroll
        r = _get(db, f"/users/{uid}/items/search", params) or {}
        batch = r.get("results") or []
        if not batch:
            break
        ids.extend(batch)
        scroll = r.get("scroll_id")
        if not scroll:
            break
    return ids


# =============================================================
# Compatibilidades (vehículos compatibles)
# =============================================================

def get_user_id(db: Session) -> Optional[str]:
    """
    Trae el seller ID (id de tu usuario en ML). Se usa para filtrar
    /orders/search por seller. Cacheamos en memoria.
    """
    if "_user_id" in _access_token_cache and _access_token_cache.get("_user_id"):
        return _access_token_cache["_user_id"]
    try:
        u = get_user_info(db)
        uid = str(u.get("id") or "") or None
        _access_token_cache["_user_id"] = uid
        return uid
    except Exception:
        return None


def search_orders(
    db: Session,
    *,
    seller_id: Optional[str] = None,
    date_from: Optional[str] = None,
    status: Optional[str] = None,
    offset: int = 0,
    limit: int = 50,
    sort: str = "date_desc",
) -> dict:
    """
    GET /orders/search?seller=ME → órdenes del vendedor autenticado.

    Filtros:
      - date_from: ISO 8601, ej "2026-05-01T00:00:00.000-00:00"
      - status: "paid", "confirmed", "cancelled", "invalid"
      - sort: "date_asc" o "date_desc"

    Devuelve el body de ML tal cual: {results: [...], paging: {total, offset, limit}}.
    """
    if not seller_id:
        seller_id = get_user_id(db) or "me"
    params: dict = {
        "seller": seller_id,
        "offset": str(offset),
        "limit": str(limit),
        "sort": sort,
    }
    if date_from:
        params["order.date_created.from"] = date_from
    if status:
        params["order.status"] = status
    try:
        return _get(db, "/orders/search", params=params) or {}
    except MLClientError:
        return {"results": [], "paging": {"total": 0}}


def add_item_compatibilities(
    db: Session,
    item_id: str,
    vehicle_ids: list[str],
) -> dict:
    """
    POST /items/{id}/compatibilities con un array de IDs de vehículos del
    catálogo de ML. Cada elemento del payload es {"id": "MLA-VEHICLE-..."}.

    Reemplaza la lista de compatibilidades del item (no agrega, sobrescribe).
    El caller debe haber chequeado is_write_enabled() antes.
    """
    if not item_id or not vehicle_ids:
        return {}
    payload = {
        "compatibilities": [{"id": vid} for vid in vehicle_ids if vid]
    }
    return _post(db, f"/items/{item_id}/compatibilities", payload)


# =============================================================
# Diagnóstico: visitas, preguntas, health, reputación del seller
# =============================================================

def get_item_visits(db: Session, item_id: str, *, last_days: int = 30) -> dict:
    """
    GET /items/{id}/visits/time_window — visitas en los últimos N días.
    Devuelve {total_visits, results:[{date, total}]} o {} si falla.
    """
    if not item_id:
        return {}
    try:
        return _get(
            db,
            f"/items/{item_id}/visits/time_window",
            params={"last": int(last_days), "unit": "day"},
        ) or {}
    except MLClientError:
        return {}


def get_item_health(db: Session, item_id: str) -> dict:
    """
    GET /items/{id}/health — score de calidad de la publicación y campos faltantes.
    Útil para ranking del algoritmo: items con `health < 0.7` están penalizados.
    """
    if not item_id:
        return {}
    try:
        return _get(db, f"/items/{item_id}/health") or {}
    except MLClientError:
        return {}


def get_questions_unanswered(db: Session, seller_id, *, limit: int = 50) -> dict:
    """
    GET /my/received_questions/search?status=UNANSWERED → preguntas sin responder.
    Las preguntas viejas hunden el ranking de la publicación.
    """
    try:
        return _get(
            db,
            "/my/received_questions/search",
            params={"status": "UNANSWERED", "limit": int(limit), "api_version": 4},
        ) or {}
    except MLClientError:
        return {}


def get_seller_reputation(db: Session) -> dict:
    """
    Trae info completa del seller (incluye seller_reputation con level_id,
    power_seller_status, transactions cancelled/claims, etc.).
    """
    try:
        return _get(db, "/users/me") or {}
    except MLClientError:
        return {}


def search_user_items_ids(
    db: Session,
    seller_id,
    *,
    limit: int = 50,
    offset: int = 0,
    status: Optional[str] = None,
) -> dict:
    """
    GET /users/{user_id}/items/search — lista IDs de items del seller.
    Para iterar el catálogo entero hay que paginar (max 1000 por scroll).
    """
    params = {"limit": int(limit), "offset": int(offset)}
    if status:
        params["status"] = status
    try:
        return _get(db, f"/users/{seller_id}/items/search", params=params) or {}
    except MLClientError:
        return {}


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


# =============================================================
# Creación de items (POST) — para publicar productos nuevos
# =============================================================

def _post(db: Session, path: str, payload: dict) -> dict:
    """
    POST autenticado a la API de ML. Mismo patrón de retry-on-401 que _put.
    NO chequea is_write_enabled aquí — es responsabilidad del caller.
    """
    token = get_access_token(db)
    url = f"{ML_API_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
    except requests.RequestException as e:
        raise MLClientError(f"Error de red en POST {path}: {e}") from e

    if response.status_code == 401:
        _access_token_cache["expires_at"] = 0
        token = get_access_token(db)
        try:
            response = requests.post(
                url,
                headers={**headers, "Authorization": f"Bearer {token}"},
                json=payload,
                timeout=30,
            )
        except requests.RequestException as e:
            raise MLClientError(f"Error de red en POST {path} (retry): {e}") from e

    if not response.ok:
        # ML devuelve detalles útiles en el body — los mostramos en el error
        # para que el caller pueda surface qué falta (atributo X requerido, etc.).
        raise MLClientError(
            f"ML POST {path} → {response.status_code}: {response.text[:500]}"
        )

    return response.json()


def predict_category(
    db: Session,
    title: str,
    *,
    site_id: str = "MLA",
    limit: int = 5,
) -> list[dict]:
    """
    Pregunta a ML qué categoría usar a partir del título del producto.
    Endpoint público: /sites/{SITE}/category_predictor/predict?title=...

    Devuelve lista de candidatos ordenados por probabilidad descendente:
      [{"category_id": "MLA1234", "category_name": "...",
        "prediction_probability": 0.95, "path_from_root": [...]}, ...]

    Lista vacía si falla — el caller decide cómo manejarlo.
    """
    if not (title or "").strip():
        return []
    try:
        resp = _get(
            db,
            f"/sites/{site_id}/category_predictor/predict",
            params={"title": title.strip(), "limit": str(limit)},
        )
    except MLClientError:
        return []
    # ML devuelve un dict con la categoría top y a veces incluye `alternatives`.
    # Normalizamos a lista de candidatos.
    if isinstance(resp, dict):
        candidates: list[dict] = []
        if resp.get("id"):
            candidates.append({
                "category_id": resp.get("id"),
                "category_name": resp.get("name", ""),
                "prediction_probability": resp.get("prediction_probability"),
                "path_from_root": resp.get("path_from_root", []),
            })
        for alt in resp.get("alternatives") or []:
            candidates.append({
                "category_id": alt.get("id"),
                "category_name": alt.get("name", ""),
                "prediction_probability": alt.get("prediction_probability"),
                "path_from_root": alt.get("path_from_root", []),
            })
        return candidates[:limit]
    if isinstance(resp, list):
        return [
            {
                "category_id": c.get("id"),
                "category_name": c.get("name", ""),
                "prediction_probability": c.get("prediction_probability"),
                "path_from_root": c.get("path_from_root", []),
            }
            for c in resp[:limit]
        ]
    return []


# Cache de atributos por categoría (clave = category_id, valor = lista de attrs).
# Las definiciones de categoría no cambian seguido — vacía en cada redeploy.
_category_attrs_cache: dict = {}


def get_category_attributes(db: Session, category_id: str) -> list[dict]:
    """
    GET /categories/{id}/attributes — devuelve la lista completa de atributos
    de la categoría, incluyendo cuáles son required (`tags.required` o
    `attribute_group_id == "OTHERS"` con tags).

    Cada attr tiene: id, name, value_type, tags{required, allow_variations,...},
    values (lista cerrada si aplica), allowed_units, etc.

    Lista vacía si falla.
    """
    if not category_id:
        return []
    if category_id in _category_attrs_cache:
        return _category_attrs_cache[category_id]
    try:
        resp = _get(db, f"/categories/{category_id}/attributes")
    except MLClientError:
        resp = []
    attrs = resp if isinstance(resp, list) else []
    _category_attrs_cache[category_id] = attrs
    return attrs


def create_item(db: Session, payload: dict) -> dict:
    """
    POST /items con el payload completo. Crea una publicación nueva en ML.
    Devuelve el dict del item recién creado (incluye `id` ML, `permalink`,
    `status`, etc.).

    El caller debe haber chequeado is_write_enabled() y armado el payload
    con todos los campos requeridos.
    """
    return _post(db, "/items", payload)


def domain_discovery_search(
    db: Session,
    query: str,
    *,
    site_id: str = "MLA",
    limit: int = 8,
) -> list[dict]:
    """
    Endpoint de "qué categoría es este producto" que ML usa internamente para
    sugerir mientras tipeás el título de una publicación nueva.

    GET /sites/{SITE}/domain_discovery/search?q=...&limit=N

    Devuelve lista normalizada:
      [{"category_id": "MLA1234", "category_name": "Camisas de Motor",
        "domain_id": "MLA-CYLINDER_LINERS", "domain_name": "...",
        "attributes": [...]}, ...]

    Lista vacía si falla. Es un endpoint público autenticado — usa _get.
    """
    if not (query or "").strip():
        return []
    try:
        resp = _get(
            db,
            f"/sites/{site_id}/domain_discovery/search",
            params={"q": query.strip(), "limit": str(limit)},
        )
    except MLClientError:
        return []
    if not isinstance(resp, list):
        return []
    out = []
    for r in resp:
        if not isinstance(r, dict):
            continue
        out.append({
            "category_id": r.get("category_id"),
            "category_name": r.get("category_name") or "",
            "domain_id": r.get("domain_id"),
            "domain_name": r.get("domain_name") or "",
        })
    return out


def search_items(
    db: Session,
    query: str,
    *,
    site_id: str = "MLA",
    limit: int = 10,
) -> list[dict]:
    """
    Búsqueda pública de items por keyword. La usamos como fallback al
    domain_discovery: si éste no devuelve nada, sacamos los category_id
    de los items que matchean la query y los aglomeramos por frecuencia.

    GET /sites/{SITE}/search?q=...&limit=N — endpoint público autenticado.

    Devuelve la lista raw de results (cada item con category_id, title, etc.).
    """
    if not (query or "").strip():
        return []
    try:
        resp = _get(
            db,
            f"/sites/{site_id}/search",
            params={"q": query.strip(), "limit": str(limit)},
        )
    except MLClientError:
        return []
    if isinstance(resp, dict):
        return list(resp.get("results") or [])
    return []
