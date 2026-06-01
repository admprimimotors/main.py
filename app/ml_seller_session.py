"""
app/ml_seller_session.py
========================
Scraper del endpoint `/api/seller-item-history/table/events` del seller hub
web de Mercado Libre.

Auth: cookies de sesión pegadas manualmente por el usuario desde devtools.
Se persisten en la tabla `ml_seller_cookies` (singleton). Cuando expiran,
el usuario las refresca via UI.

Por qué no playwright:
  Render Starter no permite instalar libs del sistema (libnss3, libatk, etc.)
  que requiere Chromium. La opción manual es más liviana y robusta — el user
  pega las cookies cada vez que expiran (típicamente unos pocos días).

Formatos aceptados (auto-detectados):
  1. JSON array: [{"name": "X", "value": "Y", "domain": ".mercadolibre.com.ar"}, ...]
     (formato del export de Chrome devtools → Application → Cookies → "Copy all as JSON")
  2. Cookie header raw: "name1=val1; name2=val2; ..."
     (formato del header "Cookie:" copiable desde devtools Network)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import httpx


SELLER_HUB_BASE = "https://www.mercadolibre.com.ar"
HISTORIAL_ENDPOINT = "/api/seller-item-history/table/events"

PERIOD_LAST_WEEK = "WITH_DATE_CLOSED_LAST_WEEK"
PERIOD_LAST_MONTH = "WITH_DATE_CLOSED_LAST_MONTH"


class SellerSessionError(Exception):
    """Error al obtener/usar la sesión del seller hub."""


# =============================================================
# Cookies storage (singleton ml_seller_cookies)
# =============================================================

def _get_cookies_row(db):
    """Devuelve la única fila de ml_seller_cookies (o None)."""
    from sqlalchemy import select as _select
    from .models import MLSellerCookies
    return db.execute(
        _select(MLSellerCookies).order_by(MLSellerCookies.id).limit(1)
    ).scalar_one_or_none()


def save_cookies(
    db,
    *,
    cookies_text: str,
    updated_by: Optional[str] = None,
) -> dict:
    """
    Guarda cookies pegadas por el usuario. Detecta formato automáticamente:
      - Si arranca con [ o { → JSON
      - Si tiene "name=value;" → raw cookie header

    Devuelve {"ok": bool, "n_cookies": int, "format": str, "error": str|None}.
    """
    from .models import MLSellerCookies

    text = (cookies_text or "").strip()
    if not text:
        return {"ok": False, "error": "Texto vacío", "n_cookies": 0, "format": None}

    cookies_json: Optional[list[dict]] = None
    cookies_raw: Optional[str] = None
    fmt = None

    if text.startswith("[") or text.startswith("{"):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                parsed = [parsed]
            if not isinstance(parsed, list):
                return {"ok": False, "error": "JSON no es un array de cookies", "n_cookies": 0, "format": None}
            cookies_json = parsed
            fmt = "json"
        except Exception as e:
            return {"ok": False, "error": f"JSON inválido: {e}", "n_cookies": 0, "format": None}
    else:
        # raw cookie header
        # ej: "name1=val1; name2=val2"
        cookies_raw = text
        fmt = "raw"

    n_cookies = (
        len(cookies_json) if cookies_json
        else len([c for c in (cookies_raw or "").split(";") if "=" in c])
    )

    if n_cookies == 0:
        return {"ok": False, "error": "No detecté ninguna cookie", "n_cookies": 0, "format": fmt}

    # Upsert singleton
    row = _get_cookies_row(db)
    if row is None:
        row = MLSellerCookies(
            cookies_json=cookies_json,
            cookies_raw=cookies_raw,
            updated_by=updated_by,
        )
        db.add(row)
    else:
        row.cookies_json = cookies_json
        row.cookies_raw = cookies_raw
        row.updated_by = updated_by
    db.commit()
    return {"ok": True, "n_cookies": n_cookies, "format": fmt, "error": None}


def _build_cookie_jar(db) -> httpx.Cookies:
    """Carga las cookies guardadas y arma el jar para httpx."""
    row = _get_cookies_row(db)
    if row is None:
        raise SellerSessionError(
            "No hay cookies de ML cargadas. Andá a /precios-historial y pegá las "
            "cookies de tu sesión web (botón ⚙ Configurar cookies)."
        )

    jar = httpx.Cookies()
    if row.cookies_json:
        for c in row.cookies_json:
            name = c.get("name")
            value = c.get("value")
            if not name or value is None:
                continue
            domain = c.get("domain") or ".mercadolibre.com.ar"
            path = c.get("path") or "/"
            jar.set(name, value, domain=domain, path=path)
    elif row.cookies_raw:
        for chunk in row.cookies_raw.split(";"):
            chunk = chunk.strip()
            if "=" not in chunk:
                continue
            name, _, value = chunk.partition("=")
            jar.set(name.strip(), value.strip(), domain=".mercadolibre.com.ar", path="/")
    else:
        raise SellerSessionError("La fila ml_seller_cookies existe pero está vacía.")

    return jar


def cookies_status(db) -> dict:
    """Info para mostrar en la UI: cuándo se cargaron las cookies."""
    row = _get_cookies_row(db)
    if row is None:
        return {"configured": False}
    n = 0
    if row.cookies_json:
        n = len(row.cookies_json)
    elif row.cookies_raw:
        n = len([c for c in row.cookies_raw.split(";") if "=" in c])
    return {
        "configured": True,
        "n_cookies": n,
        "updated_at": row.updated_at,
        "updated_by": row.updated_by,
        "format": "json" if row.cookies_json else "raw",
    }


# =============================================================
# Fetch del endpoint de historial
# =============================================================

_DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


def fetch_item_history_raw(
    db,
    ml_item_id: str,
    *,
    period: str = PERIOD_LAST_MONTH,
    page: int = 1,
) -> dict:
    """
    Llama al endpoint /api/seller-item-history/table/events con cookies guardadas.
    Devuelve el JSON crudo.
    """
    if not ml_item_id:
        raise SellerSessionError("ml_item_id requerido")

    jar = _build_cookie_jar(db)
    url = f"{SELLER_HUB_BASE}{HISTORIAL_ENDPOINT}"
    params = {"period": period, "page": page, "item_id": ml_item_id}
    headers = dict(_DEFAULT_HEADERS)
    headers["Referer"] = (
        f"{SELLER_HUB_BASE}/historial-de-modificaciones"
        f"?item_id={ml_item_id}"
    )

    with httpx.Client(cookies=jar, headers=headers, timeout=20.0, follow_redirects=True) as client:
        resp = client.get(url, params=params)
        if resp.status_code in (401, 403):
            raise SellerSessionError(
                f"Cookies expiradas o inválidas (status {resp.status_code}). "
                "Volvé a pegar las cookies frescas desde /precios-historial."
            )
        if resp.status_code != 200:
            raise SellerSessionError(
                f"Endpoint devolvió {resp.status_code}: {resp.text[:200]}"
            )

        # Detectar si el response es HTML (login page) en lugar de JSON
        ct = (resp.headers.get("content-type") or "").lower()
        if "application/json" not in ct and "text/html" in ct:
            raise SellerSessionError(
                "El endpoint devolvió HTML (probablemente página de login). "
                "Las cookies expiraron. Refrescalas desde la UI."
            )

        try:
            return resp.json()
        except Exception as e:
            raise SellerSessionError(f"Response no es JSON válido: {e}")


# =============================================================
# Parser: bricks JSON → eventos normalizados
# =============================================================

def _parse_money_brick(brick_data: dict) -> Optional[Decimal]:
    """
    Convierte una celda 'money' del JSON a Decimal.
    Formato: {"moneyAmount": {"value": {"cents": "89", "fraction": "84.591"}, ...}}
    fraction "84.591" = 84591 (puntos = miles en AR), cents = "89".
    Resultado: 84591.89
    """
    try:
        money = brick_data.get("moneyAmount") or {}
        value = money.get("value") or {}
        fraction = (value.get("fraction") or "").replace(".", "").replace(",", "")
        cents = value.get("cents") or "00"
        if not fraction:
            return None
        return Decimal(f"{fraction}.{cents}")
    except Exception:
        return None


def parse_events(json_data: dict) -> list[dict]:
    """Convierte respuesta `update_bricks` a lista de eventos normalizados."""
    bricks = (json_data.get("data") or {}).get("bricks") or []
    table = None
    for b in bricks:
        if b.get("id") == "table_item_history":
            table = b.get("data") or {}
            break
    if not table:
        return []

    rows = table.get("rows") or []
    events: list[dict] = []

    for row in rows:
        cols = row.get("columns") or []
        if len(cols) < 5:
            continue

        date_col = cols[0]
        date_iso = ((date_col.get("data") or {}).get("date") or "").strip()
        try:
            fecha_evento = datetime.fromisoformat(date_iso)
        except Exception:
            continue

        tipo = ((cols[1].get("data") or {}).get("label") or "").strip()

        antes_data = cols[2].get("data") or {}
        precio_antes = _parse_money_brick(antes_data)
        antes_raw = None
        if precio_antes is not None:
            antes_raw = f"${precio_antes}"
        elif (cols[2].get("data") or {}).get("label"):
            antes_raw = cols[2]["data"]["label"]

        desp_data = cols[3].get("data") or {}
        precio_despues = _parse_money_brick(desp_data)
        desp_raw = None
        if precio_despues is not None:
            desp_raw = f"${precio_despues}"
        elif (cols[3].get("data") or {}).get("label"):
            desp_raw = cols[3]["data"]["label"]

        delta_pct: Optional[Decimal] = None
        delta_signo: Optional[str] = None
        pct_obj = desp_data.get("percent")
        if pct_obj:
            label = (pct_obj.get("label") or "").replace("%", "").replace(",", ".").strip()
            try:
                delta_pct = Decimal(label)
                delta_signo = "up" if pct_obj.get("positive") else "down"
            except Exception:
                pass
        if delta_signo is None and precio_antes is not None and precio_despues is not None:
            if precio_despues > precio_antes:
                delta_signo = "up"
            elif precio_despues < precio_antes:
                delta_signo = "down"
            else:
                delta_signo = "flat"

        realizada = ((cols[4].get("data") or {}).get("label") or "").strip() or None

        events.append({
            "fecha_evento": fecha_evento,
            "tipo_modificacion": tipo,
            "valor_antes_raw": antes_raw,
            "valor_despues_raw": desp_raw,
            "precio_antes": precio_antes,
            "precio_despues": precio_despues,
            "delta_pct": delta_pct,
            "delta_signo": delta_signo,
            "realizada_desde": realizada,
            "raw_event": row,
        })

    return events


# =============================================================
# Wrapper de alto nivel: fetch + parse + persist
# =============================================================

def sync_item_history_to_db(
    db,
    ml_item_id: str,
    *,
    sku: Optional[str] = None,
    period: str = PERIOD_LAST_MONTH,
) -> dict:
    """
    Sincroniza histórico de un item a la tabla `ml_item_history`.
    Idempotente: UniqueConstraint (ml_item_id, fecha_evento, tipo_modificacion).
    """
    from sqlalchemy import select as _select
    from .models import MLItemHistory

    summary = {
        "ok": False, "n_eventos": 0, "n_nuevos": 0, "n_dups": 0, "error": None
    }

    try:
        raw = fetch_item_history_raw(db, ml_item_id, period=period)
        events = parse_events(raw)
        summary["n_eventos"] = len(events)
    except SellerSessionError as e:
        summary["error"] = str(e)
        return summary
    except Exception as e:
        summary["error"] = f"{type(e).__name__}: {e}"
        return summary

    for ev in events:
        existing = db.execute(
            _select(MLItemHistory.id).where(
                MLItemHistory.ml_item_id == ml_item_id,
                MLItemHistory.fecha_evento == ev["fecha_evento"],
                MLItemHistory.tipo_modificacion == ev["tipo_modificacion"],
            )
        ).first()
        if existing is not None:
            summary["n_dups"] += 1
            continue

        row = MLItemHistory(
            ml_item_id=ml_item_id,
            sku=sku,
            fecha_evento=ev["fecha_evento"],
            tipo_modificacion=ev["tipo_modificacion"][:60],
            valor_antes_raw=(ev["valor_antes_raw"] or "")[:200] or None,
            valor_despues_raw=(ev["valor_despues_raw"] or "")[:200] or None,
            precio_antes=ev["precio_antes"],
            precio_despues=ev["precio_despues"],
            delta_pct=ev["delta_pct"],
            delta_signo=ev["delta_signo"],
            realizada_desde=(ev["realizada_desde"] or "")[:120] or None,
            raw_event=ev["raw_event"],
        )
        db.add(row)
        summary["n_nuevos"] += 1

    try:
        db.commit()
        summary["ok"] = True
    except Exception as e:
        db.rollback()
        summary["error"] = f"commit: {type(e).__name__}: {e}"

    return summary


def sync_all_vinculados(
    db,
    *,
    period: str = PERIOD_LAST_MONTH,
    sleep_between: float = 0.3,
    max_items: int = 2000,
) -> dict:
    """Itera todas las publicaciones vinculadas y sincroniza historial de cada."""
    import time as _time
    from sqlalchemy import select as _select
    from .models import ProductoPublicacionML, Producto

    summary = {
        "total_items": 0, "ok": 0, "fail": 0,
        "n_eventos_total": 0, "n_nuevos_total": 0, "n_dups_total": 0,
        "errores": [],
    }

    # Validamos cookies primero (sin gastar 1k requests si están vencidas).
    try:
        _build_cookie_jar(db)
    except SellerSessionError as e:
        summary["errores"].append(f"cookies: {e}")
        return summary

    rows = db.execute(
        _select(ProductoPublicacionML.ml_item_id, Producto.sku)
        .join(Producto, Producto.id == ProductoPublicacionML.producto_id)
        .where(ProductoPublicacionML.ml_item_id.is_not(None))
        .limit(max_items)
    ).all()
    summary["total_items"] = len(rows)

    for ml_item_id, sku in rows:
        try:
            res = sync_item_history_to_db(db, ml_item_id, sku=sku, period=period)
            if res["ok"]:
                summary["ok"] += 1
                summary["n_eventos_total"] += res["n_eventos"]
                summary["n_nuevos_total"] += res["n_nuevos"]
                summary["n_dups_total"] += res["n_dups"]
            else:
                summary["fail"] += 1
                if len(summary["errores"]) < 10:
                    summary["errores"].append(f"{ml_item_id}: {res['error']}")
                # Si error es por cookies → abortar el batch entero
                if res["error"] and ("cookies" in res["error"].lower() or "401" in res["error"] or "403" in res["error"]):
                    summary["errores"].append("Abortado: cookies expiradas mid-batch.")
                    break
        except Exception as e:
            summary["fail"] += 1
            if len(summary["errores"]) < 10:
                summary["errores"].append(f"{ml_item_id}: {type(e).__name__}: {e}")
        _time.sleep(sleep_between)

    return summary
