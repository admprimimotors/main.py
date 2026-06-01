"""
app/ml_seller_session.py
========================
Login programado + scraper del endpoint `/api/seller-item-history/table/events`
del seller hub web de Mercado Libre.

Por qué este módulo:
  La API pública de ML NO expone el "Historial de modificaciones" (cambios de
  precio, stock, status) que sí está en el seller hub web. El endpoint vive
  en `https://www.mercadolibre.com.ar/api/seller-item-history/` y requiere
  cookies de sesión del usuario logueado en el seller hub — no el API token.

Cómo funciona:
  1. `_ensure_session()` chequea si hay cookies guardadas y vigentes.
  2. Si no las hay (primera vez o expiraron), usa Playwright con Chromium
     headless para abrir mercadolibre.com.ar/jms/mla/lgz/login, ingresar
     ML_SELLER_USER + ML_SELLER_PASSWORD, capturar cookies (incl. HttpOnly).
  3. Las cookies se persisten en /tmp/ml_seller_cookies.json + en tabla
     simple en DB (campo JSONB) para sobrevivir reinicios.
  4. `fetch_item_history(ml_item_id, period)` usa httpx con esas cookies
     para llamar al endpoint y devuelve eventos normalizados.

Credenciales:
  Se leen de env vars ML_SELLER_USER + ML_SELLER_PASSWORD. NO se loguean,
  NO se guardan en DB. Solo en memoria del proceso al momento del login.

Manejo de errores:
  - Si Playwright no está instalado o falla: se levanta SellerSessionError.
  - Si las cookies expiraron (401 del endpoint): re-login automático.
  - Si las credenciales son inválidas: SellerSessionError con mensaje claro.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

import httpx


COOKIES_FILE = Path("/tmp/ml_seller_cookies.json")
COOKIES_TTL_HOURS = 6  # Las cookies del seller hub duran ~horas. Re-login proactivo.

SELLER_HUB_BASE = "https://www.mercadolibre.com.ar"
HISTORIAL_ENDPOINT = "/api/seller-item-history/table/events"
LOGIN_URL = "https://www.mercadolibre.com.ar/jms/mla/lgz/login"

# Períodos válidos del endpoint (descubiertos via XHR inspection del seller hub)
PERIOD_LAST_WEEK = "WITH_DATE_CLOSED_LAST_WEEK"
PERIOD_LAST_MONTH = "WITH_DATE_CLOSED_LAST_MONTH"


class SellerSessionError(Exception):
    """Error al obtener/usar la sesión del seller hub."""


# =============================================================
# Cookies — persistencia + freshness
# =============================================================

def _load_cookies_from_disk() -> Optional[list[dict]]:
    """Carga cookies guardadas en disco si están vigentes."""
    if not COOKIES_FILE.exists():
        return None
    try:
        raw = json.loads(COOKIES_FILE.read_text())
        saved_at = raw.get("saved_at")
        if not saved_at:
            return None
        saved_dt = datetime.fromisoformat(saved_at)
        if datetime.now(timezone.utc) - saved_dt > timedelta(hours=COOKIES_TTL_HOURS):
            return None  # Expiradas
        return raw.get("cookies") or None
    except Exception as e:
        print(f"[ml_seller_session] error cargando cookies: {e}")
        return None


def _save_cookies_to_disk(cookies: list[dict]) -> None:
    """Persiste cookies + timestamp en /tmp."""
    try:
        COOKIES_FILE.write_text(json.dumps({
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "cookies": cookies,
        }))
    except Exception as e:
        print(f"[ml_seller_session] error guardando cookies: {e}")


def _cookies_to_jar(cookies: list[dict]) -> httpx.Cookies:
    """Convierte cookies de Playwright [{name, value, domain, path, ...}] a httpx.Cookies."""
    jar = httpx.Cookies()
    for c in cookies:
        name = c.get("name")
        value = c.get("value")
        domain = c.get("domain") or ".mercadolibre.com.ar"
        path = c.get("path") or "/"
        if not name or value is None:
            continue
        jar.set(name, value, domain=domain, path=path)
    return jar


# =============================================================
# Login con Playwright
# =============================================================

def _login_playwright() -> list[dict]:
    """
    Hace login programado en el seller hub usando Playwright + Chromium headless.
    Devuelve la lista de cookies (incluidas HttpOnly).
    """
    usuario = os.environ.get("ML_SELLER_USER", "").strip()
    password = os.environ.get("ML_SELLER_PASSWORD", "").strip()
    if not usuario or not password:
        raise SellerSessionError(
            "Faltan env vars ML_SELLER_USER y ML_SELLER_PASSWORD. "
            "Configurálas en el dashboard de Render."
        )

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise SellerSessionError(
            "Playwright no está instalado. Verificá que esté en requirements.txt "
            "y que el buildCommand de Render incluya `python -m playwright install chromium`."
        )

    cookies_capturadas: list[dict] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
            )
            page = ctx.new_page()
            page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)

            # Paso 1: usuario
            page.fill('input[name="user_id"]', usuario)
            page.click('button:has-text("Continuar")')

            # Paso 2: contraseña (puede tardar en aparecer)
            page.wait_for_selector('input[name="password"]', timeout=15000)
            page.fill('input[name="password"]', password)
            page.click('button[type="submit"]')

            # Esperamos a que redirija a una página interna del seller hub
            try:
                page.wait_for_url(
                    lambda url: "myaccount" in url
                    or "mercadolibre.com.ar/home" in url
                    or url == "https://www.mercadolibre.com.ar/"
                    or "publicaciones" in url,
                    timeout=20000,
                )
            except Exception:
                # Igual capturamos cookies — a veces el redirect tarda más
                pass

            time.sleep(2)
            cookies_capturadas = ctx.cookies()
            browser.close()
    except SellerSessionError:
        raise
    except Exception as e:
        raise SellerSessionError(f"Falló el login Playwright: {type(e).__name__}: {e}")

    # Sanity check: que esté la cookie de sesión del seller hub
    cookie_names = {c.get("name") for c in cookies_capturadas}
    has_session = any(
        n in cookie_names for n in ("orguserid", "orguseridp", "_d2id", "ssid")
    )
    if not has_session:
        raise SellerSessionError(
            "Login completado pero no se capturaron cookies de sesión "
            f"(cookies: {sorted(cookie_names)[:10]}). Revisar credenciales."
        )

    _save_cookies_to_disk(cookies_capturadas)
    return cookies_capturadas


def _ensure_session(force_relogin: bool = False) -> list[dict]:
    """Devuelve cookies vigentes. Hace login si no hay o expiraron."""
    if not force_relogin:
        cached = _load_cookies_from_disk()
        if cached:
            return cached
    return _login_playwright()


# =============================================================
# Fetch del endpoint de historial
# =============================================================

def fetch_item_history_raw(
    ml_item_id: str,
    *,
    period: str = PERIOD_LAST_MONTH,
    page: int = 1,
    cookies: Optional[list[dict]] = None,
) -> dict:
    """
    Llama al endpoint /api/seller-item-history/table/events con cookies de sesión.
    Devuelve el JSON crudo (estructura "update_bricks").
    """
    if not ml_item_id:
        raise SellerSessionError("ml_item_id requerido")

    if cookies is None:
        cookies = _ensure_session()

    jar = _cookies_to_jar(cookies)
    url = f"{SELLER_HUB_BASE}{HISTORIAL_ENDPOINT}"
    params = {"period": period, "page": page, "item_id": ml_item_id}
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
        "Referer": f"{SELLER_HUB_BASE}/historial-de-modificaciones?item_id={ml_item_id}",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    }

    with httpx.Client(cookies=jar, headers=headers, timeout=20.0, follow_redirects=True) as client:
        resp = client.get(url, params=params)
        # 401/403 → cookies expiradas → re-login una vez
        if resp.status_code in (401, 403):
            cookies = _ensure_session(force_relogin=True)
            jar = _cookies_to_jar(cookies)
            with httpx.Client(cookies=jar, headers=headers, timeout=20.0, follow_redirects=True) as c2:
                resp = c2.get(url, params=params)
        if resp.status_code != 200:
            raise SellerSessionError(
                f"Endpoint historial devolvió {resp.status_code}: {resp.text[:200]}"
            )
        return resp.json()


# =============================================================
# Parser: bricks JSON → eventos normalizados
# =============================================================

def _parse_money_brick(brick_data: dict) -> Optional[Decimal]:
    """
    Convierte una celda 'money' del JSON a Decimal.
    Formato: {"moneyAmount": {"value": {"cents": "89", "fraction": "84.591"}, ...}}
    fraction = "84.591" → 84591 (los puntos son separadores de miles en AR)
    cents = "89" → 0.89
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
    """
    Convierte la respuesta de `update_bricks` a lista de eventos normalizados.

    Cada evento tiene shape:
      {
        "fecha_evento": datetime,
        "tipo_modificacion": str,
        "valor_antes_raw": str|None,
        "valor_despues_raw": str|None,
        "precio_antes": Decimal|None,
        "precio_despues": Decimal|None,
        "delta_pct": Decimal|None,
        "delta_signo": "up"|"down"|"flat"|None,
        "realizada_desde": str|None,
        "raw_event": dict (original),
      }
    """
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

        # Col 0: fecha
        date_col = cols[0]
        date_iso = ((date_col.get("data") or {}).get("date") or "").strip()
        try:
            fecha_evento = datetime.fromisoformat(date_iso)
        except Exception:
            continue

        # Col 1: tipo de modificación
        tipo = ((cols[1].get("data") or {}).get("label") or "").strip()

        # Col 2: antes
        antes_data = cols[2].get("data") or {}
        precio_antes = _parse_money_brick(antes_data)
        antes_raw = None
        if precio_antes is not None:
            antes_raw = f"${precio_antes}"
        elif (cols[2].get("data") or {}).get("label"):
            antes_raw = cols[2]["data"]["label"]

        # Col 3: después + percent
        desp_data = cols[3].get("data") or {}
        precio_despues = _parse_money_brick(desp_data)
        desp_raw = None
        if precio_despues is not None:
            desp_raw = f"${precio_despues}"
        elif (cols[3].get("data") or {}).get("label"):
            desp_raw = cols[3]["data"]["label"]

        # Delta % del JSON
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

        # Col 4: realizada desde
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
    db,  # sqlalchemy Session — typed sin import para evitar circulares
    ml_item_id: str,
    *,
    sku: Optional[str] = None,
    period: str = PERIOD_LAST_MONTH,
) -> dict:
    """
    Sincroniza el histórico de un item ML a la tabla `ml_item_history`.
    Idempotente: usa UniqueConstraint (ml_item_id, fecha_evento, tipo_modificacion).

    Devuelve:
      {"ok": bool, "n_eventos": int, "n_nuevos": int, "n_dups": int, "error": str|None}
    """
    from sqlalchemy import select as _select
    from .models import MLItemHistory

    summary = {
        "ok": False, "n_eventos": 0, "n_nuevos": 0, "n_dups": 0, "error": None
    }

    try:
        raw = fetch_item_history_raw(ml_item_id, period=period)
        events = parse_events(raw)
        summary["n_eventos"] = len(events)
    except SellerSessionError as e:
        summary["error"] = str(e)
        return summary
    except Exception as e:
        summary["error"] = f"{type(e).__name__}: {e}"
        return summary

    for ev in events:
        # Check dup por (item_id, fecha_evento, tipo)
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
    """
    Itera todas las publicaciones ML vinculadas y sincroniza el historial de
    cada una. Usa una sola sesión de cookies para todas (no re-login por item).

    Devuelve resumen agregado.
    """
    from sqlalchemy import select as _select
    from .models import ProductoPublicacionML, Producto

    summary = {
        "total_items": 0,
        "ok": 0,
        "fail": 0,
        "n_eventos_total": 0,
        "n_nuevos_total": 0,
        "n_dups_total": 0,
        "errores": [],
    }

    # Pre-cargamos cookies (un solo login)
    try:
        cookies = _ensure_session()
    except SellerSessionError as e:
        summary["errores"].append(f"session: {e}")
        return summary

    # Pedimos pares (ml_item_id, sku) — sku puede ser None.
    rows = db.execute(
        _select(ProductoPublicacionML.ml_item_id, Producto.sku)
        .join(Producto, Producto.id == ProductoPublicacionML.producto_id)
        .where(ProductoPublicacionML.ml_item_id.is_not(None))
        .limit(max_items)
    ).all()
    summary["total_items"] = len(rows)

    for ml_item_id, sku in rows:
        try:
            res = sync_item_history_to_db(
                db, ml_item_id, sku=sku, period=period
            )
            if res["ok"]:
                summary["ok"] += 1
                summary["n_eventos_total"] += res["n_eventos"]
                summary["n_nuevos_total"] += res["n_nuevos"]
                summary["n_dups_total"] += res["n_dups"]
            else:
                summary["fail"] += 1
                if len(summary["errores"]) < 10:
                    summary["errores"].append(f"{ml_item_id}: {res['error']}")
        except Exception as e:
            summary["fail"] += 1
            if len(summary["errores"]) < 10:
                summary["errores"].append(f"{ml_item_id}: {type(e).__name__}: {e}")
        time.sleep(sleep_between)

    return summary
