"""
app/ml_diagnostico.py
=====================
Diagnóstico del seller en Mercado Libre — agrega múltiples señales para
identificar por qué bajaron las ventas y dónde están las oportunidades.

Fuentes de data:
  - DB local (rápido): conteos de status, shipping, drift, cambios de precio.
  - API ML (limitado a top-N items para no romper rate limits): visitas,
    health, preguntas sin responder, reputación del seller.

El resultado es un dict estructurado consumido por la vista /diagnostico-ml.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import select, func as sql_func, and_
from sqlalchemy.orm import Session

from . import ml_client
from .models import (
    MLOrder,
    Producto,
    ProductoPublicacionML,
    PrecioCambioLog,
)


# =============================================================
# Reputación del seller
# =============================================================

def analizar_seller(db: Session) -> dict:
    """Reputación del vendedor — color, nivel, % cancelados, reclamos."""
    info = ml_client.get_seller_reputation(db)
    if not info:
        return {"ok": False, "error": "No se pudo leer /users/me"}

    rep = info.get("seller_reputation") or {}
    metrics = rep.get("metrics") or {}
    tx = metrics.get("transactions") or {}

    return {
        "ok": True,
        "nickname": info.get("nickname"),
        "id": info.get("id"),
        "level_id": rep.get("level_id"),
        "power_seller_status": rep.get("power_seller_status"),
        "metrics_period": rep.get("metrics_period"),
        "transactions_completed": tx.get("completed"),
        "transactions_canceled_pct": (
            (metrics.get("cancellations") or {}).get("rate")
        ),
        "claims_rate": (metrics.get("claims") or {}).get("rate"),
        "delayed_handling_time_rate": (
            (metrics.get("delayed_handling_time") or {}).get("rate")
        ),
        "user_type": info.get("user_type"),
        "site_id": info.get("site_id"),
    }


# =============================================================
# Conteos locales: status, shipping, drift
# =============================================================

def analizar_publicaciones_locales(db: Session) -> dict:
    """
    Cruza producto_publicaciones_ml + productos para obtener:
      - Total / activas / pausadas / cerradas
      - Cuántas con drift (DB local vs snapshot ML)
      - Cuántas sin sku_ml seteado
      - Distribución de precios
    """
    Pub = ProductoPublicacionML
    P = Producto

    base_q = (
        select(Pub.ml_status, sql_func.count(Pub.id))
        .group_by(Pub.ml_status)
    )
    status_counts = {
        (s or "unknown"): int(c)
        for s, c in db.execute(base_q).all()
    }
    total = sum(status_counts.values())

    # Drift: stock_actual del producto != ml_stock_snapshot de la publicación
    drift_stock = db.execute(
        select(sql_func.count(Pub.id))
        .join(P, P.id == Pub.producto_id)
        .where(Pub.ml_stock_snapshot.is_not(None))
        .where(P.stock_actual.is_not(None))
        .where(Pub.ml_stock_snapshot != P.stock_actual)
    ).scalar() or 0

    # Items pausados con stock > 0 (oportunidad: reactivar)
    pausados_con_stock = db.execute(
        select(sql_func.count(Pub.id))
        .join(P, P.id == Pub.producto_id)
        .where(Pub.ml_status == "paused")
        .where(P.stock_actual > 0)
    ).scalar() or 0

    # Items sin sku_ml (penaliza ranking si ML no tiene el SELLER_SKU)
    sin_sku_ml = db.execute(
        select(sql_func.count(P.id))
        .where(P.ml_item_id.is_not(None))
        .where(sql_func.coalesce(P.sku_ml, "") == "")
    ).scalar() or 0

    # Distribución de precios (buckets) sobre activos
    activos_precios = db.execute(
        select(P.precio_final)
        .join(Pub, Pub.producto_id == P.id)
        .where(Pub.ml_status == "active")
        .where(P.precio_final.is_not(None))
    ).scalars().all()

    buckets = {
        "0-7k": 0, "7k-15k": 0, "15k-30k": 0, "30k-50k": 0,
        "50k-100k": 0, "100k-200k": 0, "200k+": 0,
    }
    for p in activos_precios:
        v = float(p)
        if v < 7000: buckets["0-7k"] += 1
        elif v < 15000: buckets["7k-15k"] += 1
        elif v < 30000: buckets["15k-30k"] += 1
        elif v < 50000: buckets["30k-50k"] += 1
        elif v < 100000: buckets["50k-100k"] += 1
        elif v < 200000: buckets["100k-200k"] += 1
        else: buckets["200k+"] += 1

    return {
        "total": total,
        "status_counts": status_counts,
        "drift_stock": drift_stock,
        "pausados_con_stock": pausados_con_stock,
        "sin_sku_ml": sin_sku_ml,
        "price_buckets": buckets,
        "n_con_precio": len(activos_precios),
    }


# =============================================================
# Ventas: ¿cuándo fue la última y cómo viene la tendencia?
# =============================================================

def analizar_ventas(db: Session) -> dict:
    """Última venta, ventas por ventana de tiempo (7d/30d/60d)."""
    now = datetime.now(timezone.utc)

    ultima = db.execute(
        select(sql_func.max(MLOrder.date_created))
        .where(MLOrder.status.in_(["paid", "confirmed"]))
    ).scalar()

    def count_in_window(days):
        cut = now - timedelta(days=days)
        return db.execute(
            select(sql_func.count(MLOrder.id))
            .where(MLOrder.status.in_(["paid", "confirmed"]))
            .where(MLOrder.date_created >= cut)
        ).scalar() or 0

    def revenue_in_window(days):
        cut = now - timedelta(days=days)
        return db.execute(
            select(sql_func.coalesce(sql_func.sum(MLOrder.total_amount), 0))
            .where(MLOrder.status.in_(["paid", "confirmed"]))
            .where(MLOrder.date_created >= cut)
        ).scalar() or 0

    return {
        "ultima_venta": ultima,
        "dias_sin_vender": (
            (now - ultima).days if ultima else None
        ),
        "ventas_7d": count_in_window(7),
        "ventas_30d": count_in_window(30),
        "ventas_60d": count_in_window(60),
        "revenue_7d": float(revenue_in_window(7)),
        "revenue_30d": float(revenue_in_window(30)),
        "revenue_60d": float(revenue_in_window(60)),
    }


# =============================================================
# Visitas: top items por visitas (API ML, limit estricto)
# =============================================================

def analizar_visitas_top(
    db: Session,
    *,
    max_items: int = 30,
    last_days: int = 30,
    sleep_between: float = 0.15,
) -> dict:
    """
    Trae las visitas (últimos N días) de los items activos top-N (por sold_quantity
    en MLOrder de los últimos 60 días, para focalizar en los que tenían tracción).
    """
    Pub = ProductoPublicacionML
    P = Producto

    # Top items por ventas históricas (60d)
    cut = datetime.now(timezone.utc) - timedelta(days=60)
    top_q = (
        select(
            MLOrder.ml_item_id,
            sql_func.sum(MLOrder.cantidad).label("vendidas"),
        )
        .where(MLOrder.date_created >= cut)
        .where(MLOrder.status.in_(["paid", "confirmed"]))
        .group_by(MLOrder.ml_item_id)
        .order_by(sql_func.sum(MLOrder.cantidad).desc())
        .limit(max_items)
    )
    top = db.execute(top_q).all()

    # Si no hay ventas en 60d, caemos a active publicaciones aleatorias
    if not top:
        top = [
            (mi, 0) for mi in db.execute(
                select(Pub.ml_item_id)
                .where(Pub.ml_status == "active")
                .limit(max_items)
            ).scalars().all()
        ]

    items = []
    api_errors = 0
    for ml_id, vendidas in top:
        if not ml_id:
            continue
        # Datos locales
        prod = db.execute(
            select(P.sku, P.titulo, P.precio_final, P.stock_actual, P.ml_status)
            .where(P.ml_item_id == ml_id)
        ).first()
        sku, titulo, precio, stock, status = (prod or (None, None, None, None, None))

        # Visitas via API
        visits_data = ml_client.get_item_visits(db, ml_id, last_days=last_days)
        total_visits = visits_data.get("total_visits") if visits_data else None
        if not visits_data:
            api_errors += 1

        items.append({
            "ml_item_id": ml_id,
            "sku": sku,
            "titulo": (titulo or "")[:80],
            "precio": float(precio) if precio else None,
            "stock": stock,
            "status": status,
            "vendidas_60d": int(vendidas) if vendidas else 0,
            "visitas_30d": total_visits,
            "conversion": (
                round((int(vendidas) / total_visits) * 100, 2)
                if (total_visits and vendidas) else None
            ),
        })
        time.sleep(sleep_between)

    return {
        "rows": items,  # 'items' colisiona con dict.items() en Jinja
        "max_items": max_items,
        "last_days": last_days,
        "api_errors": api_errors,
    }


# =============================================================
# Preguntas sin responder
# =============================================================

def analizar_preguntas(db: Session, *, limit: int = 50) -> dict:
    """Preguntas sin responder del seller — degradan el ranking."""
    info = ml_client.get_seller_reputation(db)
    if not info:
        return {"ok": False, "n_unanswered": None, "questions": []}
    seller_id = info.get("id")
    resp = ml_client.get_questions_unanswered(db, seller_id, limit=limit)
    questions = resp.get("questions") or []

    out = []
    for q in questions[:limit]:
        out.append({
            "id": q.get("id"),
            "item_id": q.get("item_id"),
            "date_created": q.get("date_created"),
            "text": (q.get("text") or "")[:140],
            "from_user_id": (q.get("from") or {}).get("id"),
        })
    return {
        "ok": True,
        "n_unanswered": int(resp.get("total", len(questions))),
        "questions": out,
    }


# =============================================================
# Audit log de cambios recientes (sospechosos de la caída)
# =============================================================

def analizar_cambios_recientes(db: Session, *, days: int = 14) -> dict:
    """Cambios de precio internos en los últimos N días — pueden ser causa."""
    cut = datetime.now(timezone.utc) - timedelta(days=days)
    total = db.execute(
        select(sql_func.count(PrecioCambioLog.id))
        .where(PrecioCambioLog.created_at >= cut)
    ).scalar() or 0

    by_fonte = {
        f: int(c)
        for f, c in db.execute(
            select(PrecioCambioLog.fonte, sql_func.count(PrecioCambioLog.id))
            .where(PrecioCambioLog.created_at >= cut)
            .group_by(PrecioCambioLog.fonte)
        ).all()
    }

    # Bajas vs subas (delta = precio_nuevo - precio_anterior)
    bajas = db.execute(
        select(sql_func.count(PrecioCambioLog.id))
        .where(PrecioCambioLog.created_at >= cut)
        .where(PrecioCambioLog.precio_anterior.is_not(None))
        .where(PrecioCambioLog.precio_nuevo < PrecioCambioLog.precio_anterior)
    ).scalar() or 0
    subas = db.execute(
        select(sql_func.count(PrecioCambioLog.id))
        .where(PrecioCambioLog.created_at >= cut)
        .where(PrecioCambioLog.precio_anterior.is_not(None))
        .where(PrecioCambioLog.precio_nuevo > PrecioCambioLog.precio_anterior)
    ).scalar() or 0

    return {
        "days": days,
        "total": total,
        "by_fonte": by_fonte,
        "bajas": bajas,
        "subas": subas,
    }


# =============================================================
# Orquestador principal
# =============================================================

def generar_diagnostico(
    db: Session,
    *,
    sample_visitas: int = 30,
    sample_preguntas: int = 50,
) -> dict:
    """
    Diagnóstico completo. Devuelve dict listo para renderizar en HTML.
    Cada sección está aislada en try/except: si falla una, el resto sigue.
    """
    out: dict = {
        "generated_at": datetime.now(timezone.utc),
        "errors": [],
    }

    for name, fn in [
        ("seller", lambda: analizar_seller(db)),
        ("publicaciones", lambda: analizar_publicaciones_locales(db)),
        ("ventas", lambda: analizar_ventas(db)),
        ("cambios_recientes", lambda: analizar_cambios_recientes(db)),
        ("visitas_top", lambda: analizar_visitas_top(db, max_items=sample_visitas)),
        ("preguntas", lambda: analizar_preguntas(db, limit=sample_preguntas)),
    ]:
        try:
            out[name] = fn()
        except Exception as e:
            import traceback
            traceback.print_exc()
            out[name] = {"error": f"{type(e).__name__}: {e}"}
            out["errors"].append(f"{name}: {type(e).__name__}: {e}")

    # Findings / banderas: heurísticas para priorizar acciones
    findings = []

    pub = out.get("publicaciones") or {}
    if isinstance(pub, dict):
        if pub.get("drift_stock", 0) > 20:
            findings.append({
                "severidad": "alta",
                "titulo": f"{pub['drift_stock']} publicaciones con drift de stock",
                "accion": "Resync local desde catálogo + sync from ML los items afectados",
                "url": "/publicaciones?drift=si",
            })
        if pub.get("pausados_con_stock", 0) > 10:
            findings.append({
                "severidad": "alta",
                "titulo": f"{pub['pausados_con_stock']} publicaciones pausadas con stock disponible",
                "accion": "Reactivarlas — ML no las activa solo, las perdés en el ranking",
                "url": "/publicaciones?status=paused",
            })
        if pub.get("sin_sku_ml", 0) > 50:
            findings.append({
                "severidad": "media",
                "titulo": f"{pub['sin_sku_ml']} publicaciones sin SELLER_SKU en ML",
                "accion": "Pushar SKU_ML — mejora vinculación con catálogo ML y multiplica visitas",
            })

    ventas = out.get("ventas") or {}
    if isinstance(ventas, dict) and ventas.get("dias_sin_vender") is not None:
        if ventas["dias_sin_vender"] >= 5:
            findings.append({
                "severidad": "alta",
                "titulo": f"{ventas['dias_sin_vender']} días sin ventas",
                "accion": (
                    "Combinar: reactivar pausados + arreglar drift + bajar 5-10% precios "
                    "estratégicos en items top con caída de visitas"
                ),
            })

    seller = out.get("seller") or {}
    if isinstance(seller, dict):
        cancel_pct = seller.get("transactions_canceled_pct")
        if cancel_pct is not None and cancel_pct > 0.04:
            findings.append({
                "severidad": "alta",
                "titulo": f"Tasa de cancelaciones {round(cancel_pct*100,2)}%",
                "accion": (
                    "Cancelaciones >4% hunden el ranking. Auditar últimas cancelaciones "
                    "y proceso de stock para no aceptar pedidos imposibles."
                ),
            })

    preg = out.get("preguntas") or {}
    if isinstance(preg, dict) and preg.get("n_unanswered", 0) and preg["n_unanswered"] > 5:
        findings.append({
            "severidad": "alta",
            "titulo": f"{preg['n_unanswered']} preguntas sin responder",
            "accion": (
                "Respuesta lenta = penalización en ranking. Responder las viejas YA — "
                "es la acción más rápida con retorno inmediato."
            ),
        })

    out["findings"] = findings
    return out
