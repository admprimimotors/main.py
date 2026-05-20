"""
app/publicaciones.py
====================
Módulo "Publicaciones ML" — gestión de los items publicados en Mercado Libre.

Diferencia con `app/catalogo.py`:
  - `catalogo` = el catálogo local (todos los productos, vinculados o no a ML).
  - `publicaciones` = vista enfocada solo en los que ESTÁN en ML, con acciones
    relacionadas con el estado de la publicación (pausar, activar, refresh).

Funciones públicas:
  - list_publicaciones(db, filters) → (rows, total, stats)
  - bulk_change_status(db, skus, new_status) → (n_ok, n_fail, errors)
  - refresh_status_from_ml(db, skus) → trae el status real desde ML y lo actualiza
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func as sql_func, or_, select
from sqlalchemy.orm import Session, selectinload

from . import ml_client
from .models import FotoProducto, Producto


PAGE_SIZE = 100


def list_publicaciones(
    db: Session,
    *,
    search: str = "",
    status: str = "",
    categoria: str = "",
    marca: str = "",
    drift: str = "",
    page: int = 1,
) -> tuple[list[dict], int, dict]:
    """
    Lista productos con `ml_item_id` (publicados). Devuelve (rows, total, stats).

    Filtros:
      - search: matchea SKU, título, marca, categoría
      - status: "active", "paused", "closed", "under_review", "" (todos)
      - categoria / marca: filtros exactos
      - drift: "si" → solo los que tienen mismatch stock o precio entre ML y DB
               "no" → solo los sincronizados
               "" → todos

    Stats devuelve totales por status:
      {"total": N, "active": N, "paused": N, "closed": N, "under_review": N,
       "drift": N}
    """
    base_q = (
        select(Producto)
        .options(selectinload(Producto.fotos))
        .where(Producto.ml_item_id.is_not(None))
    )
    count_q = (
        select(sql_func.count(Producto.id))
        .where(Producto.ml_item_id.is_not(None))
    )

    extra = []
    if search and search.strip():
        like = f"%{search.strip()}%"
        extra.append(or_(
            Producto.sku.ilike(like),
            Producto.titulo.ilike(like),
            Producto.marca.ilike(like),
            Producto.categoria.ilike(like),
            Producto.ml_item_id.ilike(like),
        ))
    if status:
        extra.append(Producto.ml_status == status)
    if categoria:
        extra.append(Producto.categoria == categoria)
    if marca:
        extra.append(Producto.marca == marca)

    # drift se aplica en SQL: stock_drift OR precio_drift
    if drift in ("si", "no"):
        # Stock drift: ml_stock IS NOT NULL AND stock_actual IS NOT NULL AND ml_stock != stock_actual
        stock_drift_cond = (
            Producto.ml_stock.is_not(None)
            & Producto.stock_actual.is_not(None)
            & (Producto.ml_stock != Producto.stock_actual)
        )
        precio_drift_cond = (
            Producto.ml_precio.is_not(None)
            & Producto.precio_final.is_not(None)
            & (Producto.ml_precio != Producto.precio_final)
        )
        drift_cond = stock_drift_cond | precio_drift_cond
        if drift == "si":
            extra.append(drift_cond)
        elif drift == "no":
            extra.append(~drift_cond)

    if extra:
        base_q = base_q.where(*extra)
        count_q = count_q.where(*extra)

    total = db.execute(count_q).scalar() or 0

    base_q = (
        base_q
        .order_by(Producto.ml_last_synced_at.asc().nulls_first())
        .limit(PAGE_SIZE)
        .offset((page - 1) * PAGE_SIZE)
    )
    products = list(db.execute(base_q).scalars().all())

    # Stats globales (sin paginación, con search/categoria/marca pero
    # sin filtro de status ni de drift — los tiles muestran absolutos
    # para que se vea "X activas" independientemente del filtro activo).
    stats_q = (
        select(Producto.ml_status, sql_func.count(Producto.id))
        .where(Producto.ml_item_id.is_not(None))
        .group_by(Producto.ml_status)
    )
    # Solo aplicamos los filtros que NO sean status ni drift
    stats_filters = []
    if search and search.strip():
        like = f"%{search.strip()}%"
        stats_filters.append(or_(
            Producto.sku.ilike(like),
            Producto.titulo.ilike(like),
            Producto.marca.ilike(like),
            Producto.categoria.ilike(like),
            Producto.ml_item_id.ilike(like),
        ))
    if categoria:
        stats_filters.append(Producto.categoria == categoria)
    if marca:
        stats_filters.append(Producto.marca == marca)
    if stats_filters:
        stats_q = stats_q.where(*stats_filters)
    raw_stats = {row[0] or "unknown": row[1] for row in db.execute(stats_q).all()}
    stats = {
        "total": sum(raw_stats.values()),
        "active": raw_stats.get("active", 0),
        "paused": raw_stats.get("paused", 0),
        "closed": raw_stats.get("closed", 0),
        "under_review": raw_stats.get("under_review", 0),
        "inactive": raw_stats.get("inactive", 0),
    }

    # Drift count global (no depende de paginación) — usamos misma condición SQL
    drift_count_q = (
        select(sql_func.count(Producto.id))
        .where(Producto.ml_item_id.is_not(None))
        .where(
            (Producto.ml_stock.is_not(None)
             & Producto.stock_actual.is_not(None)
             & (Producto.ml_stock != Producto.stock_actual))
            |
            (Producto.ml_precio.is_not(None)
             & Producto.precio_final.is_not(None)
             & (Producto.ml_precio != Producto.precio_final))
        )
    )
    if stats_filters:
        drift_count_q = drift_count_q.where(*stats_filters)
    drift_count = db.execute(drift_count_q).scalar() or 0

    rows = []
    for p in products:
        stock_drift = (
            p.ml_stock is not None
            and p.stock_actual is not None
            and p.ml_stock != p.stock_actual
        )
        precio_drift = (
            p.ml_precio is not None
            and p.precio_final is not None
            and p.ml_precio != p.precio_final
        )

        rows.append({
            "id": p.id,
            "sku": p.sku,
            "titulo": p.titulo,
            "categoria": p.categoria,
            "marca": p.marca,
            "stock_actual": p.stock_actual,
            "ml_stock": p.ml_stock,
            "stock_drift": stock_drift,
            "precio_final": p.precio_final,
            "ml_precio": p.ml_precio,
            "precio_drift": precio_drift,
            "ml_item_id": p.ml_item_id,
            "ml_status": p.ml_status,
            "ml_permalink": p.ml_permalink,
            "ml_last_synced_at": p.ml_last_synced_at,
            "foto_url": p.fotos[0].url if p.fotos else None,
        })
    stats["drift"] = drift_count

    return rows, total, stats


def bulk_change_status(
    db: Session,
    skus: list[str],
    new_status: str,
) -> tuple[int, int, list[str]]:
    """
    Cambia el status de múltiples publicaciones (active / paused / closed).
    Devuelve (n_ok, n_fail, errores).

    Atención: "closed" es IRREVERSIBLE por API — usar solo si estás seguro.
    Pausar/activar se pueden tirar y volver a tirar.
    """
    if new_status not in ("active", "paused", "closed"):
        return 0, len(skus), [f"Status inválido: {new_status}"]
    if not ml_client.is_write_enabled():
        return 0, len(skus), ["Write sync ML deshabilitado"]

    n_ok = 0
    errores: list[str] = []
    for sku in skus:
        sku = (sku or "").strip()
        if not sku:
            continue
        prod = db.execute(
            select(Producto).where(Producto.sku == sku)
        ).scalar_one_or_none()
        if prod is None:
            errores.append(f"{sku}: SKU no existe")
            continue
        if not prod.ml_item_id:
            errores.append(f"{sku}: no está vinculado a ML")
            continue
        try:
            ml_client.update_item_status(db, prod.ml_item_id, new_status)
            prod.ml_status = new_status
            prod.ml_last_synced_at = datetime.now(timezone.utc)
            n_ok += 1
        except Exception as e:
            errores.append(f"{sku}: {type(e).__name__}: {str(e)[:150]}")
        # Rate limit suave: ~0.2s entre llamadas
        time.sleep(0.2)
    if n_ok:
        db.commit()
    return n_ok, len(errores), errores


def bulk_push_to_ml(
    db: Session,
    skus: list[str],
    *,
    push_stock: bool = True,
    push_price: bool = True,
) -> tuple[int, int, list[str]]:
    """
    Para cada SKU con vinculación ML, pushea stock + precio desde la BD local
    a la publicación de ML. Útil cuando hay drift y la BD es la fuente de verdad.

    Devuelve (n_ok, n_fail, errores).
    """
    if not ml_client.is_write_enabled():
        return 0, len(skus), ["Write sync ML deshabilitado"]

    from . import catalogo as catalogo_module  # lazy para evitar circular
    n_ok = 0
    errores: list[str] = []
    for sku in skus:
        sku = (sku or "").strip()
        if not sku:
            continue
        try:
            success, msg = catalogo_module.push_to_ml(
                db, sku,
                push_stock=push_stock,
                push_price=push_price,
                push_description=False,
                push_attributes=False,
                push_title=False,
                push_pictures=False,
            )
        except Exception as e:
            success = False
            msg = f"{type(e).__name__}: {str(e)[:150]}"
        if success:
            n_ok += 1
        else:
            errores.append(f"{sku}: {msg[:150]}")
        time.sleep(0.2)
    return n_ok, len(errores), errores


def refresh_status_from_ml(
    db: Session,
    skus: list[str],
) -> tuple[int, int, list[str]]:
    """
    Para cada SKU, hace GET /items/{id} y actualiza ml_status, ml_stock,
    ml_precio, ml_last_synced_at locales con lo que ML reporta.
    Útil cuando ML pausó publicaciones por stock=0 y queremos sincronizar.
    """
    n_ok = 0
    errores: list[str] = []
    for sku in skus:
        sku = (sku or "").strip()
        if not sku:
            continue
        prod = db.execute(
            select(Producto).where(Producto.sku == sku)
        ).scalar_one_or_none()
        if prod is None or not prod.ml_item_id:
            errores.append(f"{sku}: sin ml_item_id")
            continue
        try:
            item = ml_client.get_item(db, prod.ml_item_id)
            prod.ml_status = item.get("status")
            prod.ml_stock = item.get("available_quantity")
            price = item.get("price")
            if price is not None:
                from decimal import Decimal
                try:
                    prod.ml_precio = Decimal(str(price))
                except Exception:
                    pass
            prod.ml_last_synced_at = datetime.now(timezone.utc)
            n_ok += 1
        except Exception as e:
            errores.append(f"{sku}: {type(e).__name__}: {str(e)[:150]}")
        time.sleep(0.15)
    if n_ok:
        db.commit()
    return n_ok, len(errores), errores
