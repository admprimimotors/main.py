"""
app/ml_price_tracker.py
=======================
Tracking de precios de Mercado Libre — captura snapshots periódicos para
construir el histórico de cambios de precio que ML no expone vía API.

Cómo funciona:
  1. Cada N horas (auto-sync al cargar el dashboard, igual que ml_orders.py)
     o vía endpoint manual, se ejecuta `snapshot_all_active(db)`.
  2. Lista todas las publicaciones activas del seller (~1000+).
  3. Trae precio actual de cada una desde ML (en lotes de 20 con multi-get).
  4. Compara contra el último snapshot del mismo item en `ml_price_snapshots`.
  5. Inserta una fila nueva por item. Marca `is_change=True` si el precio
     cambió respecto al snapshot anterior.

El histórico se consulta filtrando por `ml_item_id` y/o `is_change=True`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import select, func as sql_func
from sqlalchemy.orm import Session

from . import ml_client
from .models import MLPriceSnapshot


def _seller_id() -> Optional[int]:
    """Obtiene el user_id del seller actual desde ML."""
    try:
        me = ml_client.api_get("/users/me")
        return me.get("id")
    except Exception:
        return None


def _listar_ids_activos(user_id: int) -> list[str]:
    """Scan completo de items activos via /users/{id}/items/search."""
    ids: list[str] = []
    scroll_id: Optional[str] = None
    while True:
        params = {"search_type": "scan", "limit": 100, "status": "active"}
        if scroll_id:
            params["scroll_id"] = scroll_id
        r = ml_client.api_get(f"/users/{user_id}/items/search", params=params)
        batch = r.get("results", []) or []
        if not batch:
            break
        ids.extend(batch)
        scroll_id = r.get("scroll_id")
        if not scroll_id:
            break
    return ids


def _traer_meta_lotes(ids: list[str]) -> dict[str, dict]:
    """Multi-get de items en lotes de 20."""
    out: dict[str, dict] = {}
    attrs = "id,title,price,base_price,original_price,currency_id,status,available_quantity,sold_quantity,seller_sku,seller_custom_field,attributes"
    for i in range(0, len(ids), 20):
        batch = ids[i:i + 20]
        r = ml_client.api_get("/items", params={"ids": ",".join(batch), "attributes": attrs})
        for entry in r:
            if entry.get("code") != 200:
                continue
            b = entry["body"]
            sku = b.get("seller_sku") or b.get("seller_custom_field")
            if not sku:
                for a in b.get("attributes", []) or []:
                    if a.get("id") == "SELLER_SKU":
                        sku = a.get("value_name")
                        break
            out[b["id"]] = {
                "title": b.get("title"),
                "sku": sku,
                "price": b.get("price"),
                "base_price": b.get("base_price"),
                "original_price": b.get("original_price"),
                "currency": b.get("currency_id"),
                "status": b.get("status"),
                "available_quantity": b.get("available_quantity"),
                "sold_quantity": b.get("sold_quantity"),
            }
    return out


def _ultimo_precio(db: Session, ml_item_id: str) -> Optional[Decimal]:
    """Devuelve el precio del último snapshot del item (o None si no hay)."""
    r = db.execute(
        select(MLPriceSnapshot.price)
        .where(MLPriceSnapshot.ml_item_id == ml_item_id)
        .order_by(MLPriceSnapshot.captured_at.desc(), MLPriceSnapshot.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    return r


def snapshot_all_active(db: Session, *, changed_only: bool = False) -> dict:
    """
    Captura snapshots de TODAS las publicaciones activas.

    Args:
        db: sesión SQLAlchemy.
        changed_only: si True, solo inserta filas para items donde el precio
                      cambió respecto al snapshot anterior. Por defecto False
                      (insertamos siempre, manteniendo el histórico completo).

    Returns:
        dict con resumen: total_items, n_nuevos, n_cambios, n_guardados.
    """
    user_id = _seller_id()
    if user_id is None:
        return {"ok": False, "error": "No se pudo obtener seller_id (¿ML configurado?)"}

    ids = _listar_ids_activos(user_id)
    meta = _traer_meta_lotes(ids)

    now = datetime.now(timezone.utc)
    n_guardados = 0
    n_nuevos = 0
    n_cambios = 0
    cambios_sample: list[dict] = []

    for iid, m in meta.items():
        precio = m.get("price")
        if precio is None:
            continue
        precio_dec = Decimal(str(precio))
        prev = _ultimo_precio(db, iid)
        es_cambio = (prev is None) or (abs(precio_dec - prev) > Decimal("0.005"))
        if prev is None:
            n_nuevos += 1
        if es_cambio and prev is not None:
            n_cambios += 1
            if len(cambios_sample) < 10:
                cambios_sample.append({
                    "iid": iid,
                    "title": m.get("title", "")[:80],
                    "antes": float(prev),
                    "ahora": float(precio_dec),
                    "delta_pct": float((precio_dec - prev) / prev * 100) if prev else 0,
                })

        if changed_only and not es_cambio:
            continue

        snap = MLPriceSnapshot(
            ml_item_id=iid,
            title=m.get("title"),
            sku=m.get("sku"),
            price=precio_dec,
            base_price=Decimal(str(m["base_price"])) if m.get("base_price") is not None else None,
            original_price=Decimal(str(m["original_price"])) if m.get("original_price") is not None else None,
            currency=m.get("currency"),
            status=m.get("status"),
            available_quantity=m.get("available_quantity"),
            sold_quantity=m.get("sold_quantity"),
            is_change=es_cambio,
            captured_at=now,
        )
        db.add(snap)
        n_guardados += 1

    db.commit()
    return {
        "ok": True,
        "captured_at": now.isoformat(),
        "total_items_activos": len(meta),
        "n_guardados": n_guardados,
        "n_nuevos": n_nuevos,
        "n_cambios": n_cambios,
        "cambios_sample": cambios_sample,
    }


def historial_de_item(db: Session, ml_item_id: str, *, only_changes: bool = True, limit: int = 100) -> list[MLPriceSnapshot]:
    """
    Devuelve el histórico de precios de un item, ordenado de más reciente
    a más viejo. Por defecto solo trae cambios (is_change=True).
    """
    stmt = (
        select(MLPriceSnapshot)
        .where(MLPriceSnapshot.ml_item_id == ml_item_id)
        .order_by(MLPriceSnapshot.captured_at.desc(), MLPriceSnapshot.id.desc())
        .limit(limit)
    )
    if only_changes:
        stmt = stmt.where(MLPriceSnapshot.is_change.is_(True))
    return list(db.execute(stmt).scalars().all())


def cambios_recientes(db: Session, *, days: int = 7, limit: int = 50) -> list[MLPriceSnapshot]:
    """Lista los cambios de precio detectados en los últimos N días."""
    from datetime import timedelta
    desde = datetime.now(timezone.utc) - timedelta(days=days)
    return list(db.execute(
        select(MLPriceSnapshot)
        .where(MLPriceSnapshot.captured_at >= desde)
        .where(MLPriceSnapshot.is_change.is_(True))
        .order_by(MLPriceSnapshot.captured_at.desc())
        .limit(limit)
    ).scalars().all())
